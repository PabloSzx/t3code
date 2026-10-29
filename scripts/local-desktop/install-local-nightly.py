#!/usr/bin/env python3
"""Install a verified local T3 Code Nightly build over the current app.

Copy this script into a build directory next to its build.json, or pass --build.
It backs up userdata, the Electron profile and the current app, verifies those
copies, stages the candidate, and swaps it in atomically. It never restores data
and never kills processes. When T3 is running it waits, and it offers a normal
quit request (the same as Cmd-Q) only when you confirm in an interactive terminal.
"""
import argparse
import ctypes
import hashlib
import json
import os
from pathlib import Path
import plistlib
import pwd
import shutil
import sqlite3
import stat
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone

HOME = Path(pwd.getpwuid(os.getuid()).pw_dir)
APP = Path('/Applications/T3 Code (Nightly).app')
BUNDLE_ID = 'com.t3tools.t3code'
USERDATA = HOME / '.t3/userdata'
PROFILE = HOME / 'Library/Application Support/t3code-v2'
LOCAL_ROOT = HOME / '.local/share/t3code'
# Backup of the official app this machine started from; accepted as a base.
OFFICIAL_BASES = {'d49a0ecb68d2302d68a8e401c41c8f2961b16a23f8f12493dae3d95e4362924c'}
WAIT_INTERVAL_SECONDS = 2
SYNC_ATTEMPTS = 3


class Stop(RuntimeError):
    """A refusal that leaves the app and user data unchanged."""


def say(message):
    print(message, flush=True)


def run(args):
    result = subprocess.run([str(x) for x in args], capture_output=True, text=True)
    if result.returncode:
        raise Stop(f'{Path(str(args[0])).name} failed: {result.stderr.strip()}')
    return result.stdout.strip()


def write_json(path, value):
    descriptor, temporary = tempfile.mkstemp(prefix=path.name + '.', dir=path.parent)
    try:
        with os.fdopen(descriptor, 'w') as stream:
            json.dump(value, stream, indent=2)
            stream.write('\n')
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def check_data_roots():
    for root in (USERDATA, PROFILE):
        if not root.is_dir() or root.resolve() != root:
            raise Stop(f'Confirm the actual production path before backing up {root}.')
        for folder, directories, files in os.walk(root, followlinks=False):
            for name in directories + files:
                path = Path(folder) / name
                if path.is_symlink() and path.exists() and not path.resolve().is_relative_to(root):
                    raise Stop(f'External data link needs its own confirmed backup: {path}')


def exchange_directories(left, right):
    # macOS RENAME_SWAP exchanges both directory entries in one filesystem operation.
    library = ctypes.CDLL('/usr/lib/libSystem.B.dylib', use_errno=True)
    exchange = library.renamex_np
    exchange.argtypes = [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_uint]
    exchange.restype = ctypes.c_int
    if exchange(os.fsencode(left), os.fsencode(right), 0x00000002) != 0:
        code = ctypes.get_errno()
        raise OSError(code, os.strerror(code))


def digest(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def app_info(path):
    with (path / 'Contents/Info.plist').open('rb') as stream:
        return plistlib.load(stream)


def app_identity(path):
    info = app_info(path)
    return {
        'bundleId': info.get('CFBundleIdentifier'),
        'version': info.get('CFBundleShortVersionString'),
        'asarSha256': digest(path / 'Contents/Resources/app.asar'),
    }


def verify_candidate(path, version, asar):
    identity = app_identity(path)
    if identity['bundleId'] != BUNDLE_ID or identity['version'] != version:
        raise Stop(f'Unexpected app identity or version at {path}.')
    if identity['asarSha256'] != asar:
        raise Stop(f'Archive at {path} does not match the pinned build.')
    run(['/usr/bin/codesign', '--verify', '--deep', '--strict', path])
    if (path / 'Contents/Resources/app-update.yml').exists():
        raise Stop('Refusing a local build with a binary update feed.')
    if run(['/usr/bin/lipo', '-archs', path / 'Contents/MacOS/T3 Code (Nightly)']) != 'arm64':
        raise Stop('Candidate is not the verified arm64 app.')


def known_bases():
    """Archive hashes of builds this machine produced, plus the original official app."""
    known = set(OFFICIAL_BASES)
    for manifest in (LOCAL_ROOT / 'builds').glob('*/build.json'):
        try:
            record = json.loads(manifest.read_text())
        except (OSError, ValueError):
            continue
        if isinstance(record.get('asarSha256'), str):
            known.add(record['asarSha256'])
        app = manifest.parent / 'artifacts/T3 Code (Nightly).app/Contents/Resources/app.asar'
        if record.get('installed') is True and app.exists():
            known.add(digest(app))
    return known


def verify_base(accept_unknown):
    if APP.is_symlink():
        raise Stop('The installed app is a link. Confirm its installation ownership before replacing it.')
    identity = app_identity(APP)
    if identity['bundleId'] != BUNDLE_ID:
        raise Stop(f'{APP} is not T3 Code Nightly.')
    run(['/usr/bin/codesign', '--verify', '--deep', '--strict', APP])
    if identity['asarSha256'] not in known_bases() and not accept_unknown:
        raise Stop(
            f'The installed app ({identity["version"]}) is not a build this machine produced. '
            'Rerun with --accept-unknown-base after confirming you want to replace it.'
        )
    return identity


def inventory(root):
    result = {}
    for folder, directories, files in os.walk(root, followlinks=False):
        for name in directories + files:
            path = Path(folder) / name
            relative = path.relative_to(root).as_posix()
            metadata = path.lstat()
            mode = stat.S_IMODE(metadata.st_mode)
            if stat.S_ISLNK(metadata.st_mode):
                result[relative] = ['link', os.readlink(path)]
            elif stat.S_ISDIR(metadata.st_mode):
                result[relative] = ['directory', mode]
            elif stat.S_ISREG(metadata.st_mode):
                result[relative] = ['file', mode, metadata.st_size, digest(path)]
            else:
                raise Stop(f'Cannot verify a special file in {root}: {relative}')
    return result


def summary(entries):
    encoded = json.dumps(entries, sort_keys=True, separators=(',', ':')).encode()
    return {'entries': len(entries), 'inventorySha256': hashlib.sha256(encoded).hexdigest()}


def open_pids(paths):
    """PIDs holding files open under any of the paths."""
    pids = set()
    for path in paths:
        if not path.exists():
            continue
        result = subprocess.run(['/usr/sbin/lsof', '-nP', '-t', '+D', str(path)], capture_output=True, text=True)
        if result.returncode not in (0, 1):
            raise Stop(f'Cannot establish whether {path} is in use. lsof: {result.stderr.strip()}')
        pids.update(result.stdout.split())
    pids.discard(str(os.getpid()))
    return sorted(pids, key=int)


def describe(pids):
    names = []
    for pid in pids:
        result = subprocess.run(['/bin/ps', '-o', 'comm=', '-p', pid], capture_output=True, text=True)
        name = Path(result.stdout.strip()).name or 'exited'
        names.append(f'{pid} ({name})')
    return ', '.join(names)


def request_quit():
    # A normal quit, exactly what Cmd-Q sends; T3 shuts down its own server.
    subprocess.run(
        ['/usr/bin/osascript', '-e', f'tell application id "{BUNDLE_ID}" to quit'],
        capture_output=True,
        text=True,
    )


def wait_until_closed(paths, assume_yes):
    """Waits until nothing holds the paths open. Never stops a process itself."""
    pids = open_pids(paths)
    if not pids:
        return
    say(f'T3 is still running: {describe(pids)}.')
    asked = False
    if sys.stdin.isatty() or assume_yes:
        answer = 'y' if assume_yes else input('Quit T3 normally now, like Cmd-Q? [Y/n] ').strip().lower()
        asked = answer in ('', 'y', 'yes')
        if asked:
            request_quit()
    if not asked:
        say('Quit T3 with Cmd-Q, and stop any standalone T3 server you started.')
    say('Waiting for it to close. Press Ctrl-C to cancel; nothing has changed yet.')
    while pids:
        time.sleep(WAIT_INTERVAL_SECONDS)
        pids = open_pids(paths)
    say('T3 is closed.')


def copy_tree(source, target):
    run(['/usr/bin/ditto', '--rsrc', '--extattr', source, target])


class Unstable(Exception):
    """The source changed while it was being copied."""


def snapshot(source, target):
    """Copies source into target and returns the verified inventory summary."""
    if target.exists():
        # Only this in-progress backup's own superseded copy; never a completed backup.
        shutil.rmtree(target)
    copy_tree(source, target)
    expected = inventory(source)
    if inventory(target) != expected:
        raise Unstable(source)
    return summary(expected)


def stable_snapshot(source, target, watched, assume_yes):
    """Copies source once nothing has it open, retrying when it changes during the copy."""
    for _ in range(SYNC_ATTEMPTS):
        wait_until_closed(watched, assume_yes)
        check_data_roots()
        try:
            return snapshot(source, target)
        except Unstable:
            say(f'{source.name} changed while it was copied; copying it again.')
    raise Stop(f'{source} kept changing during the backup. Quit T3 and run this again.')


def verify_databases(backup):
    # Recover and check disposable clones, never the live databases or the archived originals.
    clone_root = Path(tempfile.mkdtemp(prefix='database-check-', dir=backup))
    checked = []
    for name in ('statev2.sqlite', 'state.sqlite'):
        source = backup / 'userdata' / name
        if not source.exists():
            continue
        target = clone_root / name
        for suffix in ('', '-wal', '-shm', '-journal'):
            sibling = Path(str(source) + suffix)
            if sibling.exists():
                shutil.copy2(sibling, Path(str(target) + suffix))
        connection = sqlite3.connect(target)
        try:
            if connection.execute('PRAGMA quick_check').fetchall() != [('ok',)]:
                raise Stop(f'Backup database integrity check failed for {name}. Clones kept in {clone_root}.')
        finally:
            connection.close()
        checked.append(name)
    if 'statev2.sqlite' not in checked:
        raise Stop('The expected conversation database is missing from the backup.')
    shutil.rmtree(clone_root)
    return checked


def required_space(candidate):
    total = 0
    for base in (APP, candidate, USERDATA, PROFILE):
        total += sum(p.stat().st_size for p in base.rglob('*') if p.is_file() and not p.is_symlink())
    return total + (USERDATA / 'statev2.sqlite').stat().st_size + 1024 * 1024 * 1024


def app_running():
    return bool(open_pids([APP]))


def install(build, check_only, assume_yes, accept_unknown):
    if os.geteuid() == 0:
        raise Stop('Run this as your normal macOS user, without sudo.')
    os.umask(0o077)
    record = json.loads((build / 'build.json').read_text())
    version, source, asar = record.get('version'), record.get('sourceCommit'), record.get('asarSha256')
    if not (isinstance(version, str) and isinstance(source, str) and isinstance(asar, str)):
        raise Stop('build.json does not pin version, sourceCommit and asarSha256.')
    candidate = build / 'artifacts/T3 Code (Nightly).app'
    verify_candidate(candidate, version, asar)

    current = app_identity(APP)
    if current['version'] == version and current['asarSha256'] == asar:
        say(f'{version} is already installed. Nothing to do.')
        return
    base = verify_base(accept_unknown)
    if check_only:
        say(f'Ready to replace {base["version"]} with {version}. Nothing was changed.')
        return
    if record.get('readyForManualInstall') is not True:
        raise Stop('This build has not been released for manual installation.')
    if not os.access(APP.parent, os.W_OK):
        raise Stop('Your user cannot replace apps in /Applications. Do not retry with sudo.')
    check_data_roots()
    if min(shutil.disk_usage(HOME).free, shutil.disk_usage(APP.parent).free) < required_space(candidate):
        raise Stop('Not enough free space for verified backups, database verification and app staging.')

    watched = [APP, USERDATA, PROFILE]
    wait_until_closed(watched, assume_yes)

    stamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S.%fZ')
    backup = LOCAL_ROOT / 'backups' / f'install-{stamp}'
    backup.mkdir(parents=True, mode=0o700, exist_ok=False)
    receipt = {'sourceCommit': source, 'version': version, 'base': base, 'backup': str(backup),
               'status': 'backing-up', 'installed': False}

    def save():
        write_json(backup / 'installation.json', receipt)

    save()
    stage = APP.parent / f'.T3-Code-Nightly-{stamp}.staging.app'
    previous = APP.parent / f'.T3-Code-Nightly-{stamp}.previous.app'
    if stage.exists() or previous.exists():
        raise Stop('A staging path already exists. No replacement is attempted.')
    copy_tree(candidate, stage)
    if inventory(stage) != inventory(candidate):
        raise Stop(f'Staging verification failed. The staged copy is kept at {stage}.')
    verify_candidate(stage, version, asar)

    say('Backing up userdata, the Electron profile and the current app. Live files are only read.')
    sources = ((USERDATA, 'userdata'), (PROFILE, 'electron-profile'), (APP, 'T3 Code (Nightly).app'))
    verification = {}
    checked_databases = None
    # Every pass waits for T3 to close and recopies anything that changed since the last pass.
    # The swap happens only after a pass in which nothing changed and nothing reopened.
    for attempt in range(SYNC_ATTEMPTS):
        changed = False
        for path, name in sources:
            target = backup / name
            if name in verification and inventory(path) == inventory(target):
                continue
            verification[name] = stable_snapshot(path, target, watched, assume_yes)
            changed = True
            if name == 'userdata':
                checked_databases = None
        if checked_databases is None:
            checked_databases = verify_databases(backup)
        if app_identity(APP)['asarSha256'] != base['asarSha256']:
            raise Stop('The installed app changed during the backup. Run this again.')
        check_data_roots()
        if not changed and not open_pids(watched):
            break
        if attempt == SYNC_ATTEMPTS - 1:
            raise Stop('T3 kept reopening during the backup. Quit it and run this again.')

    receipt['verifiedDatabases'] = checked_databases
    receipt['copyVerification'] = verification
    receipt.update(status='swapping', stagedBundle=str(stage), previousBundle=str(previous))
    save()
    say(f'Backups verified: {backup}')

    original_inode = APP.stat().st_ino
    try:
        exchange_directories(APP, stage)
        verify_candidate(APP, version, asar)
    except BaseException:
        if stage.exists() and stage.stat().st_ino == original_inode:
            exchange_directories(APP, stage)
        raise
    # Committed: the new app is in place. Later steps only report problems as warnings.
    finish(build, record, receipt, save, stage, previous, version, backup)


def warn(message):
    print(f'Warning: {message}', file=sys.stderr, flush=True)


def finish(build, record, receipt, save, stage, previous, version, backup):
    kept = stage
    try:
        os.rename(stage, previous)
        kept = previous
    except OSError as error:
        warn(f'The previous app is still at {stage}: {error}')
    receipt.update(status='installed', installed=True, userVerificationPending=True, previousBundle=str(kept))
    try:
        save()
    except OSError as error:
        warn(f'Could not update the installation receipt: {error}')
    try:
        record.update(installed=True, installedAt=datetime.now(timezone.utc).isoformat(), backup=str(backup),
                      installationReceipt=str(backup / 'installation.json'))
        write_json(build / 'build.json', record)
    except OSError as error:
        warn(f'Could not mark the build as installed: {error}')
    try:
        if app_running():
            receipt['launch'] = 'already-running'
            say(f'Installed {version}. T3 is already running the new app.')
        else:
            launched = subprocess.run(['/usr/bin/open', str(APP)], capture_output=True, text=True).returncode == 0
            receipt['launch'] = 'requested' if launched else 'not-started'
            say(f'Installed {version}.' + (' Opening T3.' if launched else ' Open T3 from /Applications.'))
        save()
    except (OSError, Stop) as error:
        say(f'Installed {version}. Open T3 from /Applications.')
        warn(f'Could not open T3: {error}')
    say('Check your projects, conversations, attachments and sign-in. Keychain approval may be needed.')
    say(f'The previous app is kept at {kept}. Keep the backup until the new app works.')


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--build', type=Path, default=Path(__file__).resolve().parent,
                        help='Build directory containing build.json (default: this script\'s directory).')
    parser.add_argument('--check', action='store_true', help='Verify only; change nothing.')
    parser.add_argument('--yes', action='store_true', help='Send the normal quit request without asking.')
    parser.add_argument('--accept-unknown-base', action='store_true',
                        help='Replace an installed app this machine did not build.')
    args = parser.parse_args()
    try:
        install(args.build.resolve(), args.check, args.yes, args.accept_unknown_base)
    except KeyboardInterrupt:
        replaced = app_identity(APP)['asarSha256'] == json.loads(
            (args.build.resolve() / 'build.json').read_text()).get('asarSha256')
        say('\nCancelled. ' + ('The new app was already installed.' if replaced else 'The installed app was not replaced.'))
        sys.exit(130)
    except Stop as error:
        print(f'STOP: {error}', file=sys.stderr)
        print('No user data is restored or deleted. Any backup or staged copy is kept for diagnosis.', file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()

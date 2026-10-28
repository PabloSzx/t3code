"""Fixture-only checks for install-local-nightly.py. Never touches the real app or user data.

Real ditto, lsof, SQLite and RENAME_SWAP are used. Code signatures, architecture and the
launch request are stubbed for the tiny fixture bundles.
"""
import hashlib
import importlib.util
import json
from pathlib import Path
import plistlib
import shutil
import sqlite3
import subprocess
import sys
import tempfile

SCRIPT = Path(__file__).resolve().with_name('install-local-nightly.py')
spec = importlib.util.spec_from_file_location('installer', SCRIPT)
installer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(installer)
real_run = installer.run


def sha(data):
    return hashlib.sha256(data).hexdigest()


def bundle(path, version, content):
    (path / 'Contents/Resources').mkdir(parents=True)
    (path / 'Contents/MacOS').mkdir(parents=True)
    (path / 'Contents/Info.plist').write_bytes(
        plistlib.dumps({'CFBundleIdentifier': installer.BUNDLE_ID, 'CFBundleShortVersionString': version})
    )
    (path / 'Contents/Resources/app.asar').write_bytes(content)


def fixture(root):
    installer.HOME = root / 'home'
    installer.LOCAL_ROOT = installer.HOME / '.local/share/t3code'
    installer.APP = root / 'Applications/T3 Code (Nightly).app'
    installer.USERDATA = installer.HOME / '.t3/userdata'
    installer.PROFILE = installer.HOME / 'Library/Application Support/t3code-v2'
    installer.USERDATA.mkdir(parents=True)
    installer.PROFILE.mkdir(parents=True)
    (installer.USERDATA / 'settings.json').write_text('{}')
    (installer.PROFILE / 'Preferences').write_text('{}')
    connection = sqlite3.connect(installer.USERDATA / 'statev2.sqlite')
    connection.execute('CREATE TABLE conversation (text TEXT)')
    connection.commit()
    connection.close()
    base = installer.LOCAL_ROOT / 'builds/base'
    bundle(base / 'artifacts/T3 Code (Nightly).app', '1.0.0', b'base')
    (base / 'build.json').write_text(json.dumps({'version': '1.0.0', 'asarSha256': sha(b'base')}))
    bundle(installer.APP, '1.0.0', b'base')
    build = installer.LOCAL_ROOT / 'builds/candidate'
    bundle(build / 'artifacts/T3 Code (Nightly).app', '2.0.0', b'candidate')
    record = {'version': '2.0.0', 'sourceCommit': 'abc', 'asarSha256': sha(b'candidate'),
              'readyForManualInstall': True, 'installed': False}
    (build / 'build.json').write_text(json.dumps(record))
    return build


def stub_run(launches):
    def run(args):
        executable = str(args[0])
        if executable == '/usr/bin/codesign':
            return ''
        if executable == '/usr/bin/lipo':
            return 'arm64'
        return real_run(args)
    installer.run = run
    original = subprocess.run

    def subprocess_run(args, **kwargs):
        if args[0] == '/usr/bin/open':
            launches.append(args[1])
            return subprocess.CompletedProcess(args, 0, '', '')
        if args[0] == '/usr/bin/osascript':
            raise AssertionError('Fixture runs must not send a quit request')
        return original(args, **kwargs)
    installer.subprocess.run = subprocess_run
    return original


def asar(path):
    return (path / 'Contents/Resources/app.asar').read_bytes()


with tempfile.TemporaryDirectory(prefix='t3-installer-test-') as temporary:
    root = Path(temporary).resolve()
    launches = []
    original_subprocess_run = stub_run(launches)
    try:
        build = fixture(root / 'reopen')
        installer.install(build, check_only=True, assume_yes=False, accept_unknown=False)
        assert asar(installer.APP) == b'base'

        # Waits instead of failing while a process holds user data open, and keeps a backup
        # that matches what T3 wrote while it was open during the copy.
        holder = subprocess.Popen(
            [sys.executable, '-c', 'import sys,time;f=open(sys.argv[1]);time.sleep(3)',
             str(installer.USERDATA / 'settings.json')]
        )
        snapshots = []
        real_snapshot = installer.snapshot

        def snapshot(source, target):
            result = real_snapshot(source, target)
            if source == installer.USERDATA and not snapshots:
                (installer.USERDATA / 'settings.json').write_text('{"written":"while copying"}')
            snapshots.append(source)
            return result
        installer.snapshot = snapshot
        waits = []
        real_wait = installer.wait_until_closed

        def wait(paths, assume_yes):
            # The holder exits by itself; the real wait must poll until it has.
            if holder.poll() is None:
                waits.append(True)
            real_wait(paths, assume_yes)
            assert holder.poll() is not None
        installer.wait_until_closed = wait
        installer.WAIT_INTERVAL_SECONDS = 0.2
        installer.install(build, check_only=False, assume_yes=False, accept_unknown=False)
        holder.wait()
        installer.snapshot = real_snapshot
        installer.wait_until_closed = real_wait
        assert waits, 'expected the installer to wait for the open file'
        assert asar(installer.APP) == b'candidate'
        record = json.loads((build / 'build.json').read_text())
        receipt = json.loads(Path(record['installationReceipt']).read_text())
        backup = Path(receipt['backup'])
        assert receipt['installed'] is True and receipt['status'] == 'installed'
        assert (backup / 'userdata/settings.json').read_text() == '{"written":"while copying"}'
        assert asar(backup / 'T3 Code (Nightly).app') == b'base'
        assert asar(Path(receipt['previousBundle'])) == b'base'
        assert launches == [str(installer.APP)]

        # Running it again after a successful install is a no-op, not an error.
        installer.install(build, check_only=False, assume_yes=False, accept_unknown=False)
        assert len(launches) == 1

        # An app this machine never built is refused unless explicitly accepted.
        build = fixture(root / 'unknown')
        shutil.rmtree(installer.APP)
        bundle(installer.APP, '0.9.0', b'someone else')
        try:
            installer.install(build, check_only=True, assume_yes=False, accept_unknown=False)
        except installer.Stop as error:
            assert 'not a build this machine produced' in str(error)
        else:
            raise AssertionError('expected an unknown base to be refused')
        installer.install(build, check_only=True, assume_yes=False, accept_unknown=True)
    finally:
        installer.subprocess.run = original_subprocess_run
        installer.run = real_run

print('install-local-nightly fixture checks passed')

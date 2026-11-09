import { GitCommandError, type ProjectId, type VcsCreateWorktreeInput } from "@t3tools/contracts";
import { buildTemporaryWorktreeBranchName } from "@t3tools/shared/git";
import { resolveProjectSettings } from "@t3tools/shared/projectSettings";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Context from "effect/Context";
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeChildProcessSpawner from "@effect/platform-node/NodeChildProcessSpawner";
import * as SecureRandom from "effect/crypto/SecureRandom";
import * as Effect from "effect/Effect";
import * as Hex from "effect/encoding/Hex";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import { LeaseInventory, type LeaseInventoryEntry } from "../workspace/LeaseCleanupQueue.ts";
import type * as Scope from "effect/Scope";

import * as ProcessRunner from "../processRunner.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as TreehouseLeases from "../workspace/treehouseLeases.ts";
import { withWorkspaceLease } from "../workspace/workspaceLease.ts";
import {
  TreehouseLease,
  TreehouseLeaseError,
  treehousePoolForPath,
} from "../workspace/treehouseLeases.ts";

export interface TreehouseCreateOptions {
  readonly preparationScope: Scope.Scope;
}

export interface TreehouseIdleReturnOptions {
  readonly cwd: string;
  readonly path: string;
  readonly branch: string;
  readonly headSha: string;
  readonly threadIds: readonly string[];
  readonly recheck: Effect.Effect<string | null, GitCommandError>;
}
export interface TreehouseIdleDestroyOptions extends TreehouseIdleReturnOptions {
  readonly leaseId: string;
}
export interface TreehouseResumeOptions {
  readonly projectId?: ProjectId;
  readonly cwd: string;
  readonly path: string;
  readonly branch: string;
  readonly threadId: string;
  readonly preparationScope: Scope.Scope;
}
export class TreehouseWorktrees extends Context.Service<
  TreehouseWorktrees,
  {
    readonly listLeases: Effect.Effect<readonly LeaseInventoryEntry[], GitCommandError>;
    readonly create: (
      input: VcsCreateWorktreeInput,
      options: TreehouseCreateOptions,
    ) => Effect.Effect<string | null, GitCommandError>;
    readonly returnIdle: (
      input: TreehouseIdleReturnOptions,
    ) => Effect.Effect<string | null, GitCommandError>;
    readonly destroyIdle: (
      input: TreehouseIdleDestroyOptions,
    ) => Effect.Effect<string | null, GitCommandError>;
    readonly resume: (
      input: TreehouseResumeOptions,
    ) => Effect.Effect<string | null, GitCommandError>;
    readonly remove: (input: {
      cwd: string;
      path: string;
    }) => Effect.Effect<boolean, GitCommandError>;
  }
>()("t3/vcs/TreehouseWorktrees") {}

const isGitError = Schema.is(GitCommandError);
const isLeaseError = Schema.is(TreehouseLeaseError);
const isSpawnError = Schema.is(ProcessRunner.ProcessSpawnError);
const isTimeoutError = Schema.is(ProcessRunner.ProcessTimeoutError);
const TREEHOUSE_COMMAND_TIMEOUT = "15 minutes";
const decodeLease = Schema.decodeEffect(Schema.fromJsonString(TreehouseLease));
const decodeInventoryLease = Schema.decodeUnknownEffect(TreehouseLease);
const decodeAcquiredAt = Schema.decodeEffect(Schema.DateTimeUtcFromString);

const make = Effect.gen(function* () {
  const runner = yield* ProcessRunner.ProcessRunner;
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const random = yield* SecureRandom.SecureRandom;
  const settings = yield* ServerSettings.ServerSettingsService;
  const leases = yield* TreehouseLeases.TreehouseLeaseStore.pipe(
    Effect.provide(TreehouseLeases.layer),
  );

  const run = Effect.fn("TreehouseWorktrees.run")(function* (input: {
    cwd: string;
    command: string;
    args: ReadonlyArray<string>;
    trimOutput?: boolean;
  }) {
    const { trimOutput = true, ...command } = input;
    const output = yield* runner.run({
      ...command,
      ...(input.command === "git" ? {} : { stdin: "" }),
      timeout: TREEHOUSE_COMMAND_TIMEOUT,
      maxOutputBytes: 1024 * 1024,
    });
    if (output.code !== 0 || output.stdoutTruncated || output.stderrTruncated) {
      return yield* new GitCommandError({
        operation: "TreehouseWorktrees.run",
        command: input.command,
        cwd: input.cwd,
        detail:
          "Treehouse worktree operation failed. T3 retains ownership evidence; inspect the pool before retrying.",
        cause: output,
      });
    }
    return trimOutput ? output.stdout.trim() : output.stdout;
  });
  const mapError = (input: { cwd: string; cause: unknown }) =>
    isGitError(input.cause)
      ? input.cause
      : new GitCommandError({
          operation: "TreehouseWorktrees.lifecycle",
          command: "treehouse",
          cwd: input.cwd,
          detail: isLeaseError(input.cause)
            ? input.cause.message
            : isSpawnError(input.cause)
              ? "Cannot start Treehouse on this environment. Install a compatible CLI on the host or set its absolute binary path in Settings. No native Git fallback is performed."
              : isTimeoutError(input.cause)
                ? `${input.cause.command === "git" ? "Git" : "Treehouse"} did not finish within ${TREEHOUSE_COMMAND_TIMEOUT}. The worktree pool may be busy with other allocations; retry once it settles. No native Git fallback is performed.`
                : "Cannot verify Treehouse worktree ownership. No native Git fallback is performed.",
          cause: input.cause,
        });

  const create = Effect.fn("TreehouseWorktrees.create")(
    function* (
      input: VcsCreateWorktreeInput,
      options: TreehouseCreateOptions & { readonly resume?: TreehouseLeases.OwnedTreehouseLease },
    ) {
      // Explicit paths are recovery requests, not permission to recreate a missing pooled checkout.
      if (
        input.path !== null &&
        input.path !== undefined &&
        ((yield* treehousePoolForPath(input.path)) !== null ||
          (yield* leases.knownPath(input.path)))
      ) {
        return yield* new TreehouseLeaseError({ path: input.path, reason: "missing" });
      }
      const current = yield* settings.getSettings;
      if (options.resume === undefined && !current.treehouseWorktreesEnabled) return null;
      if (input.path != null)
        return yield* new GitCommandError({
          operation: "TreehouseWorktrees.create",
          command: "treehouse get",
          cwd: input.cwd,
          detail: "Treehouse chooses the pool path; an explicit worktree path is not supported.",
        });
      const binary = options.resume?.record.binary ?? (current.treehouseBinaryPath || "treehouse");
      if (binary !== "treehouse" && !path.isAbsolute(binary))
        return yield* new GitCommandError({
          operation: "TreehouseWorktrees.create",
          command: "treehouse",
          cwd: input.cwd,
          detail:
            "Treehouse binary path must be an absolute executable path on this environment's host. Change it in Settings or leave it empty to use the host PATH.",
        });
      const commit = yield* run({
        cwd: input.cwd,
        command: "git",
        args: ["rev-parse", "--verify", "--end-of-options", `${input.refName}^{commit}`],
      });
      const resume = options.resume?.record.release?.resume;
      const holder = resume?.holder ?? `t3-code:${Hex.encode(yield* random.randomBytes(16))}`;
      const lease = yield* Effect.gen(function* () {
        if (resume?.allocationStarted) {
          if (resume.lease === undefined)
            return yield* new TreehouseLeaseError({
              path: options.resume!.record.lease.path,
              reason: "resume-incomplete",
            });
          const pool = options.resume?.record.pool;
          if (pool === undefined)
            return yield* new TreehouseLeaseError({ path: input.cwd, reason: "metadata" });
          const inventory = yield* run({
            cwd: input.cwd,
            command: binary,
            args: ["leases", "--json", "--pool", pool],
          }).pipe(Effect.flatMap(Schema.decodeEffect(Schema.fromJsonString(LeaseInventory))));
          const matches = inventory.filter((entry) => entry.lease_holder === holder);
          const candidate = matches[0];
          if (
            matches.length !== 1 ||
            candidate === undefined ||
            candidate.quarantined ||
            candidate.pool !== pool
          )
            return yield* new TreehouseLeaseError({
              path: options.resume!.record.lease.path,
              reason: "resume-incomplete",
            });
          return yield* decodeInventoryLease(candidate);
        }
        if (options.resume !== undefined)
          yield* leases.withLock({
            path: options.resume.record.lease.path,
            effect: leases.startResumeAllocation(options.resume),
          });
        // --base accepts branch names only. Acquire detached, then let Git interpret the exact caller ref.
        const stdout = yield* run({
          cwd: input.cwd,
          command: binary,
          args: ["get", "--lease", "--json", "--lease-holder", holder, "--no-fetch"],
        });
        return yield* decodeLease(stdout);
      });
      if (lease.lease_holder !== holder || !path.isAbsolute(lease.path))
        return yield* new TreehouseLeaseError({ path: lease.path, reason: "unknown-lease" });
      if (options.resume !== undefined && !resume?.allocationStarted)
        yield* leases.withLock({
          path: options.resume.record.lease.path,
          effect: leases.finishResumeAllocation({ owned: options.resume, lease }),
        });
      const pool = yield* treehousePoolForPath(lease.path);
      if (pool === null)
        return yield* new TreehouseLeaseError({ path: lease.path, reason: "metadata" });
      return yield* leases.withLock({
        path: lease.path,
        effect: Effect.gen(function* () {
          const canonical = yield* fs.realPath(lease.path);
          yield* leases.register({
            lease: { ...lease, path: canonical },
            binary,
            pool,
            preparationScope: options.preparationScope,
            ...(options.resume === undefined ? {} : { resume: options.resume }),
          });
          yield* leases.read(canonical);
          // Validate repository identity before switching any checkout returned by the external allocator.
          const common = yield* run({
            cwd: input.cwd,
            command: "git",
            args: ["rev-parse", "--path-format=absolute", "--git-common-dir"],
          });
          const allocatedCommon = yield* run({
            cwd: canonical,
            command: "git",
            args: ["rev-parse", "--path-format=absolute", "--git-common-dir"],
          });
          if ((yield* fs.realPath(common)) !== (yield* fs.realPath(allocatedCommon)))
            return yield* new TreehouseLeaseError({ path: canonical, reason: "unknown-lease" });
          const local = yield* runner.run({
            command: "git",
            args: ["show-ref", "--verify", "--quiet", `refs/heads/${input.refName}`],
            cwd: input.cwd,
          });
          if (local.code !== 0 && local.code !== 1)
            return yield* new TreehouseLeaseError({
              path: canonical,
              reason: "metadata",
              cause: local,
            });
          if (resume?.allocationStarted) {
            const branch = yield* runner.run({
              command: "git",
              args: ["symbolic-ref", "--quiet", "HEAD"],
              cwd: canonical,
            });
            if (
              branch.code === 0 &&
              branch.stdout.trim() === `refs/heads/${input.newRefName ?? input.refName}`
            )
              return canonical;
            if (branch.code !== 1)
              return yield* new TreehouseLeaseError({ path: canonical, reason: "stale-binding" });
            const changes = yield* run({
              cwd: canonical,
              command: "git",
              args: ["status", "--porcelain=v1", "--untracked-files=normal", "-z"],
            });
            if (changes !== "")
              return yield* new TreehouseLeaseError({ path: canonical, reason: "in-use" });
          }
          yield* run({
            cwd: canonical,
            command: "git",
            args: input.newRefName
              ? ["switch", "--no-guess", "-c", input.newRefName, "--", commit]
              : [
                  "switch",
                  "--no-guess",
                  ...(local.code === 0 ? [] : ["--detach"]),
                  "--",
                  local.code === 0 ? input.refName : commit,
                ],
          });
          return canonical;
        }),
      });
    },
    (effect, input) =>
      effect.pipe(
        Effect.provideService(FileSystem.FileSystem, fs),
        Effect.provideService(Path.Path, path),
        Effect.mapError((cause) => mapError({ cwd: input.cwd, cause })),
      ),
  );
  return TreehouseWorktrees.of({
    listLeases: Effect.gen(function* () {
      const current = yield* settings.getSettings;
      const pools = new Set<string>();
      for (const held of yield* leases.listHeld()) {
        const pool = yield* treehousePoolForPath(held.path);
        if (pool !== null) pools.add(pool);
      }
      return yield* run({
        cwd: yield* fs.realPath(yield* HostProcess.HomeDirectory),
        command: current.treehouseBinaryPath || "treehouse",
        args: ["leases", "--json", ...[...pools].flatMap((pool) => ["--pool", pool])],
      }).pipe(Effect.flatMap(Schema.decodeEffect(Schema.fromJsonString(LeaseInventory))));
    }).pipe(
      Effect.provideService(FileSystem.FileSystem, fs),
      Effect.provideService(Path.Path, path),
      Effect.mapError((cause) => mapError({ cwd: "Treehouse lease catalogue", cause })),
    ),
    create,
    returnIdle: Effect.fn("TreehouseWorktrees.returnIdle")(
      function* (input) {
        return yield* withWorkspaceLease(
          path.resolve(input.path),
          leases.withLock({
            path: input.path,
            effect: Effect.gen(function* () {
              const owned = yield* leases.read(input.path);
              if (owned === null)
                return yield* new TreehouseLeaseError({
                  path: input.path,
                  reason: "unknown-lease",
                });
              if ((yield* fs.realPath(input.cwd)) === (yield* fs.realPath(input.path)))
                return "contains a project checkout";
              const blocked = yield* input.recheck;
              if (blocked !== null) return blocked;
              const common = yield* run({
                cwd: input.cwd,
                command: "git",
                args: ["rev-parse", "--path-format=absolute", "--git-common-dir"],
              });
              const actual = yield* run({
                cwd: input.path,
                command: "git",
                args: ["rev-parse", "--path-format=absolute", "--git-common-dir"],
              });
              const branch = yield* run({
                cwd: input.path,
                command: "git",
                args: ["symbolic-ref", "HEAD"],
              });
              const head = yield* run({
                cwd: input.path,
                command: "git",
                args: ["rev-parse", "HEAD"],
              });
              const retained = yield* run({
                cwd: input.cwd,
                command: "git",
                args: [
                  "rev-parse",
                  "--verify",
                  "--end-of-options",
                  `refs/heads/${input.branch}^{commit}`,
                ],
              });
              if (
                (yield* fs.realPath(common)) !== (yield* fs.realPath(actual)) ||
                branch !== `refs/heads/${input.branch}` ||
                head !== input.headSha ||
                retained !== head
              )
                return "repository, branch, or commit changed since check";
              // Returning a pool slot retains ignored files for its next lease.
              const changes = yield* run({
                cwd: input.path,
                command: "git",
                args: [
                  "status",
                  "--porcelain=v1",
                  "-z",
                  "--untracked-files=normal",
                  "--ignored=matching",
                  "--ignore-submodules=none",
                ],
                trimOutput: false,
              });
              const checkout = yield* fs.realPath(input.path);
              for (const entry of changes.split("\0")) {
                if (entry === "") continue;
                if (!entry.startsWith("!! ")) return "Treehouse slot has local files";
                const relative = entry.slice(3).replace(/\/$/, "");
                if (
                  (yield* fs.realPath(path.join(input.path, relative))) !==
                  path.join(checkout, relative)
                )
                  return "Treehouse slot has ignored symbolic links";
              }
              const submodules = yield* run({
                cwd: input.path,
                command: "git",
                args: ["submodule", "status"],
              });
              const admin = yield* run({
                cwd: input.path,
                command: "git",
                args: ["rev-parse", "--absolute-git-dir"],
              });
              if (
                submodules !== "" ||
                (yield* fs.exists(path.join(admin, "modules"))) ||
                (yield* fs.exists(path.join(input.path, ".gitmodules")))
              )
                return "Treehouse slot has submodules";
              const releasing = yield* leases.beginCleanupReturn({
                owned,
                release: {
                  projectPath: yield* fs.realPath(input.cwd),
                  repository: yield* fs.realPath(common),
                  branch: input.branch,
                  headSha: head,
                  threadIds: [...input.threadIds],
                },
              });
              yield* run({
                cwd: input.cwd,
                command: owned.record.binary,
                args: [
                  "return",
                  owned.record.lease.path,
                  "--if-lease-id",
                  owned.record.lease.lease_id,
                ],
              });
              yield* leases.markReturned(releasing);
              return null;
            }),
          }),
        );
      },
      (effect, input) =>
        effect.pipe(
          Effect.provideService(FileSystem.FileSystem, fs),
          Effect.provideService(Path.Path, path),
          Effect.mapError((cause) => mapError({ cwd: input.cwd, cause })),
        ),
    ),
    destroyIdle: Effect.fn("TreehouseWorktrees.destroyIdle")(
      function* (input) {
        if (yield* HostProcess.isWindows)
          return "Treehouse expiry deletion needs a junction-safe Windows backend";
        return yield* withWorkspaceLease(
          path.resolve(input.path),
          leases.withLock({
            path: input.path,
            effect: Effect.gen(function* () {
              const owned = yield* leases.read(input.path);
              if (owned === null || owned.record.lease.lease_id !== input.leaseId)
                return "Treehouse lease generation changed since check";
              const checkout = yield* fs.realPath(input.path);
              if (checkout !== input.path || checkout === (yield* fs.realPath(input.cwd)))
                return "worktree path is not a canonical linked checkout";
              const blocked = yield* input.recheck;
              if (blocked !== null) return blocked;
              const commonArgs = ["rev-parse", "--path-format=absolute", "--git-common-dir"];
              const repository = yield* run({ cwd: input.cwd, command: "git", args: commonArgs });
              const actual = yield* run({ cwd: input.path, command: "git", args: commonArgs });
              const head = yield* run({
                cwd: input.path,
                command: "git",
                args: ["rev-parse", "HEAD"],
              });
              if (
                (yield* fs.realPath(repository)) !== (yield* fs.realPath(actual)) ||
                head !== input.headSha
              )
                return "repository or commit changed since check";
              const help = yield* run({
                cwd: input.cwd,
                command: owned.record.binary,
                args: ["destroy", "--help"],
              });
              if (
                !help.includes("--if-lease-id") ||
                !help.includes("--expire-after") ||
                !help.includes("--terminate-agent-sessions")
              )
                return "recorded Treehouse binary needs verified process-terminating destroy support";
              const acquired = yield* decodeAcquiredAt(owned.record.lease.leased_at);
              if (DateTime.toEpochMillis(acquired) > (yield* Clock.currentTimeMillis))
                return "lease acquisition time cannot be verified";
              const blockedBeforeReturn = yield* input.recheck;
              if (blockedBeforeReturn !== null) return blockedBeforeReturn;
              const releasing = yield* leases.beginCleanupReturn({
                owned,
                discardLocalFiles: true,
                release: {
                  projectPath: yield* fs.realPath(input.cwd),
                  repository: yield* fs.realPath(repository),
                  branch: input.branch,
                  headSha: head,
                  threadIds: [...input.threadIds],
                },
              });
              const destroyed = yield* Effect.gen(function* () {
                yield* run({
                  cwd: input.cwd,
                  command: owned.record.binary,
                  args: [
                    "destroy",
                    owned.record.lease.path,
                    "--yes",
                    "--include-leased",
                    "--include-unlanded",
                    "--include-in-use",
                    "--if-lease-id",
                    input.leaseId,
                    // T3 rechecks the configured deadline; the allocator verifies this exact acquisition.
                    "--expire-after",
                    "1ns",
                    "--terminate-agent-sessions",
                  ],
                });
                yield* leases.markReturned(releasing);
                return null;
              }).pipe(Effect.result);
              if (destroyed._tag === "Failure") {
                if (yield* fs.exists(input.path))
                  yield* leases
                    .cancelCleanupReturn(releasing)
                    .pipe(Effect.ignoreCause({ log: true }));
                return yield* destroyed.failure;
              }
              return destroyed.success;
            }),
          }),
        );
      },
      (effect, input) =>
        effect.pipe(
          Effect.provideService(FileSystem.FileSystem, fs),
          Effect.provideService(Path.Path, path),
          Effect.mapError((cause) => mapError({ cwd: input.cwd, cause })),
        ),
    ),
    resume: Effect.fn("TreehouseWorktrees.resume")(
      function* (input) {
        return yield* withWorkspaceLease(
          `treehouse-resume:${input.threadId}`,
          Effect.gen(function* () {
            let released = yield* leases.releasedThread(input);
            if (released === null) return null;
            return yield* withWorkspaceLease(
              `treehouse-resume-lease:${released.record.lease.lease_id}`,
              Effect.gen(function* () {
                released = yield* leases.releasedThread(input);
                if (
                  released === null ||
                  !released.record.returned ||
                  released.record.release === undefined
                )
                  return yield* new TreehouseLeaseError({ path: input.path, reason: "in-use" });
                const release = released.record.release;
                const common = yield* run({
                  cwd: input.cwd,
                  command: "git",
                  args: ["rev-parse", "--path-format=absolute", "--git-common-dir"],
                });
                if (
                  (yield* fs.realPath(input.cwd)) !== release.projectPath ||
                  (yield* fs.realPath(common)) !== release.repository ||
                  input.branch !== release.branch
                )
                  return yield* new TreehouseLeaseError({
                    path: input.path,
                    reason: "stale-binding",
                  });
                yield* run({
                  cwd: input.cwd,
                  command: "git",
                  args: [
                    "rev-parse",
                    "--verify",
                    "--end-of-options",
                    `refs/heads/${input.branch}^{commit}`,
                  ],
                });
                yield* run({
                  cwd: input.cwd,
                  command: "git",
                  args: ["cat-file", "-e", `${release.headSha}^{commit}`],
                });
                if (release.replacement !== undefined) {
                  const owned = yield* leases.read(release.replacement.path);
                  if (owned?.record.lease.lease_id !== release.replacement.leaseId)
                    return yield* new TreehouseLeaseError({
                      path: input.path,
                      reason: "stale-binding",
                    });
                  const actual = yield* run({
                    cwd: release.replacement.path,
                    command: "git",
                    args: ["rev-parse", "--path-format=absolute", "--git-common-dir"],
                  });
                  const branch = yield* run({
                    cwd: release.replacement.path,
                    command: "git",
                    args: ["symbolic-ref", "HEAD"],
                  });
                  if (
                    (yield* fs.realPath(common)) !== (yield* fs.realPath(actual)) ||
                    branch !== `refs/heads/${release.resume?.branch ?? input.branch}`
                  )
                    return yield* new TreehouseLeaseError({
                      path: input.path,
                      reason: "stale-binding",
                    });
                  return release.replacement.path;
                }
                let recoveryBranch = release.resume?.branch;
                if (recoveryBranch === undefined) {
                  const worktrees = yield* run({
                    cwd: input.cwd,
                    command: "git",
                    args: ["worktree", "list", "--porcelain", "-z"],
                    trimOutput: false,
                  });
                  const occupied = worktrees.split("\0\0").some((entry) => {
                    const fields = entry.split("\0");
                    return (
                      fields.includes(`branch refs/heads/${input.branch}`) &&
                      !fields.includes(`worktree ${release.resume?.lease?.path}`)
                    );
                  });
                  if (occupied) {
                    const current = yield* settings.getSettings;
                    const naming =
                      input.projectId === undefined
                        ? current
                        : resolveProjectSettings(current, input.projectId).settings;
                    const token = Hex.encode(yield* random.randomBytes(16));
                    recoveryBranch = buildTemporaryWorktreeBranchName({
                      randomHex: () => token,
                      naming: {
                        mode: naming.branchNamingMode,
                        prefix: naming.branchNamePrefix,
                        instructions: naming.branchNameInstructions,
                      },
                    });
                  }
                }
                const resuming = yield* leases.withLock({
                  path: input.path,
                  effect: leases.beginResume({
                    owned: released,
                    holder: `t3-code:${Hex.encode(yield* random.randomBytes(16))}`,
                    ...(recoveryBranch === undefined ? {} : { branch: recoveryBranch }),
                  }),
                });
                const created = yield* create(
                  {
                    cwd: input.cwd,
                    refName: recoveryBranch === undefined ? input.branch : release.headSha,
                    ...(recoveryBranch === undefined ? {} : { newRefName: recoveryBranch }),
                    path: null,
                  },
                  {
                    preparationScope: input.preparationScope,
                    resume: resuming,
                  },
                );
                if (created === null)
                  return yield* new TreehouseLeaseError({ path: input.path, reason: "missing" });
                yield* leases.withLock({
                  path: created,
                  effect: leases.setReplacement({ owned: resuming, path: created }),
                });
                return created;
              }),
            );
          }),
        );
      },
      (effect, input) =>
        effect.pipe(
          Effect.provideService(FileSystem.FileSystem, fs),
          Effect.provideService(Path.Path, path),
          Effect.mapError((cause) => mapError({ cwd: input.cwd, cause })),
        ),
    ),
    remove: Effect.fn("TreehouseWorktrees.remove")(
      function* (input) {
        if ((yield* leases.read(input.path)) === null) return false;
        return yield* withWorkspaceLease(
          path.resolve(input.path),
          leases.withLock({
            path: input.path,
            effect: Effect.gen(function* () {
              const owned = yield* leases.read(input.path);
              if (owned === null) return false;
              yield* leases.assertReturnable(owned);
              yield* run({
                cwd: input.cwd,
                command: owned.record.binary,
                args: [
                  "return",
                  owned.record.lease.path,
                  "--if-lease-id",
                  owned.record.lease.lease_id,
                ],
              });
              yield* leases.markReturned(owned);
              return true;
            }),
          }),
        );
      },
      (effect, input) =>
        effect.pipe(
          Effect.provideService(FileSystem.FileSystem, fs),
          Effect.provideService(Path.Path, path),
          Effect.mapError((cause) => mapError({ cwd: input.cwd, cause })),
        ),
    ),
  });
});

export const layer = Layer.effect(TreehouseWorktrees, make).pipe(
  Layer.provide(ProcessRunner.layer.pipe(Layer.provide(NodeChildProcessSpawner.layer))),
  Layer.provide(NodeCrypto.layer),
);

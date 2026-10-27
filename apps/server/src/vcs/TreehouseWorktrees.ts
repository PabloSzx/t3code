import { GitCommandError, type VcsCreateWorktreeInput } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Hex from "effect/encoding/Hex";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
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

export class TreehouseWorktrees extends Context.Service<
  TreehouseWorktrees,
  {
    readonly create: (
      input: VcsCreateWorktreeInput,
      options: TreehouseCreateOptions,
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
const decodeLease = Schema.decodeEffect(Schema.fromJsonString(TreehouseLease));

const make = Effect.gen(function* () {
  const runner = yield* ProcessRunner.ProcessRunner;
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const crypto = yield* Crypto.Crypto;
  const settings = yield* ServerSettings.ServerSettingsService;
  const leases = yield* TreehouseLeases.TreehouseLeaseStore.pipe(
    Effect.provide(TreehouseLeases.layer),
  );

  const run = Effect.fn("TreehouseWorktrees.run")(function* (input: {
    cwd: string;
    command: string;
    args: ReadonlyArray<string>;
  }) {
    const output = yield* runner.run({
      ...input,
      stdin: "",
      timeout: "15 minutes",
      maxOutputBytes: 1024 * 1024,
    });
    if (output.code !== 0) {
      return yield* new GitCommandError({
        operation: "TreehouseWorktrees.run",
        command: input.command,
        cwd: input.cwd,
        detail:
          "Treehouse worktree operation failed. The lease is retained; inspect the pool before retrying.",
        cause: output,
      });
    }
    return output.stdout.trim();
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
              : "Cannot verify Treehouse worktree ownership. No native Git fallback is performed.",
          cause: input.cause,
        });

  return TreehouseWorktrees.of({
    create: Effect.fn("TreehouseWorktrees.create")(
      function* (input, options) {
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
        if (!current.treehouseWorktreesEnabled) return null;
        if (input.path != null)
          return yield* new GitCommandError({
            operation: "TreehouseWorktrees.create",
            command: "treehouse get",
            cwd: input.cwd,
            detail: "Treehouse chooses the pool path; an explicit worktree path is not supported.",
          });
        const binary = current.treehouseBinaryPath || "treehouse";
        if (current.treehouseBinaryPath !== "" && !path.isAbsolute(binary))
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
        const holder = `t3-code:${Hex.encode(yield* crypto.randomBytes(16))}`;
        // --base accepts branch names only. Acquire detached, then let Git interpret the exact caller ref.
        const stdout = yield* run({
          cwd: input.cwd,
          command: binary,
          args: ["get", "--lease", "--json", "--lease-holder", holder, "--no-fetch"],
        });
        const lease = yield* decodeLease(stdout);
        if (lease.lease_holder !== holder || !path.isAbsolute(lease.path))
          return yield* new TreehouseLeaseError({ path: lease.path, reason: "unknown-lease" });
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
              stdin: "",
            });
            if (local.code !== 0 && local.code !== 1)
              return yield* new TreehouseLeaseError({
                path: canonical,
                reason: "metadata",
                cause: local,
              });
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
  Layer.provide(ProcessRunner.layer),
);

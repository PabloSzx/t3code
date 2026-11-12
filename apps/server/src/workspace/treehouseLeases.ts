import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Base64Url from "effect/encoding/Base64Url";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import type * as PlatformError from "effect/PlatformError";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as ServerConfig from "../config.ts";
import { isProcessAlive, readPersistedServerRuntimeState } from "../serverRuntimeState.ts";
import { withWorkspaceLease } from "./workspaceLease.ts";
import { ThreadId, type WorkspaceOperationContext } from "@t3tools/contracts";
import { isNetworkPath } from "@t3tools/shared/path";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as WorkspaceLifecycle from "./WorkspaceLifecycle.ts";

const LeaseId = Schema.String.check(Schema.isPattern(/^[a-f0-9]{32}$/));
export const TreehouseLease = Schema.Struct({
  path: Schema.String,
  lease_id: LeaseId,
  lease_holder: Schema.String,
  leased_at: Schema.String,
  base_branch: Schema.optional(Schema.String),
});
const Resume = Schema.Struct({
  branch: Schema.optional(Schema.String),
  holder: Schema.String.check(Schema.isPattern(/^t3-code:[a-f0-9]{32}$/)),
  allocationStarted: Schema.Boolean,
  lease: Schema.optional(TreehouseLease),
  ownerPid: Schema.optional(
    Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 2_147_483_647 })),
  ),
  ownerId: Schema.optional(Schema.String),
});
const Release = Schema.Struct({
  projectPath: Schema.String,
  repository: Schema.String,
  branch: Schema.String,
  headSha: Schema.String,
  threadIds: Schema.Array(Schema.String),
  resuming: Schema.optional(Schema.Boolean),
  resume: Schema.optional(Resume),
  abandonedResumes: Schema.optional(
    Schema.Array(Schema.Struct({ resume: Schema.optional(Resume), abandonedAt: Schema.String })),
  ),
  replacement: Schema.optional(Schema.Struct({ path: Schema.String, leaseId: LeaseId })),
});
const Record = Schema.Struct({
  lease: TreehouseLease,
  binary: Schema.String,
  pool: Schema.optional(Schema.String),
  returned: Schema.Boolean,
  release: Schema.optional(Release),
});
const Index = Schema.Array(Schema.Struct({ path: Schema.String, pool: Schema.String }));
const Binding = Schema.Struct({
  path: Schema.String,
  leaseId: LeaseId,
  active: Schema.Boolean,
  sequence: Schema.Number,
  resumedFrom: Schema.optional(Schema.Struct({ path: Schema.String, leaseId: LeaseId })),
});
const Use = Schema.Struct({
  path: Schema.String,
  leaseId: LeaseId,
  kind: Schema.Literals(["provider", "terminal", "preparation", "operation"]),
  ownerPid: Schema.optional(
    Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 2_147_483_647 })),
  ),
  ownerId: Schema.optional(Schema.String),
});
const PoolState = Schema.Struct({
  version: Schema.Number,
  worktrees: Schema.Array(
    Schema.Struct({
      path: Schema.String,
      leased: Schema.optional(Schema.Boolean),
      lease_id: Schema.optional(Schema.String),
      lease_holder: Schema.optional(Schema.String),
      leased_at: Schema.optional(Schema.String),
      destroying: Schema.optional(Schema.Boolean),
      seeded_paths: Schema.optional(Schema.Array(Schema.String)),
      recovery_error: Schema.optional(Schema.String),
      recovery_reason: Schema.optional(Schema.String),
    }),
  ),
});
export class TreehouseLeaseError extends Schema.TaggedError<TreehouseLeaseError>()(
  "TreehouseLeaseError",
  {
    path: Schema.String,
    reason: Schema.Literals([
      "metadata",
      "unknown-lease",
      "stale-binding",
      "in-use",
      "missing",
      "update-required",
      "resume-incomplete",
    ]),
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    if (this.reason === "resume-incomplete")
      return `An interrupted workspace resume at ${this.path} needs allocation ownership verification. T3 preserves the reservation and any partial checkout instead of allocating again.`;
    return `Treehouse workspace ${this.path} is unavailable (${this.reason}). T3 retains its ownership records rather than using or deleting an unverified slot.`;
  }
}
export const isTreehouseLeaseError = Schema.is(TreehouseLeaseError);
const encodeJson = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));
const encodeRelease = Schema.encodeEffect(Schema.fromJsonString(Release));
const decodeLeaseId = Schema.decodeEffect(LeaseId);
const isLeaseId = Schema.is(LeaseId);
const metadataError = (input: { path: string; cause: unknown }) =>
  isTreehouseLeaseError(input.cause)
    ? input.cause
    : new TreehouseLeaseError({ ...input, reason: "metadata" });

// Only read Treehouse's atomic snapshot; never write to its pool or run status scans.
const isAbsentPath = (error: PlatformError.PlatformError) => {
  const reason = error.reason;
  return (
    reason._tag === "NotFound" ||
    (reason._tag === "BadResource" &&
      typeof reason.cause === "object" &&
      reason.cause !== null &&
      "code" in reason.cause &&
      reason.cause.code === "ENOTDIR")
  );
};

export const treehousePoolForPath = Effect.fn("TreehouseLeases.poolForPath")(
  function* (target: string) {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    let current = path.resolve(target);
    const suffix: string[] = [];
    while (true) {
      const real = yield* fs
        .realPath(current)
        .pipe(Effect.catchIf(isAbsentPath, () => Effect.succeed(null)));
      if (real !== null) {
        current = path.join(real, ...suffix);
        break;
      }
      const parent = path.dirname(current);
      if (parent === current) break;
      suffix.unshift(path.basename(current));
      current = parent;
    }
    while (true) {
      const present = yield* fs.stat(path.join(current, "treehouse-state.json")).pipe(
        Effect.as(true),
        Effect.catchIf(isAbsentPath, () => Effect.succeed(false)),
      );
      if (present) return current;
      const parent = path.dirname(current);
      if (parent === current) return null;
      current = parent;
    }
  },
  (effect, target) =>
    effect.pipe(Effect.mapError((cause) => metadataError({ path: target, cause }))),
);

export type OwnedTreehouseLease = { recordPath: string; record: typeof Record.Type };
type Owned = OwnedTreehouseLease;
export interface TreehouseCleanupReturnOptions {
  readonly owned: Owned;
  readonly release: typeof Release.Type;
  readonly discardLocalFiles?: boolean;
}
export interface TreehouseReleasedThreadOptions {
  readonly path: string;
  readonly threadId: string;
}
export interface TreehouseOperationOptions {
  readonly targets: readonly string[];
  readonly context?: WorkspaceOperationContext;
}
export class TreehouseLeaseStore extends Context.Service<
  TreehouseLeaseStore,
  {
    readonly read: (target: string) => Effect.Effect<Owned | null, TreehouseLeaseError>;
    readonly listHeld: () => Effect.Effect<
      ReadonlyArray<{
        readonly path: string;
        readonly leaseId: string | null;
        readonly verified: boolean;
      }>,
      TreehouseLeaseError
    >;
    readonly knownPath: (target: string) => Effect.Effect<boolean, TreehouseLeaseError>;
    readonly currentThreads: <
      T extends { readonly id: string; readonly deletedAt: DateTime.Utc | null },
    >(input: {
      readonly path: string;
      readonly threads: readonly T[];
    }) => Effect.Effect<
      {
        readonly threads: T[];
        readonly leaseId: string | null;
        readonly verified: boolean;
      },
      TreehouseLeaseError
    >;
    readonly register: (input: {
      lease: typeof TreehouseLease.Type;
      binary: string;
      pool: string;
      preparationScope?: Scope.Scope;
      resume?: Owned;
    }) => Effect.Effect<void, TreehouseLeaseError>;
    readonly markReturned: (owned: Owned) => Effect.Effect<void, TreehouseLeaseError>;
    readonly cancelCleanupReturn: (owned: Owned) => Effect.Effect<void, TreehouseLeaseError>;
    readonly withLock: <A, E, R>(input: {
      path: string;
      effect: Effect.Effect<A, E, R>;
    }) => Effect.Effect<A, E | TreehouseLeaseError, R>;
    readonly bindThread: (input: {
      path: string;
      threadId: string;
      sequence: number;
      sourceThreadId?: string;
      expectedPreviousPath?: string | null;
    }) => Effect.Effect<void, TreehouseLeaseError>;
    readonly commitThread: (input: {
      path: string | null;
      threadId: string;
      deleted: boolean;
      sequence: number;
    }) => Effect.Effect<void, TreehouseLeaseError>;
    readonly assertThread: (input: {
      path: string;
      threadId: string;
      requireBinding?: boolean;
    }) => Effect.Effect<void, TreehouseLeaseError>;
    readonly pinUse: (input: {
      path: string;
      threadId: string;
      kind: "provider" | "terminal";
    }) => Effect.Effect<Effect.Effect<void>, TreehouseLeaseError>;
    readonly canonicalTarget: (target: string) => Effect.Effect<string, TreehouseLeaseError>;
    readonly operationGeneration: (
      input: TreehouseReleasedThreadOptions,
    ) => Effect.Effect<string | null, TreehouseLeaseError>;
    readonly pinOperation: (
      input: TreehouseOperationOptions,
    ) => Effect.Effect<Effect.Effect<void>, TreehouseLeaseError>;
    readonly assertReturnable: (owned: Owned) => Effect.Effect<void, TreehouseLeaseError>;
    readonly assertUnused: (input: {
      readonly owned: Owned;
      readonly allowedThreadIds: readonly string[];
    }) => Effect.Effect<void, TreehouseLeaseError>;
    readonly releasedThread: (
      input: TreehouseReleasedThreadOptions,
    ) => Effect.Effect<Owned | null, TreehouseLeaseError>;
    readonly beginCleanupReturn: (
      input: TreehouseCleanupReturnOptions,
    ) => Effect.Effect<Owned, TreehouseLeaseError>;
    readonly beginResume: (input: {
      readonly owned: Owned;
      readonly holder: string;
      readonly branch?: string;
    }) => Effect.Effect<Owned, TreehouseLeaseError>;
    readonly startResumeAllocation: (owned: Owned) => Effect.Effect<void, TreehouseLeaseError>;
    readonly finishResumeAllocation: (input: {
      readonly owned: Owned;
      readonly lease: typeof TreehouseLease.Type;
    }) => Effect.Effect<void, TreehouseLeaseError>;
    readonly setReplacement: (input: {
      readonly owned: Owned;
      readonly path: string;
    }) => Effect.Effect<void, TreehouseLeaseError>;
  }
>()("t3/workspace/treehouseLeases/TreehouseLeaseStore") {}

const make = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const isWindows = yield* HostProcess.isWindows;
  const config = yield* Effect.serviceOption(ServerConfig.ServerConfig);
  const lifecycle = yield* Effect.serviceOption(WorkspaceLifecycle.WorkspaceLifecycle);
  const registry = Option.isSome(config)
    ? path.join(config.value.stateDir, "treehouse-leases")
    : null;
  const location = Effect.fn("TreehouseLeases.location")(
    function* () {
      if (registry === null || (yield* treehousePoolForPath(registry)) !== null)
        return yield* new TreehouseLeaseError({
          path: registry ?? "T3 state directory",
          reason: "metadata",
        });
      return registry;
    },
    Effect.provideService(FileSystem.FileSystem, fs),
    Effect.provideService(Path.Path, path),
  );
  const readIndex = Effect.fn("TreehouseLeases.readIndex")(function* () {
    if (registry === null) return [];
    return yield* fs.readFileString(path.join(registry, "paths.json")).pipe(
      Effect.flatMap(Schema.decodeEffect(Schema.fromJsonString(Index))),
      Effect.catchIf(
        (error) => "reason" in error && error.reason._tag === "NotFound",
        () => Effect.succeed([]),
      ),
    );
  });
  const matches = (input: { root: string; target: string }) =>
    input.root === input.target || input.target.startsWith(`${input.root}${path.sep}`);
  const knownPath = Effect.fn("TreehouseLeases.knownPath")(
    function* (target: string) {
      return (yield* readIndex()).some((entry) =>
        matches({ root: entry.path, target: path.resolve(target) }),
      );
    },
    (effect, target) =>
      effect.pipe(Effect.mapError((cause) => metadataError({ path: target, cause }))),
  );
  const writeJson = Effect.fn("TreehouseLeases.writeJson")(function* (input: {
    target: string;
    value: unknown;
    durable?: boolean;
  }) {
    yield* fs.makeDirectory(path.dirname(input.target), { recursive: true });
    const json = yield* encodeJson(input.value);
    const temp = `${input.target}.tmp`;
    yield* fs.writeFileString(temp, json, { mode: 0o600 });
    if (input.durable)
      yield* Effect.scoped(
        Effect.gen(function* () {
          const file = yield* fs.open(temp, { flag: "r+" });
          yield* file.sync;
        }),
      );
    yield* fs.rename(temp, input.target);
    if (input.durable && !isWindows)
      yield* Effect.scoped(
        Effect.gen(function* () {
          const directory = yield* fs.open(path.dirname(input.target), { flag: "r" });
          yield* directory.sync;
        }),
      );
    if (Option.isSome(lifecycle) && path.basename(input.target) !== "use.json")
      yield* lifecycle.value.invalidate;
  });
  const withLock: TreehouseLeaseStore["Service"]["withLock"] = Effect.fn(
    "TreehouseLeases.withLock",
  )(function* (input) {
    const root = yield* location();
    yield* fs
      .makeDirectory(root, { recursive: true })
      .pipe(Effect.mapError((cause) => metadataError({ path: input.path, cause })));
    const lock = path.join(root, "lifecycle.lock");
    // A crash leaves a conservative lock in T3 state, never in Treehouse's pool.
    const canonical = yield* fs
      .realPath(root)
      .pipe(Effect.mapError((cause) => metadataError({ path: input.path, cause })));
    return yield* withWorkspaceLease(
      `treehouse-lifecycle:${canonical}`,
      Effect.scoped(
        Effect.gen(function* () {
          yield* Effect.acquireRelease(
            fs
              .makeDirectory(lock)
              .pipe(Effect.mapError((cause) => metadataError({ path: input.path, cause }))),
            () => fs.remove(lock, { recursive: true }).pipe(Effect.orDie),
          );
          return yield* input.effect;
        }),
      ),
    );
  });
  const inspect = Effect.fn("TreehouseLeases.inspect")(
    function* (target: string) {
      const index = yield* readIndex();
      const pool =
        index.find((entry) => matches({ root: entry.path, target: path.resolve(target) }))?.pool ??
        (isNetworkPath(target) ? null : yield* treehousePoolForPath(target));
      if (pool == null) return null;
      const state = yield* fs
        .readFileString(path.join(pool, "treehouse-state.json"))
        .pipe(Effect.flatMap(Schema.decodeEffect(Schema.fromJsonString(PoolState))));
      const canonical = yield* fs.realPath(target);
      const slot = state.worktrees.find((entry) =>
        matches({ root: path.resolve(entry.path), target: canonical }),
      );
      if (
        state.version !== 4 ||
        !slot?.leased ||
        slot.destroying ||
        slot.recovery_error ||
        slot.recovery_reason ||
        !slot.lease_id ||
        !slot.lease_holder
      )
        return yield* new TreehouseLeaseError({ path: target, reason: "unknown-lease" });
      return { ...slot, lease_id: slot.lease_id, lease_holder: slot.lease_holder };
    },
    Effect.provideService(FileSystem.FileSystem, fs),
    Effect.provideService(Path.Path, path),
    (effect, target) =>
      effect.pipe(Effect.mapError((cause) => metadataError({ path: target, cause }))),
  );
  const read = Effect.fn("TreehouseLeases.read")(
    function* (target: string) {
      const slot = yield* inspect(target);
      // Leases held outside T3 stay unmanaged; T3 never pins, binds, or returns them.
      if (slot === null || !slot.lease_holder.startsWith("t3-code:")) return null;
      const root = yield* location();
      const id = yield* decodeLeaseId(slot.lease_id);
      const recordPath = path.join(root, "leases", `${id}.json`);
      const record = yield* fs
        .readFileString(recordPath)
        .pipe(Effect.flatMap(Schema.decodeEffect(Schema.fromJsonString(Record))));
      if (
        record.returned ||
        record.release !== undefined ||
        record.lease.path !== path.resolve(slot.path) ||
        record.lease.lease_id !== id ||
        record.lease.lease_holder !== slot.lease_holder ||
        !slot.lease_holder.startsWith("t3-code:")
      )
        return yield* new TreehouseLeaseError({ path: target, reason: "unknown-lease" });
      return { recordPath, record };
    },
    Effect.provideService(FileSystem.FileSystem, fs),
    Effect.provideService(Path.Path, path),
    (effect, target) =>
      effect.pipe(Effect.mapError((cause) => metadataError({ path: target, cause }))),
  );
  const bindingPath = (input: { root: string; threadId: string }) =>
    path.join(input.root, "threads", `${Base64Url.encode(input.threadId)}.json`);
  const readBinding = Effect.fn("TreehouseLeases.readBinding")(function* (threadId: string) {
    if (registry === null) return null;
    return yield* fs.readFileString(bindingPath({ root: registry, threadId })).pipe(
      Effect.flatMap(Schema.decodeEffect(Schema.fromJsonString(Binding))),
      Effect.catchIf(
        (error) => "reason" in error && error.reason._tag === "NotFound",
        () => Effect.succeed(null),
      ),
    );
  });
  const readRecord = Effect.fn("TreehouseLeases.readRecord")(function* (id: string) {
    const root = yield* location();
    yield* decodeLeaseId(id);
    const recordPath = path.join(root, "leases", `${id}.json`);
    const record = yield* fs
      .readFileString(recordPath)
      .pipe(Effect.flatMap(Schema.decodeEffect(Schema.fromJsonString(Record))));
    if (record.lease.lease_id !== id)
      return yield* new TreehouseLeaseError({ path: recordPath, reason: "metadata" });
    return { recordPath, record };
  });
  const leaseEnded = Effect.fn("TreehouseLeases.leaseEnded")(function* (owned: Owned) {
    const pool =
      owned.record.pool ??
      (yield* readIndex()).find((entry) => entry.path === owned.record.lease.path)?.pool;
    if (pool === undefined)
      return yield* new TreehouseLeaseError({ path: owned.record.lease.path, reason: "metadata" });
    const state = yield* fs
      .readFileString(path.join(pool, "treehouse-state.json"))
      .pipe(Effect.flatMap(Schema.decodeEffect(Schema.fromJsonString(PoolState))));
    if (state.version !== 4)
      return yield* new TreehouseLeaseError({ path: owned.record.lease.path, reason: "metadata" });
    return !state.worktrees.some(
      (entry) => entry.leased && entry.lease_id === owned.record.lease.lease_id,
    );
  });
  const releasedThread = Effect.fn("TreehouseLeases.releasedThread")(
    function* (input: TreehouseReleasedThreadOptions) {
      const binding = yield* readBinding(input.threadId);
      if (binding === null) return null;
      const identity =
        binding.path === path.resolve(input.path)
          ? { path: binding.path, leaseId: binding.leaseId }
          : binding.resumedFrom;
      if (identity === undefined || identity.path !== path.resolve(input.path)) return null;
      const owned = yield* readRecord(identity.leaseId);
      if (owned.record.release === undefined) return null;
      if (
        owned.record.lease.path !== identity.path ||
        !owned.record.release.threadIds.includes(input.threadId)
      )
        return yield* new TreehouseLeaseError({ path: input.path, reason: "stale-binding" });
      // A release intent and an ended allocator generation also cover a failed receipt write.
      if (
        !owned.record.returned &&
        !owned.record.release.resuming &&
        owned.record.pool !== undefined &&
        (yield* leaseEnded(owned))
      )
        return { ...owned, record: { ...owned.record, returned: true } };
      return owned;
    },
    (effect, input) =>
      effect.pipe(Effect.mapError((cause) => metadataError({ path: input.path, cause }))),
  );
  const assertThread = Effect.fn("TreehouseLeases.assertThread")(
    function* (input: { path: string; threadId: string; requireBinding?: boolean }) {
      const binding = yield* readBinding(input.threadId);
      if (input.requireBinding === false && binding === null && !(yield* knownPath(input.path))) {
        const slot = yield* inspect(input.path);
        if (slot === null || !slot.lease_holder.startsWith("t3-code:")) return;
      }
      const owned = yield* read(input.path);
      if (owned === null) {
        if (binding !== null && matches({ root: binding.path, target: path.resolve(input.path) }))
          return yield* new TreehouseLeaseError({ path: input.path, reason: "missing" });
        return;
      }
      if (
        binding === null ||
        binding.path !== owned.record.lease.path ||
        binding.leaseId !== owned.record.lease.lease_id ||
        !binding.active
      )
        return yield* new TreehouseLeaseError({ path: input.path, reason: "stale-binding" });
    },
    (effect, input) =>
      effect.pipe(Effect.mapError((cause) => metadataError({ path: input.path, cause }))),
  );
  const useOwner = Effect.fn("TreehouseLeases.useOwner")(function* () {
    const pid = yield* HostProcess.ProcessId;
    const runtime = Option.isSome(config)
      ? yield* readPersistedServerRuntimeState(config.value.serverRuntimeStatePath).pipe(
          Effect.provideService(FileSystem.FileSystem, fs),
        )
      : Option.none();
    return {
      pid,
      runtime: Option.filter(
        runtime,
        (state) => state.pid === pid && state.ownerId !== undefined && state.ownerId.length > 0,
      ),
    };
  });
  const pinOwnedUse = Effect.fn("TreehouseLeases.pinOwnedUse")(function* (input: {
    owned: Owned;
    kind: (typeof Use.Type)["kind"];
  }) {
    const root = yield* location();
    const uses = path.join(root, "uses");
    const owner = yield* useOwner();
    yield* fs.makeDirectory(uses, { recursive: true });
    const pin = yield* fs.makeTempDirectory({ directory: uses, prefix: "use-" });
    yield* writeJson({
      target: path.join(pin, "use.json"),
      value: {
        path: input.owned.record.lease.path,
        leaseId: input.owned.record.lease.lease_id,
        kind: input.kind,
        ownerPid: owner.pid,
        ...(Option.isSome(owner.runtime) ? { ownerId: owner.runtime.value.ownerId } : {}),
      },
    });
    return fs.remove(pin, { recursive: true, force: true }).pipe(Effect.ignoreCause({ log: true }));
  });
  const assertUnbound = Effect.fn("TreehouseLeases.assertUnbound")(function* (input: {
    owned: Owned;
    allowedThreadIds: readonly string[];
  }) {
    const { owned } = input;
    const root = yield* location();
    const threads = yield* fs.readDirectory(path.join(root, "threads")).pipe(
      Effect.catchIf(
        (error) => error.reason._tag === "NotFound",
        () => Effect.succeed([]),
      ),
    );
    const allowed = new Set(input.allowedThreadIds.map((id) => `${Base64Url.encode(id)}.json`));
    for (const name of threads) {
      const binding = yield* fs
        .readFileString(path.join(root, "threads", name))
        .pipe(Effect.flatMap(Schema.decodeEffect(Schema.fromJsonString(Binding))));
      if (binding.active && binding.leaseId === owned.record.lease.lease_id && !allowed.has(name))
        return yield* new TreehouseLeaseError({
          path: owned.record.lease.path,
          reason: "in-use",
        });
    }
  });
  const assertUnused = Effect.fn("TreehouseLeases.assertUnused")(
    function* (input: { owned: Owned; allowedThreadIds: readonly string[] }) {
      yield* assertUnbound(input);
      const { owned } = input;
      const root = yield* location();
      const uses = yield* fs.readDirectory(path.join(root, "uses")).pipe(
        Effect.catchIf(
          (error) => error.reason._tag === "NotFound",
          () => Effect.succeed([]),
        ),
      );
      const owner = uses.length > 0 ? yield* useOwner() : undefined;
      for (const name of uses) {
        const pin = path.join(root, "uses", name);
        const marker = path.join(pin, "use.json");
        const use = yield* fs
          .readFileString(marker)
          .pipe(Effect.flatMap(Schema.decodeEffect(Schema.fromJsonString(Use))));
        if (use.leaseId !== owned.record.lease.lease_id) continue;
        const runtime = owner === undefined ? Option.none() : owner.runtime;
        // A different nonce proves exit because server startup holds the exclusive home lock.
        let exited =
          Option.isSome(runtime) &&
          use.ownerPid !== undefined &&
          use.ownerId !== undefined &&
          use.ownerId.length > 0 &&
          use.ownerId !== runtime.value.ownerId;
        if (!exited && use.ownerPid !== undefined && use.ownerPid !== owner?.pid)
          exited = !isProcessAlive(use.ownerPid);
        if (
          !exited &&
          use.ownerPid === undefined &&
          use.ownerId === undefined &&
          Option.isSome(runtime)
        ) {
          // Legacy pins have no process identity; only a verified subsequent server can retire them.
          const startedAt = Date.parse(runtime.value.startedAt);
          const modified = (yield* fs.stat(marker)).mtime;
          exited = Option.exists(modified, (time) => time.getTime() < startedAt);
        }
        if (!exited)
          return yield* new TreehouseLeaseError({
            path: owned.record.lease.path,
            reason: "in-use",
          });
        yield* fs.remove(pin, { recursive: true });
      }
    },
    (effect, input) =>
      effect.pipe(
        Effect.mapError((cause) => metadataError({ path: input.owned.record.lease.path, cause })),
      ),
  );
  const operationTarget = Effect.fn("TreehouseLeases.operationTarget")(function* (target: string) {
    const absolute = path.isAbsolute(target) && path.sep !== "\\" ? target : path.resolve(target);
    if (isNetworkPath(absolute)) return path.sep === "/" ? absolute : path.resolve(absolute);
    const root = path.parse(absolute).root;
    // Callback realpath collapses raw '..' before following links; I/O follows the links first.
    const segments = absolute.slice(root.length).split(path.sep);
    let ancestor = root;
    let links = 0;
    while (segments.length > 0) {
      const segment = segments.shift()!;
      if (segment === "" || segment === ".") continue;
      if (segment === "..") {
        ancestor = path.dirname(ancestor);
        continue;
      }
      const candidate = path.join(ancestor, segment);
      const link = yield* fs.readLink(candidate).pipe(Effect.option);
      if (Option.isNone(link)) {
        ancestor = candidate;
        continue;
      }
      if (++links > 40) return yield* new TreehouseLeaseError({ path: target, reason: "missing" });
      if (isNetworkPath(link.value))
        return path.sep === "/"
          ? `${link.value}/${segments.join("/")}`
          : path.resolve(link.value, ...segments);
      const linkRoot = path.parse(link.value).root;
      if (linkRoot !== "") ancestor = linkRoot;
      segments.unshift(...link.value.slice(linkRoot.length).split(path.sep));
    }
    const suffix: string[] = [];
    while (true) {
      const real = yield* fs
        .realPath(ancestor)
        .pipe(Effect.catchIf(isAbsentPath, () => Effect.succeed(null)));
      if (real !== null) return path.join(real, ...suffix);
      const link = yield* fs.readLink(ancestor).pipe(Effect.option);
      if (Option.isSome(link)) {
        if (++links > 40)
          return yield* new TreehouseLeaseError({ path: target, reason: "missing" });
        ancestor = path.resolve(path.dirname(ancestor), link.value);
        continue;
      }
      if (path.dirname(ancestor) === ancestor) return path.join(ancestor, ...suffix);
      suffix.unshift(path.basename(ancestor));
      ancestor = path.dirname(ancestor);
    }
  });
  const validateOperationContext = Effect.fn("TreehouseLeases.validateOperationContext")(function* (
    context: WorkspaceOperationContext | undefined,
  ) {
    if (context?.generation == null) return;
    const binding = yield* readBinding(context.threadId);
    if (binding === null || !binding.active || binding.leaseId !== context.generation)
      return yield* new TreehouseLeaseError({ path: context.threadId, reason: "stale-binding" });
    const owned = yield* readRecord(binding.leaseId);
    if (
      owned.record.returned ||
      owned.record.release !== undefined ||
      owned.record.lease.path !== binding.path
    )
      return yield* new TreehouseLeaseError({ path: binding.path, reason: "stale-binding" });
  });
  const pooledTarget = Effect.fn("TreehouseLeases.pooledTarget")(function* ({
    target,
    context,
  }: {
    readonly target: string;
    readonly context: WorkspaceOperationContext | undefined;
  }) {
    const canonical = yield* operationTarget(target);
    const index = yield* readIndex();
    const entry = index.find(
      (entry) =>
        matches({ root: entry.path, target: canonical }) ||
        matches({ root: entry.path, target: path.resolve(target) }),
    );
    const known = entry !== undefined;
    // Registry and binding checks precede any share access, including through local links.
    if (known) {
      if (context?.generation == null)
        return yield* new TreehouseLeaseError({
          path: target,
          reason: context === undefined ? "update-required" : "stale-binding",
        });
      const binding = yield* readBinding(context.threadId);
      if (binding?.path !== entry.path || binding.leaseId !== context.generation || !binding.active)
        return yield* new TreehouseLeaseError({ path: target, reason: "stale-binding" });
    }
    if (!known && isNetworkPath(canonical)) return null;
    const leasePath = entry?.path ?? canonical;
    const slot = yield* inspect(leasePath);
    if (!known && (slot === null || !slot.lease_holder.startsWith("t3-code:"))) return null;
    const owned = yield* read(leasePath);
    if (owned === null) return yield* new TreehouseLeaseError({ path: target, reason: "missing" });
    return owned;
  });
  const operationGeneration = Effect.fn("TreehouseLeases.operationGeneration")(
    function* (input: TreehouseReleasedThreadOptions) {
      const binding = yield* readBinding(input.threadId);
      const context = {
        threadId: ThreadId.make(input.threadId),
        generation: binding?.active ? binding.leaseId : null,
      };
      yield* validateOperationContext(context);
      const owned = yield* pooledTarget({ target: input.path, context });
      yield* assertThread({
        path: owned?.record.lease.path ?? input.path,
        threadId: input.threadId,
        requireBinding: owned !== null,
      });
      return owned?.record.lease.lease_id ?? null;
    },
    (effect, input) =>
      effect.pipe(Effect.mapError((cause) => metadataError({ path: input.path, cause }))),
  );
  const pinOperation: TreehouseLeaseStore["Service"]["pinOperation"] = Effect.fn(
    "TreehouseLeases.pinOperation",
  )(
    function* (input) {
      const targets = [...new Set(input.targets)];
      yield* validateOperationContext(input.context);
      const pooled = yield* Effect.forEach(targets, (target) =>
        pooledTarget({ target, context: input.context }),
      );
      if (pooled.every((owned) => owned === null) && input.context?.generation == null) {
        const binding =
          input.context === undefined ? null : yield* readBinding(input.context.threadId);
        if (binding?.active)
          return yield* new TreehouseLeaseError({
            path: targets[0] ?? binding.path,
            reason: "stale-binding",
          });
        return Effect.void;
      }
      return yield* withLock({
        path: targets[0] ?? input.context?.threadId ?? "workspace",
        effect: Effect.gen(function* () {
          const context = input.context;
          yield* validateOperationContext(context);
          if (context === undefined)
            return yield* new TreehouseLeaseError({
              path: targets[0] ?? "workspace",
              reason: "update-required",
            });
          const binding = yield* readBinding(context.threadId);
          if (context.generation !== null) {
            if (binding === null || binding.leaseId !== context.generation || !binding.active)
              return yield* new TreehouseLeaseError({
                path: targets[0] ?? "workspace",
                reason: "stale-binding",
              });
            yield* assertThread({ path: binding.path, threadId: context.threadId });
          }
          const ownedById = new Map<string, Owned>();
          for (const target of targets) {
            const owned = yield* pooledTarget({ target, context });
            if (owned === null) continue;
            if (context.generation !== owned.record.lease.lease_id)
              return yield* new TreehouseLeaseError({ path: target, reason: "stale-binding" });
            yield* assertThread({ path: owned.record.lease.path, threadId: context.threadId });
            ownedById.set(owned.record.lease.lease_id, owned);
          }
          const releases: Effect.Effect<void>[] = [];
          return yield* Effect.gen(function* () {
            for (const owned of ownedById.values())
              releases.push(yield* pinOwnedUse({ owned, kind: "operation" }));
            return Effect.forEach(releases, (release) => release, { discard: true });
          }).pipe(
            Effect.onError(() => Effect.forEach(releases, (release) => release, { discard: true })),
            Effect.uninterruptible,
          );
        }),
      });
    },
    (effect, input) =>
      effect.pipe(
        Effect.mapError((cause) => metadataError({ path: input.targets[0] ?? "workspace", cause })),
      ),
  );
  return TreehouseLeaseStore.of({
    read,
    operationGeneration,
    canonicalTarget: Effect.fn("TreehouseLeases.canonicalTarget")((target) =>
      operationTarget(target).pipe(
        Effect.mapError((cause) => metadataError({ path: target, cause })),
      ),
    ),
    pinOperation,
    knownPath,
    withLock,
    assertThread,
    releasedThread,
    beginResume: Effect.fn("TreehouseLeases.beginResume")(
      function* (input) {
        const current = yield* readRecord(input.owned.record.lease.lease_id);
        const release = current.record.release;
        if (!current.record.returned || release === undefined || release.replacement !== undefined)
          return yield* new TreehouseLeaseError({
            path: input.owned.record.lease.path,
            reason: "in-use",
          });
        if (
          release.resume !== undefined &&
          (!release.resuming ||
            (!release.resume.allocationStarted && release.resume.lease !== undefined))
        )
          return yield* new TreehouseLeaseError({
            path: input.owned.record.lease.path,
            reason: "metadata",
          });
        const ownerPid = yield* HostProcess.ProcessId;
        const runtime = Option.isSome(config)
          ? yield* readPersistedServerRuntimeState(config.value.serverRuntimeStatePath).pipe(
              Effect.provideService(FileSystem.FileSystem, fs),
            )
          : Option.none();
        if (
          Option.isSome(runtime) &&
          runtime.value.pid !== ownerPid &&
          isProcessAlive(runtime.value.pid)
        )
          return yield* new TreehouseLeaseError({
            path: current.record.lease.path,
            reason: "in-use",
          });
        const ownerId =
          Option.isSome(runtime) && runtime.value.pid === ownerPid
            ? runtime.value.ownerId
            : undefined;
        const previous = release.resume;
        // A different server nonce proves the old owner exited: startup holds the exclusive home lock.
        const previousServerExited =
          ownerId !== undefined && previous?.ownerId !== undefined && ownerId !== previous.ownerId;
        if (
          previous?.ownerPid !== undefined &&
          previous.ownerPid !== ownerPid &&
          !previousServerExited &&
          isProcessAlive(previous.ownerPid)
        )
          return yield* new TreehouseLeaseError({
            path: current.record.lease.path,
            reason: "in-use",
          });
        if (!(yield* leaseEnded(current)))
          return yield* new TreehouseLeaseError({
            path: current.record.lease.path,
            reason: "unknown-lease",
          });
        yield* assertUnused({ owned: current, allowedThreadIds: release.threadIds });
        const confirmed = previous?.allocationStarted && previous.lease !== undefined;
        // The caller holds the per-release resume lease; a same-process attempt has already settled.
        const abandoned = release.resuming && !confirmed && (previous?.allocationStarted ?? true);
        const abandonedAt = abandoned ? DateTime.formatIso(yield* DateTime.now) : undefined;
        const updated = {
          ...current,
          record: {
            ...current.record,
            release: {
              ...release,
              resuming: true,
              ...(abandonedAt !== undefined
                ? {
                    abandonedResumes: [
                      ...(release.abandonedResumes ?? []),
                      { resume: previous, abandonedAt },
                    ],
                  }
                : {}),
              resume: {
                ...(confirmed ? previous : { holder: input.holder, allocationStarted: false }),
                ...(input.branch === undefined ? {} : { branch: previous?.branch ?? input.branch }),
                ownerPid,
                ownerId,
              },
            },
          },
        };
        yield* writeJson({ target: updated.recordPath, value: updated.record, durable: true });
        return updated;
      },
      (effect, input) =>
        effect.pipe(
          Effect.mapError((cause) => metadataError({ path: input.owned.record.lease.path, cause })),
        ),
    ),
    startResumeAllocation: Effect.fn("TreehouseLeases.startResumeAllocation")(
      function* (owned) {
        const current = yield* readRecord(owned.record.lease.lease_id);
        const release = current.record.release;
        const resume = release?.resume;
        if (
          !current.record.returned ||
          !release?.resuming ||
          resume === undefined ||
          resume.holder !== owned.record.release?.resume?.holder ||
          resume.allocationStarted ||
          resume.lease !== undefined ||
          release.replacement !== undefined
        )
          return yield* new TreehouseLeaseError({
            path: owned.record.lease.path,
            reason: "stale-binding",
          });
        yield* writeJson({
          target: current.recordPath,
          value: {
            ...current.record,
            release: { ...release, resume: { ...resume, allocationStarted: true } },
          },
          durable: true,
        });
      },
      (effect, owned) =>
        effect.pipe(
          Effect.mapError((cause) => metadataError({ path: owned.record.lease.path, cause })),
        ),
    ),
    finishResumeAllocation: Effect.fn("TreehouseLeases.finishResumeAllocation")(
      function* (input) {
        const current = yield* readRecord(input.owned.record.lease.lease_id);
        const release = current.record.release;
        const resume = release?.resume;
        if (
          !current.record.returned ||
          !release?.resuming ||
          !resume?.allocationStarted ||
          resume.holder !== input.owned.record.release?.resume?.holder ||
          resume.holder !== input.lease.lease_holder ||
          resume.lease !== undefined ||
          release.replacement !== undefined
        )
          return yield* new TreehouseLeaseError({
            path: input.lease.path,
            reason: "stale-binding",
          });
        yield* writeJson({
          target: current.recordPath,
          value: {
            ...current.record,
            release: { ...release, resume: { ...resume, lease: input.lease } },
          },
          durable: true,
        });
      },
      (effect, input) =>
        effect.pipe(Effect.mapError((cause) => metadataError({ path: input.lease.path, cause }))),
    ),
    setReplacement: Effect.fn("TreehouseLeases.setReplacement")(
      function* (input) {
        const owned = yield* readRecord(input.owned.record.lease.lease_id);
        const replacement = yield* read(input.path);
        if (
          !owned.record.returned ||
          !owned.record.release?.resuming ||
          owned.record.release.replacement !== undefined ||
          replacement === null ||
          owned.record.release.resume?.holder !== input.owned.record.release?.resume?.holder ||
          (owned.record.release.resume !== undefined &&
            (replacement.record.lease.lease_holder !== owned.record.release.resume.holder ||
              replacement.record.lease.lease_id !== owned.record.release.resume.lease?.lease_id))
        )
          return yield* new TreehouseLeaseError({ path: input.path, reason: "stale-binding" });
        yield* writeJson({
          target: owned.recordPath,
          value: {
            ...owned.record,
            release: {
              ...owned.record.release,
              replacement: {
                path: replacement.record.lease.path,
                leaseId: replacement.record.lease.lease_id,
              },
            },
          },
          durable: owned.record.release.resume !== undefined,
        });
      },
      (effect, input) =>
        effect.pipe(Effect.mapError((cause) => metadataError({ path: input.path, cause }))),
    ),
    beginCleanupReturn: Effect.fn("TreehouseLeases.beginCleanupReturn")(
      function* (input) {
        const owned = yield* read(input.owned.record.lease.path);
        if (owned === null || owned.record.lease.lease_id !== input.owned.record.lease.lease_id)
          return yield* new TreehouseLeaseError({
            path: input.owned.record.lease.path,
            reason: "unknown-lease",
          });
        // Only whole-checkout expiry removal may discard the trusted seed inventory.
        const slot = yield* inspect(owned.record.lease.path);
        if (!input.discardLocalFiles && slot?.seeded_paths?.length)
          return yield* new TreehouseLeaseError({
            path: owned.record.lease.path,
            reason: "metadata",
          });
        yield* assertUnused({ owned, allowedThreadIds: input.release.threadIds });
        for (const threadId of input.release.threadIds) {
          const binding = yield* readBinding(threadId);
          if (
            binding === null ||
            binding.path !== owned.record.lease.path ||
            binding.leaseId !== owned.record.lease.lease_id
          )
            return yield* new TreehouseLeaseError({
              path: owned.record.lease.path,
              reason: "stale-binding",
            });
        }
        const pool = (yield* readIndex()).find(
          (entry) => entry.path === owned.record.lease.path,
        )?.pool;
        if (pool === undefined)
          return yield* new TreehouseLeaseError({
            path: owned.record.lease.path,
            reason: "metadata",
          });
        const updated = { ...owned, record: { ...owned.record, pool, release: input.release } };
        yield* writeJson({ target: updated.recordPath, value: updated.record });
        return updated;
      },
      (effect, input) =>
        effect.pipe(
          Effect.mapError((cause) => metadataError({ path: input.owned.record.lease.path, cause })),
        ),
    ),
    register: Effect.fn("TreehouseLeases.register")(
      function* (input) {
        const root = yield* location();
        const recordPath = path.join(root, "leases", `${input.lease.lease_id}.json`);
        if (input.resume !== undefined) {
          const source = yield* readRecord(input.resume.record.lease.lease_id);
          if (
            !source.record.returned ||
            !source.record.release?.resuming ||
            !source.record.release.resume?.allocationStarted ||
            source.record.release.resume.holder !== input.lease.lease_holder ||
            source.record.release.resume.holder !== input.resume.record.release?.resume?.holder ||
            source.record.release.replacement !== undefined
          )
            return yield* new TreehouseLeaseError({
              path: input.lease.path,
              reason: "stale-binding",
            });
          const recorded = source.record.release.resume.lease;
          if (
            recorded === undefined ||
            recorded.path !== input.lease.path ||
            recorded.lease_id !== input.lease.lease_id ||
            recorded.leased_at !== input.lease.leased_at
          )
            return yield* new TreehouseLeaseError({
              path: input.lease.path,
              reason: "stale-binding",
            });
        }
        if (yield* fs.exists(recordPath)) {
          const previous = yield* readRecord(input.lease.lease_id);
          if (
            input.resume === undefined ||
            previous.record.returned ||
            previous.record.release !== undefined ||
            previous.record.binary !== input.binary ||
            previous.record.lease.path !== input.lease.path ||
            previous.record.lease.lease_holder !== input.lease.lease_holder ||
            previous.record.lease.leased_at !== input.lease.leased_at
          )
            return yield* new TreehouseLeaseError({
              path: input.lease.path,
              reason: "unknown-lease",
            });
          yield* assertUnused({ owned: previous, allowedThreadIds: [] });
        } else {
          yield* writeJson({
            target: recordPath,
            value: { lease: input.lease, binary: input.binary, returned: false },
            durable: input.resume !== undefined,
          });
        }
        if (input.preparationScope !== undefined) {
          // Allocation holds the lifecycle lock until this pin takes over.
          yield* Effect.acquireRelease(
            pinOwnedUse({
              owned: {
                recordPath,
                record: { lease: input.lease, binary: input.binary, returned: false },
              },
              kind: "preparation",
            }),
            (release) => release,
          ).pipe(Effect.provideService(Scope.Scope, input.preparationScope), Effect.asVoid);
        }
        const index = yield* readIndex();
        if (!index.some((entry) => entry.path === input.lease.path))
          yield* writeJson({
            target: path.join(root, "paths.json"),
            value: [...index, { path: input.lease.path, pool: input.pool }],
            durable: input.resume !== undefined,
          });
      },
      (effect, input) =>
        effect.pipe(Effect.mapError((cause) => metadataError({ path: input.lease.path, cause }))),
    ),
    cancelCleanupReturn: Effect.fn("TreehouseLeases.cancelCleanupReturn")(
      function* (owned) {
        const slot = yield* inspect(owned.record.lease.path);
        const current = yield* readRecord(owned.record.lease.lease_id);
        const { release, ...record } = current.record;
        if (
          slot?.lease_id !== owned.record.lease.lease_id ||
          slot.lease_holder !== owned.record.lease.lease_holder ||
          current.record.returned ||
          release === undefined ||
          owned.record.release === undefined ||
          release.resuming ||
          release.replacement !== undefined ||
          (yield* encodeRelease(release)) !== (yield* encodeRelease(owned.record.release))
        )
          return yield* new TreehouseLeaseError({
            path: owned.record.lease.path,
            reason: "unknown-lease",
          });
        yield* writeJson({ target: current.recordPath, value: record });
      },
      (effect, owned) =>
        effect.pipe(
          Effect.mapError((cause) => metadataError({ path: owned.record.lease.path, cause })),
        ),
    ),
    markReturned: Effect.fn("TreehouseLeases.markReturned")(
      function* (owned) {
        if (!(yield* leaseEnded(owned)))
          return yield* new TreehouseLeaseError({
            path: owned.record.lease.path,
            reason: "unknown-lease",
          });
        yield* writeJson({ target: owned.recordPath, value: { ...owned.record, returned: true } });
      },
      (effect, owned) =>
        effect.pipe(
          Effect.mapError((cause) => metadataError({ path: owned.record.lease.path, cause })),
        ),
    ),
    bindThread: Effect.fn("TreehouseLeases.bindThread")(
      function* (input) {
        if (!(yield* knownPath(input.path)) && (yield* treehousePoolForPath(input.path)) === null)
          return;
        yield* withLock({
          path: input.path,
          effect: Effect.gen(function* () {
            if (input.sourceThreadId !== undefined) {
              yield* assertThread({ path: input.path, threadId: input.sourceThreadId });
            }
            const owned = yield* read(input.path);
            if (owned === null) return;
            const root = yield* location();
            const previous = yield* readBinding(input.threadId);
            if (previous !== null && previous.sequence > input.sequence)
              return yield* new TreehouseLeaseError({ path: input.path, reason: "stale-binding" });
            let resumedFrom = previous?.resumedFrom;
            if (
              previous !== null &&
              (previous.path !== owned.record.lease.path ||
                previous.leaseId !== owned.record.lease.lease_id)
            ) {
              const released = yield* readRecord(previous.leaseId);
              const release = released.record.release;
              const resuming =
                released.record.returned &&
                release?.replacement?.leaseId === owned.record.lease.lease_id &&
                release.replacement.path === owned.record.lease.path &&
                release.threadIds.includes(input.threadId);
              const switching =
                input.expectedPreviousPath !== undefined &&
                (!previous.active ||
                  (input.expectedPreviousPath === previous.path &&
                    !released.record.returned &&
                    release === undefined));
              if (!resuming && !switching)
                return yield* new TreehouseLeaseError({
                  path: input.path,
                  reason: "stale-binding",
                });
              if (resuming) {
                resumedFrom = { path: previous.path, leaseId: previous.leaseId };
              } else {
                yield* assertUnbound({ owned, allowedThreadIds: [input.threadId] });
                resumedFrom = undefined;
              }
            }
            yield* writeJson({
              target: bindingPath({ root, threadId: input.threadId }),
              value: {
                path: owned.record.lease.path,
                leaseId: owned.record.lease.lease_id,
                active: true,
                sequence: input.sequence,
                ...(resumedFrom === undefined ? {} : { resumedFrom }),
              },
            });
          }),
        });
      },
      Effect.provideService(FileSystem.FileSystem, fs),
      Effect.provideService(Path.Path, path),
      (effect, input) =>
        effect.pipe(Effect.mapError((cause) => metadataError({ path: input.path, cause }))),
    ),
    listHeld: Effect.fn("TreehouseLeases.listHeld")(
      function* () {
        const held: { path: string; leaseId: string | null; verified: boolean }[] = [];
        const pools = new Set((yield* readIndex()).map((entry) => entry.pool));
        for (const pool of pools) {
          if (!path.isAbsolute(pool) || isNetworkPath(pool))
            return yield* new TreehouseLeaseError({ path: pool, reason: "metadata" });
          const canonical = yield* fs.realPath(pool);
          const state = yield* fs
            .readFileString(path.join(canonical, "treehouse-state.json"))
            .pipe(Effect.flatMap(Schema.decodeEffect(Schema.fromJsonString(PoolState))));
          for (const slot of state.worktrees) {
            if (slot.leased !== true || !slot.lease_holder?.startsWith("t3-code:")) continue;
            const target = path.resolve(slot.path);
            if (
              isNetworkPath(target) ||
              !path.isAbsolute(slot.path) ||
              !matches({ root: canonical, target }) ||
              target === canonical
            )
              return yield* new TreehouseLeaseError({ path: canonical, reason: "metadata" });
            const owned = yield* read(target).pipe(Effect.result);
            const leaseId = isLeaseId(slot.lease_id) ? slot.lease_id : null;
            held.push({
              path: target,
              leaseId,
              verified:
                owned._tag === "Success" &&
                owned.success !== null &&
                owned.success.record.lease.lease_id === leaseId,
            });
          }
        }
        return held;
      },
      (effect) =>
        effect.pipe(
          Effect.mapError((cause) =>
            metadataError({ path: registry ?? "T3 state directory", cause }),
          ),
        ),
    ),
    currentThreads: Effect.fn("TreehouseLeases.currentThreads")(
      function* <
        T extends { readonly id: string; readonly deletedAt: DateTime.Utc | null },
      >(input: { readonly path: string; readonly threads: readonly T[] }) {
        const inspected = yield* read(input.path).pipe(Effect.result);
        if (inspected._tag === "Failure") {
          const histories = yield* Effect.forEach(input.threads, (thread) =>
            releasedThread({ path: input.path, threadId: thread.id }),
          );
          if (
            histories.length > 0 &&
            histories.every((history) => history?.record.returned === true)
          )
            return { threads: [], leaseId: null, verified: true };
          return yield* inspected.failure;
        }
        const owned = inspected.success;
        if (owned === null) return { threads: [...input.threads], leaseId: null, verified: true };
        const threads: T[] = [];
        let verified = true;
        for (const thread of input.threads) {
          const binding = yield* readBinding(thread.id);
          if (
            binding?.path === owned.record.lease.path &&
            binding.leaseId === owned.record.lease.lease_id &&
            (binding.active || thread.deletedAt !== null)
          ) {
            threads.push(thread);
            continue;
          }
          const released = yield* releasedThread({ path: input.path, threadId: thread.id });
          if (released?.record.returned !== true) {
            verified = false;
            threads.push(thread);
          }
        }
        return { threads, leaseId: owned.record.lease.lease_id, verified };
      },
      (effect, input) =>
        effect.pipe(Effect.mapError((cause) => metadataError({ path: input.path, cause }))),
    ),
    commitThread: Effect.fn("TreehouseLeases.commitThread")(
      function* (input) {
        const previous = yield* readBinding(input.threadId);
        if (previous === null) return;
        yield* withLock({
          path: previous.path,
          effect: Effect.gen(function* () {
            const current = yield* readBinding(input.threadId);
            if (current === null || current.sequence > input.sequence) return;
            const root = yield* location();
            const active =
              !input.deleted && input.path !== null && path.resolve(input.path) === current.path;
            yield* writeJson({
              target: bindingPath({ root, threadId: input.threadId }),
              value: { ...current, active, sequence: input.sequence },
            });
          }),
        });
      },
      (effect, input) =>
        effect.pipe(
          Effect.mapError((cause) => metadataError({ path: input.path ?? input.threadId, cause })),
        ),
    ),
    pinUse: Effect.fn("TreehouseLeases.pinUse")(
      function* (input) {
        const binding = yield* readBinding(input.threadId);
        if (binding === null) {
          if (yield* knownPath(input.path))
            return yield* new TreehouseLeaseError({ path: input.path, reason: "stale-binding" });
          return Effect.void;
        }
        return yield* withLock({
          path: input.path,
          effect: Effect.gen(function* () {
            yield* assertThread({ ...input, requireBinding: false });
            const owned = yield* read(input.path);
            if (owned === null) return Effect.void;
            return yield* pinOwnedUse({ owned, kind: input.kind });
          }),
        });
      },
      (effect, input) =>
        effect.pipe(Effect.mapError((cause) => metadataError({ path: input.path, cause }))),
    ),
    assertUnused,
    assertReturnable: Effect.fn("TreehouseLeases.assertReturnable")(
      function* (owned) {
        yield* assertUnused({ owned, allowedThreadIds: [] });
      },
      (effect, owned) =>
        effect.pipe(
          Effect.mapError((cause) => metadataError({ path: owned.record.lease.path, cause })),
        ),
    ),
  });
});
export const layer = Layer.effect(TreehouseLeaseStore, make);

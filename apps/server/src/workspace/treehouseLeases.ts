import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Base64Url from "effect/encoding/Base64Url";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import type * as PlatformError from "effect/PlatformError";
import * as Schema from "effect/Schema";
import * as ServerConfig from "../config.ts";
import { withWorkspaceLease } from "./workspaceLease.ts";

const LeaseId = Schema.String.check(Schema.isPattern(/^[a-f0-9]{32}$/));
export const TreehouseLease = Schema.Struct({
  path: Schema.String,
  lease_id: LeaseId,
  lease_holder: Schema.String,
  leased_at: Schema.String,
  base_branch: Schema.optional(Schema.String),
});
const Record = Schema.Struct({
  lease: TreehouseLease,
  binary: Schema.String,
  returned: Schema.Boolean,
});
const Index = Schema.Array(Schema.Struct({ path: Schema.String, pool: Schema.String }));
const Binding = Schema.Struct({
  path: Schema.String,
  leaseId: LeaseId,
  active: Schema.Boolean,
  sequence: Schema.Number,
});
const Use = Schema.Struct({
  path: Schema.String,
  leaseId: LeaseId,
  kind: Schema.Literals(["provider", "terminal"]),
});
const PoolState = Schema.Struct({
  version: Schema.Number,
  worktrees: Schema.Array(
    Schema.Struct({
      path: Schema.String,
      leased: Schema.optional(Schema.Boolean),
      lease_id: Schema.optional(Schema.String),
      lease_holder: Schema.optional(Schema.String),
      destroying: Schema.optional(Schema.Boolean),
      recovery_error: Schema.optional(Schema.String),
      recovery_reason: Schema.optional(Schema.String),
    }),
  ),
});
export class TreehouseLeaseError extends Schema.TaggedError<TreehouseLeaseError>()(
  "TreehouseLeaseError",
  {
    path: Schema.String,
    reason: Schema.Literals(["metadata", "unknown-lease", "stale-binding", "in-use", "missing"]),
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return `Treehouse workspace ${this.path} is unavailable (${this.reason}). T3 keeps the lease rather than using or deleting an unverified slot.`;
  }
}
const isLeaseError = Schema.is(TreehouseLeaseError);
const encodeJson = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));
const decodeLeaseId = Schema.decodeEffect(LeaseId);
const metadataError = (input: { path: string; cause: unknown }) =>
  isLeaseError(input.cause)
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

type Owned = { recordPath: string; record: typeof Record.Type };
export class TreehouseLeaseStore extends Context.Service<
  TreehouseLeaseStore,
  {
    readonly read: (target: string) => Effect.Effect<Owned | null, TreehouseLeaseError>;
    readonly knownPath: (target: string) => Effect.Effect<boolean, TreehouseLeaseError>;
    readonly register: (input: {
      lease: typeof TreehouseLease.Type;
      binary: string;
      pool: string;
    }) => Effect.Effect<void, TreehouseLeaseError>;
    readonly markReturned: (owned: Owned) => Effect.Effect<void, TreehouseLeaseError>;
    readonly withLock: <A, E, R>(input: {
      path: string;
      effect: Effect.Effect<A, E, R>;
    }) => Effect.Effect<A, E | TreehouseLeaseError, R>;
    readonly bindThread: (input: {
      path: string;
      threadId: string;
      sequence: number;
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
    readonly assertReturnable: (owned: Owned) => Effect.Effect<void, TreehouseLeaseError>;
  }
>()("t3/workspace/treehouseLeases/TreehouseLeaseStore") {}

const make = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const config = yield* Effect.serviceOption(ServerConfig.ServerConfig);
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
  }) {
    yield* fs.makeDirectory(path.dirname(input.target), { recursive: true });
    const json = yield* encodeJson(input.value);
    const temp = `${input.target}.tmp`;
    yield* fs.writeFileString(temp, json, { mode: 0o600 });
    yield* fs.rename(temp, input.target);
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
        (yield* treehousePoolForPath(target)) ??
        index.find((entry) => matches({ root: entry.path, target: path.resolve(target) }))?.pool;
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
      if (slot === null) return null;
      const root = yield* location();
      const id = yield* decodeLeaseId(slot.lease_id);
      const recordPath = path.join(root, "leases", `${id}.json`);
      const record = yield* fs
        .readFileString(recordPath)
        .pipe(Effect.flatMap(Schema.decodeEffect(Schema.fromJsonString(Record))));
      if (
        record.returned ||
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
  return TreehouseLeaseStore.of({
    read,
    knownPath,
    withLock,
    assertThread,
    register: Effect.fn("TreehouseLeases.register")(
      function* (input) {
        const root = yield* location();
        const recordPath = path.join(root, "leases", `${input.lease.lease_id}.json`);
        if (yield* fs.exists(recordPath))
          return yield* new TreehouseLeaseError({
            path: input.lease.path,
            reason: "unknown-lease",
          });
        yield* writeJson({
          target: recordPath,
          value: { lease: input.lease, binary: input.binary, returned: false },
        });
        const index = yield* readIndex();
        if (!index.some((entry) => entry.path === input.lease.path))
          yield* writeJson({
            target: path.join(root, "paths.json"),
            value: [...index, { path: input.lease.path, pool: input.pool }],
          });
      },
      (effect, input) =>
        effect.pipe(Effect.mapError((cause) => metadataError({ path: input.lease.path, cause }))),
    ),
    markReturned: Effect.fn("TreehouseLeases.markReturned")(
      function* (owned) {
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
            const owned = yield* read(input.path);
            if (owned === null) return;
            const root = yield* location();
            const previous = yield* readBinding(input.threadId);
            if (
              previous !== null &&
              (previous.path !== owned.record.lease.path ||
                previous.leaseId !== owned.record.lease.lease_id)
            )
              return yield* new TreehouseLeaseError({ path: input.path, reason: "stale-binding" });
            yield* writeJson({
              target: bindingPath({ root, threadId: input.threadId }),
              value: {
                path: owned.record.lease.path,
                leaseId: owned.record.lease.lease_id,
                active: true,
                sequence: input.sequence,
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
            const root = yield* location();
            const uses = path.join(root, "uses");
            yield* fs.makeDirectory(uses, { recursive: true });
            const pin = yield* fs.makeTempDirectory({ directory: uses, prefix: "use-" });
            yield* writeJson({
              target: path.join(pin, "use.json"),
              value: {
                path: owned.record.lease.path,
                leaseId: owned.record.lease.lease_id,
                kind: input.kind,
              },
            });
            return fs.remove(pin, { recursive: true }).pipe(Effect.ignoreCause({ log: true }));
          }),
        });
      },
      (effect, input) =>
        effect.pipe(Effect.mapError((cause) => metadataError({ path: input.path, cause }))),
    ),
    assertReturnable: Effect.fn("TreehouseLeases.assertReturnable")(
      function* (owned) {
        const root = yield* location();
        const threads = yield* fs.readDirectory(path.join(root, "threads")).pipe(
          Effect.catchIf(
            (error) => error.reason._tag === "NotFound",
            () => Effect.succeed([]),
          ),
        );
        for (const name of threads) {
          const binding = yield* fs
            .readFileString(path.join(root, "threads", name))
            .pipe(Effect.flatMap(Schema.decodeEffect(Schema.fromJsonString(Binding))));
          if (binding.active && binding.leaseId === owned.record.lease.lease_id)
            return yield* new TreehouseLeaseError({
              path: owned.record.lease.path,
              reason: "in-use",
            });
        }
        const uses = yield* fs.readDirectory(path.join(root, "uses")).pipe(
          Effect.catchIf(
            (error) => error.reason._tag === "NotFound",
            () => Effect.succeed([]),
          ),
        );
        for (const name of uses) {
          const use = yield* fs
            .readFileString(path.join(root, "uses", name, "use.json"))
            .pipe(Effect.flatMap(Schema.decodeEffect(Schema.fromJsonString(Use))));
          if (use.leaseId === owned.record.lease.lease_id)
            return yield* new TreehouseLeaseError({
              path: owned.record.lease.path,
              reason: "in-use",
            });
        }
      },
      (effect, owned) =>
        effect.pipe(
          Effect.mapError((cause) => metadataError({ path: owned.record.lease.path, cause })),
        ),
    ),
  });
});
export const layer = Layer.effect(TreehouseLeaseStore, make);

import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Crypto from "effect/Crypto";
import * as Hex from "effect/encoding/Hex";
import { LeaseCleanupTask } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as ServerConfig from "../config.ts";
import { withWorkspaceLease } from "./workspaceLease.ts";

export const LeaseInventory = Schema.Array(
  Schema.Struct({
    pool: Schema.String,
    path: Schema.String,
    removal_root: Schema.String,
    lease_id: Schema.String,
    lease_holder: Schema.String,
    leased_at: Schema.NullOr(Schema.String),
    quarantined: Schema.Boolean,
  }),
);
export type LeaseInventoryEntry = (typeof LeaseInventory.Type)[number];
export interface ReconcileLeaseCleanupOptions {
  readonly leases: readonly LeaseInventoryEntry[];
  readonly owners: ReadonlyMap<
    string,
    { readonly threadIds: readonly string[]; readonly activityAt: number | null }
  >;
  readonly now: number;
  readonly days: number;
}
export interface UpdateLeaseCleanupOptions {
  readonly task: LeaseCleanupTask;
  readonly state: LeaseCleanupTask["state"];
  readonly reason: string;
  readonly now: number;
}
export class LeaseCleanupQueueError extends Schema.TaggedError<LeaseCleanupQueueError>()(
  "LeaseCleanupQueueError",
  {
    path: Schema.String,
    cause: Schema.Defect(),
  },
) {
  override get message(): string {
    return "Cannot read or persist lease cleanup responsibility.";
  }
}
export class LeaseCleanupQueue extends Context.Service<
  LeaseCleanupQueue,
  {
    readonly list: Effect.Effect<readonly LeaseCleanupTask[], LeaseCleanupQueueError>;
    readonly reconcile: (
      input: ReconcileLeaseCleanupOptions,
    ) => Effect.Effect<readonly LeaseCleanupTask[], LeaseCleanupQueueError>;
    readonly update: (
      input: UpdateLeaseCleanupOptions,
    ) => Effect.Effect<LeaseCleanupTask, LeaseCleanupQueueError>;
  }
>()("t3/workspace/LeaseCleanupQueue") {}

const iso = (value: number) => DateTime.formatIso(DateTime.makeUnsafe(value));
const timestamp = (value: string | null) =>
  value === null
    ? null
    : Option.getOrNull(DateTime.make(value).pipe(Option.map(DateTime.toEpochMillis)));
const isQueueError = Schema.is(LeaseCleanupQueueError);
const encodeKey = Schema.encodeEffect(Schema.fromJsonString(Schema.Array(Schema.String)));
const encode = Schema.encodeEffect(Schema.fromJsonString(LeaseCleanupTask));
const decode = Schema.decodeEffect(Schema.fromJsonString(LeaseCleanupTask));
const make = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const config = yield* ServerConfig.ServerConfig;
  const crypto = yield* Crypto.Crypto;
  const root = path.join(config.stateDir, "lease-cleanup");
  const isWindows = yield* HostProcess.isWindows;
  const syncDirectory = Effect.fn("LeaseCleanupQueue.syncDirectory")(function* (directory: string) {
    if (isWindows) return;
    yield* Effect.scoped(
      Effect.gen(function* () {
        const file = yield* fs.open(directory, { flag: "r" });
        yield* file.sync;
      }),
    );
  });
  const protect = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
    effect.pipe(
      Effect.mapError((cause) =>
        isQueueError(cause) ? cause : new LeaseCleanupQueueError({ path: root, cause }),
      ),
    );
  const verifyRoot = Effect.fn("LeaseCleanupQueue.verifyRoot")(function* () {
    if (yield* fs.exists(root)) {
      const stat = yield* fs.stat(root);
      const actual = yield* fs.realPath(root);
      const expected = path.join(yield* fs.realPath(config.stateDir), "lease-cleanup");
      if (stat.type !== "Directory" || actual !== expected)
        return yield* new LeaseCleanupQueueError({
          path: root,
          cause: "invalid cleanup directory",
        });
    }
  });
  const verifyRecord = Effect.fn("LeaseCleanupQueue.verifyRecord")(function* (target: string) {
    const stat = yield* fs.stat(target);
    const actual = yield* fs.realPath(target);
    const expected = path.join(yield* fs.realPath(root), path.basename(target));
    if (stat.type !== "File" || actual !== expected)
      return yield* new LeaseCleanupQueueError({ path: target, cause: "invalid cleanup record" });
  });
  const save = Effect.fn("LeaseCleanupQueue.save")(function* (task: LeaseCleanupTask) {
    yield* verifyRoot();
    yield* fs.makeDirectory(root, { recursive: true, mode: 0o700 });
    const target = path.join(root, `${task.id}.json`);
    if (yield* fs.exists(target)) yield* verifyRecord(target);
    const encoded = yield* encode(task);
    yield* Effect.scoped(
      Effect.gen(function* () {
        const directory = yield* fs.makeTempDirectoryScoped({ directory: root, prefix: ".write-" });
        const temporary = path.join(directory, "record");
        yield* Effect.scoped(
          Effect.gen(function* () {
            const file = yield* fs.open(temporary, { flag: "wx", mode: 0o600 });
            yield* file.writeAll(new TextEncoder().encode(encoded));
            yield* file.sync;
          }),
        );
        yield* fs.rename(temporary, target);
        yield* syncDirectory(root);
        yield* syncDirectory(config.stateDir);
      }),
    );
    return task;
  });
  const readRecord = Effect.fn("LeaseCleanupQueue.readRecord")(function* (name: string) {
    const target = path.join(root, name);
    yield* verifyRecord(target);
    const task = yield* fs.readFileString(target).pipe(Effect.flatMap(decode));
    if (`${task.id}.json` !== name)
      return yield* new LeaseCleanupQueueError({
        path: target,
        cause: "cleanup identity mismatch",
      });
    return task;
  });
  const read = Effect.fn("LeaseCleanupQueue.read")(function* () {
    if (!(yield* fs.exists(root))) return [];
    yield* verifyRoot();
    const names = yield* fs.readDirectory(root);
    return yield* Effect.forEach(
      names.filter((name) => /^[a-f0-9]{64}\.json$/.test(name)),
      readRecord,
    );
  });
  return LeaseCleanupQueue.of({
    list: protect(read()),
    reconcile: Effect.fn("LeaseCleanupQueue.reconcile")(function* (input) {
      return yield* withWorkspaceLease(
        root,
        Effect.gen(function* () {
          const stored = yield* read();
          const present = new Set<string>();
          const result: LeaseCleanupTask[] = [];
          for (const lease of input.leases) {
            const key = yield* encodeKey([lease.pool, lease.path, lease.lease_id]);
            const id = Hex.encode(yield* crypto.digest("SHA-256", new TextEncoder().encode(key)));
            present.add(id);
            const previous = stored.find((task) => task.id === id);
            const acquired = timestamp(lease.leased_at);
            const changed =
              previous !== undefined &&
              (previous.acquiredAt !== lease.leased_at || previous.holder !== lease.lease_holder);
            const invalid =
              acquired === null ||
              acquired > input.now ||
              !/^[a-f0-9]{32}$/.test(lease.lease_id) ||
              lease.quarantined;
            const owner = input.owners.get(lease.path);
            const task: LeaseCleanupTask = {
              id,
              leaseId: lease.lease_id,
              path: lease.path,
              pool: lease.pool,
              holder: previous?.holder ?? lease.lease_holder,
              acquiredAt: previous !== undefined ? previous.acquiredAt : lease.leased_at,
              dueAt:
                previous !== undefined
                  ? previous.dueAt
                  : invalid
                    ? null
                    : iso(acquired + input.days * 86400000),
              responsibleService: "storage-cleanup",
              ownerThreadIds: [...(owner?.threadIds ?? [])],
              lastOwnerActivityAt: owner?.activityAt == null ? null : iso(owner.activityAt),
              state:
                changed || invalid
                  ? "blocked"
                  : previous?.state === "removed" || previous?.state === "released"
                    ? "blocked"
                    : (previous?.state ?? "scheduled"),
              reason: changed
                ? "Lease acquisition identity changed"
                : invalid
                  ? "Lease age or allocator identity cannot be verified"
                  : previous?.state === "removed" || previous?.state === "released"
                    ? "A completed lease generation reappeared"
                    : (previous?.reason ?? "Cleanup responsibility accepted"),
              attempts: previous?.attempts ?? 0,
              updatedAt: iso(input.now),
              nextAttemptAt: previous?.nextAttemptAt ?? null,
            };
            result.push(yield* save(task));
          }
          for (const previous of stored) {
            if (
              present.has(previous.id) ||
              previous.state === "removed" ||
              previous.state === "released"
            )
              continue;
            yield* save({
              ...previous,
              state: "released",
              reason: "Lease no longer held; checkout deletion is not implied",
              updatedAt: iso(input.now),
              nextAttemptAt: null,
            });
          }
          return result;
        }),
      ).pipe(protect);
    }),
    update: Effect.fn("LeaseCleanupQueue.update")(function* (input) {
      return yield* withWorkspaceLease(
        root,
        Effect.gen(function* () {
          yield* verifyRoot();
          const current = yield* readRecord(`${input.task.id}.json`);
          if ((yield* encode(current)) !== (yield* encode(input.task)))
            return yield* new LeaseCleanupQueueError({
              path: root,
              cause: "cleanup request changed",
            });
          return yield* save({
            ...current,
            state: input.state,
            reason: input.reason,
            attempts: current.attempts + (input.state === "reclaiming" ? 1 : 0),
            updatedAt: iso(input.now),
            nextAttemptAt: input.state === "blocked" ? iso(input.now + 300000) : null,
          });
        }),
      ).pipe(protect);
    }),
  });
});
export const layer = Layer.effect(LeaseCleanupQueue, make).pipe(Layer.provide(NodeCrypto.layer));

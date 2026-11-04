import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as ServerConfig from "../config.ts";
import * as LeaseCleanupQueue from "./LeaseCleanupQueue.ts";

const NOW = Date.parse("2026-11-04T00:00:00Z");
const DAY = 86400000;
const lease = (
  overrides: Partial<LeaseCleanupQueue.LeaseInventoryEntry> = {},
): LeaseCleanupQueue.LeaseInventoryEntry => ({
  pool: "/pools/repository",
  path: "/pools/repository/1/code",
  removal_root: "/pools/repository/1",
  lease_id: "a".repeat(32),
  lease_holder: "legacy",
  leased_at: "2026-11-01T00:00:00Z",
  quarantined: false,
  ...overrides,
});
const fixture = Effect.gen(function* () {
  const context = yield* Layer.build(
    ServerConfig.layerTest(process.cwd(), { prefix: "lease-cleanup-" }),
  );
  const config = Context.get(context, ServerConfig.ServerConfig);
  const restart = () =>
    Layer.build(LeaseCleanupQueue.layer).pipe(
      Effect.provideService(ServerConfig.ServerConfig, config),
      Effect.map((context) => Context.get(context, LeaseCleanupQueue.LeaseCleanupQueue)),
    );
  return {
    queue: yield* restart(),
    restart,
    config,
    fs: yield* FileSystem.FileSystem,
    path: yield* Path.Path,
  };
});

it.layer(NodeServices.layer)("durable lease cleanup responsibility", (it) => {
  it.effect(
    "retains deadlines and blocked retries across restart without extending age on progress",
    () =>
      Effect.gen(function* () {
        const f = yield* fixture;
        const [task] = yield* f.queue.reconcile({
          leases: [lease()],
          owners: new Map(),
          now: NOW,
          days: 7,
        });
        expect(task?.responsibleService).toBe("storage-cleanup");
        expect(task?.dueAt).toBe("2026-11-08T00:00:00.000Z");
        expect(task).toBeDefined();
        if (!task) return;
        const attempt = yield* f.queue.update({
          task,
          state: "reclaiming",
          reason: "Age limit reached",
          now: NOW,
        });
        yield* f.queue.update({
          task: attempt,
          state: "blocked",
          reason: "Allocator temporarily unavailable",
          now: NOW,
        });
        const restarted = yield* f.restart();
        const [observed] = yield* restarted.list;
        expect(observed?.state).toBe("blocked");
        expect(observed?.attempts).toBe(1);
        expect(observed?.nextAttemptAt).toBe("2026-11-04T00:05:00.000Z");
        const [updated] = yield* restarted.reconcile({
          leases: [lease()],
          owners: new Map([[lease().path, { threadIds: ["owner"], activityAt: NOW + DAY }]]),
          now: NOW + DAY,
          days: 7,
        });
        expect(updated?.ownerThreadIds).toEqual(["owner"]);
        expect(updated?.lastOwnerActivityAt).toBe("2026-11-05T00:00:00.000Z");
        expect(updated?.dueAt).toBe(task.dueAt);
      }),
  );

  it.effect("does not transfer an expired cleanup request onto a replacement generation", () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const [old] = yield* f.queue.reconcile({
        leases: [lease()],
        owners: new Map(),
        now: NOW,
        days: 7,
      });
      expect(old).toBeDefined();
      if (!old) return;
      yield* f.queue.update({ task: old, state: "reclaiming", reason: "Expired", now: NOW });
      const replacement = lease({ lease_id: "b".repeat(32), leased_at: "2026-11-04T00:00:00Z" });
      const [current] = yield* f.queue.reconcile({
        leases: [replacement],
        owners: new Map(),
        now: NOW,
        days: 7,
      });
      expect(current?.id).not.toBe(old.id);
      expect(current?.state).toBe("scheduled");
      expect(current?.dueAt).toBe("2026-11-11T00:00:00.000Z");
      const records = yield* f.queue.list;
      expect(records.find((record) => record.id === old.id)?.state).toBe("released");
      expect(
        (yield* Effect.result(
          f.queue.update({ task: old, state: "reclaiming", reason: "Stale retry", now: NOW }),
        ))._tag,
      ).toBe("Failure");
      expect((yield* f.queue.list).find((record) => record.id === old.id)?.state).toBe("released");
    }),
  );

  it.effect("keeps missing, future and changed acquisition times visibly blocked", () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const [first] = yield* f.queue.reconcile({
        leases: [lease()],
        owners: new Map(),
        now: NOW,
        days: 7,
      });
      for (const overrides of [
        { leased_at: "2026-11-03T00:00:00Z" },
        { lease_holder: "changed-owner" },
      ]) {
        const [changed] = yield* f.queue.reconcile({
          leases: [lease(overrides)],
          owners: new Map(),
          now: NOW,
          days: 7,
        });
        expect(changed?.state).toBe("blocked");
        expect(changed?.dueAt).toBe(first?.dueAt);
        expect(changed?.acquiredAt).toBe(first?.acquiredAt);
        expect(changed?.holder).toBe(first?.holder);
      }
      for (const leased_at of [null, "invalid", "2027-01-01T00:00:00Z"]) {
        const [blocked] = yield* f.queue.reconcile({
          leases: [
            lease({ lease_id: String(leased_at).length.toString(16).padStart(32, "0"), leased_at }),
          ],
          owners: new Map(),
          now: NOW,
          days: 7,
        });
        expect(blocked?.state).toBe("blocked");
        expect(blocked?.dueAt).toBeNull();
        for (const _attempt of [1, 2]) {
          const [reobserved] = yield* f.queue.reconcile({
            leases: [
              lease({
                lease_id: String(leased_at).length.toString(16).padStart(32, "0"),
                leased_at:
                  leased_at === "2027-01-01T00:00:00Z" ? leased_at : "2026-11-01T00:00:00Z",
              }),
            ],
            owners: new Map(),
            now: NOW + 400 * DAY,
            days: 7,
          });
          expect(reobserved?.state).toBe("blocked");
          expect(reobserved?.dueAt).toBeNull();
          expect(reobserved?.acquiredAt).toBe(leased_at);
        }
      }
    }),
  );

  it.effect("does not follow a substituted cleanup-record symlink", () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const [task] = yield* f.queue.reconcile({
        leases: [lease()],
        owners: new Map(),
        now: NOW,
        days: 7,
      });
      expect(task).toBeDefined();
      if (!task) return;
      const external = f.path.join(f.config.stateDir, "unrelated");
      yield* f.fs.writeFileString(external, "preserve");
      const record = f.path.join(f.config.stateDir, "lease-cleanup", `${task.id}.json`);
      yield* f.fs.remove(record);
      yield* f.fs.symlink(external, record);
      expect(
        (yield* Effect.result(f.queue.update({ task, state: "removed", reason: "Done", now: NOW })))
          ._tag,
      ).toBe("Failure");
      expect(yield* f.fs.readFileString(external)).toBe("preserve");
    }),
  );
});

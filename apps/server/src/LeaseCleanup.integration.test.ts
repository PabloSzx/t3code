import { assert, it } from "@effect/vitest";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import { TestClock } from "effect/testing";
import { ThreadId } from "@t3tools/contracts";
import * as ProcessRunner from "./processRunner.ts";
import * as StorageCleanup from "./storageCleanup.ts";
import * as ProjectionStore from "./orchestration-v2/ProjectionStore.ts";
import * as ProjectStore from "./orchestration-v2/ProjectStore.ts";
import * as Orchestrator from "./orchestration-v2/Orchestrator.ts";
import * as ProviderSessionManager from "./orchestration-v2/ProviderSessionManager.ts";
import * as TerminalManager from "./terminal/Manager.ts";
import * as GitVcsDriver from "./vcs/GitVcsDriver.ts";
import * as GitManager from "./git/GitManager.ts";
import * as Sqlite from "./persistence/Sqlite.ts";
import { ServerActivation } from "./serverActivation.ts";
import { fixture, testLayer } from "./vcs/TreehouseWorktrees.testkit.ts";

it.effect(
  "owns an unregistered repository's legacy lease through failure, restart and verified deletion",
  () =>
    Effect.gen(function* () {
      yield* TestClock.setTime(Date.parse("2026-01-10T00:00:00Z"));
      const f = yield* fixture();
      yield* f.settings.updateSettings({ storageCleanup: { leaseHardExpiryDays: 7 } });
      const runner = yield* ProcessRunner.ProcessRunner;
      assert.equal(
        (yield* runner.run({
          command: f.binary,
          args: [
            "get",
            "--lease",
            "--json",
            "--lease-holder",
            "retired-external-task",
            "--no-fetch",
          ],
          cwd: f.cwd,
          stdin: "",
        })).code,
        0,
      );
      const failureMarker = f.path.join(f.root, "fail-destroy");
      yield* f.fs.writeFileString(failureMarker, "");
      yield* f.fs.writeFileString(f.path.join(f.slot, "local-work"), "synthetic local content");
      let ownershipUnavailable = false;
      const ports = Layer.mergeAll(
        Layer.mock(ProjectionStore.ProjectionStoreV2)({
          getShellSnapshot: () =>
            ownershipUnavailable
              ? Effect.fail(
                  new ProjectionStore.ProjectionStoreReadError({
                    threadId: ThreadId.make("unknown"),
                  }),
                )
              : Effect.succeed({
                  schemaVersion: 1,
                  snapshotSequence: 0,
                  projects: [],
                  threads: [],
                  archivedThreads: [],
                }),
        }),
        Layer.mock(ProjectStore.ProjectStoreV2)({ listShells: () => Effect.succeed([]) }),
        Layer.mock(Orchestrator.OrchestratorV2)({ streamDomainEvents: Stream.empty }),
        Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({}),
        Layer.mock(TerminalManager.TerminalManager)({
          subscribeMetadata: () => Effect.succeed(() => {}),
        }),
        Layer.mock(GitVcsDriver.GitVcsDriver)({}),
        Layer.mock(GitManager.GitManager)({ invalidateStatus: () => Effect.void }),
        Sqlite.layerMemory,
      );
      const start = () =>
        Layer.build(StorageCleanup.layer.pipe(Layer.provide(ports))).pipe(
          Effect.provideService(ServerActivation, Effect.never),
          Effect.map((context) => Context.get(context, StorageCleanup.StorageCleanup)),
        );
      const cleanup = yield* start();
      const failed = yield* cleanup.runNow;
      assert.equal(failed.counts.failed, 1);
      const [pending] = yield* cleanup.leaseTasks;
      if (!pending) return assert.fail("Missing durable cleanup responsibility");
      assert.equal(pending.responsibleService, "storage-cleanup");
      assert.deepEqual(pending.ownerThreadIds, []);
      assert.equal(pending.state, "blocked");
      assert.equal(pending.attempts, 1);
      assert.equal(
        yield* f.fs.readFileString(f.path.join(f.slot, "local-work")),
        "synthetic local content",
      );
      const restarted = yield* start();
      assert.equal((yield* restarted.leaseTasks)[0]?.id, pending.id);
      yield* f.fs.remove(failureMarker);
      ownershipUnavailable = true;
      yield* TestClock.adjust("5 minutes");
      yield* restarted.runNow;
      const [unverified] = yield* restarted.leaseTasks;
      assert.equal(unverified?.state, "blocked");
      assert.equal(unverified?.reason, "Task ownership lookup failed");
      assert.equal(unverified?.attempts, 1);
      assert.isTrue(yield* f.fs.exists(f.slot));
      ownershipUnavailable = false;
      yield* TestClock.adjust("5 minutes");
      const result = yield* restarted.runNow;
      assert.equal(result.counts.removed, 1);
      assert.isFalse(yield* f.fs.exists(f.slot));
      const [finished] = yield* restarted.leaseTasks;
      assert.equal(finished?.id, pending.id);
      assert.equal(finished?.state, "removed");
      assert.equal(finished?.attempts, 2);
      assert.equal(yield* f.git({ args: ["rev-parse", "refs/heads/parent"] }), f.parentSha);
    }).pipe(Effect.provide(testLayer)),
);

import { assert, it } from "@effect/vitest";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { TestClock } from "effect/testing";
import { ServerSettingsPatch } from "@t3tools/contracts";
import * as ProcessRunner from "./processRunner.ts";
import * as StorageCleanup from "./storageCleanup.ts";
import * as ProjectStore from "./orchestration-v2/ProjectStore.ts";
import * as ProjectionStore from "./orchestration-v2/ProjectionStore.ts";
import * as ProviderSessionManager from "./orchestration-v2/ProviderSessionManager.ts";
import * as GitVcsDriver from "./vcs/GitVcsDriver.ts";
import * as GitManager from "./git/GitManager.ts";
import * as Sqlite from "./persistence/Sqlite.ts";
import * as Orchestrator from "./orchestration-v2/Orchestrator.ts";
import * as TerminalManager from "./terminal/Manager.ts";
import { ServerActivation } from "./serverActivation.ts";
import { fixture, testLayer } from "./vcs/TreehouseWorktrees.testkit.ts";

const decodeSettingsPatch = Schema.decodeUnknownEffect(ServerSettingsPatch);

it.effect("does not reclaim an external lease using the retired acquisition-age policy", () =>
  Effect.gen(function* () {
    yield* TestClock.setTime(Date.parse("2026-01-10T00:00:00Z"));
    const f = yield* fixture();
    yield* f.settings.updateSettings(
      yield* decodeSettingsPatch({ storageCleanup: { leaseHardExpiryDays: 7 } }),
    );
    const runner = yield* ProcessRunner.ProcessRunner;
    assert.equal(
      (yield* runner.run({
        command: f.binary,
        args: ["get", "--lease", "--json", "--lease-holder", "external-task", "--no-fetch"],
        cwd: f.cwd,
        stdin: "",
      })).code,
      0,
    );
    yield* f.fs.writeFileString(f.path.join(f.slot, "local-work"), "preserve external work");
    const ports = Layer.mergeAll(
      Layer.mock(ProjectionStore.ProjectionStoreV2)({}),
      Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({}),
      Layer.mock(GitVcsDriver.GitVcsDriver)({}),
      Layer.mock(GitManager.GitManager)({}),
      Sqlite.layerMemory,
      Layer.mock(ProjectStore.ProjectStoreV2)({ listShells: () => Effect.succeed([]) }),
      Layer.mock(Orchestrator.OrchestratorV2)({ streamDomainEvents: Stream.empty }),
      Layer.mock(TerminalManager.TerminalManager)({
        subscribeMetadata: () => Effect.succeed(() => {}),
      }),
    );
    const context = yield* Layer.build(StorageCleanup.layer.pipe(Layer.provide(ports))).pipe(
      Effect.provideService(ServerActivation, Effect.never),
    );
    const cleanup = Context.get(context, StorageCleanup.StorageCleanup);
    const report = yield* cleanup.runNow;
    assert.equal(report.counts.removed, 0);
    assert.equal(
      yield* f.fs.readFileString(f.path.join(f.slot, "local-work")),
      "preserve external work",
    );
    assert.deepEqual(yield* cleanup.leaseTasks, []);
  }).pipe(Effect.provide(testLayer)),
);

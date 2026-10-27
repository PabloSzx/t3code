import { assert, it, expect } from "@effect/vitest";
import { WorkspaceOperation, type GitManagerServiceError } from "@t3tools/contracts";
import { accessFixture, testLayer as treehouseLayer } from "../vcs/TreehouseWorktrees.testkit.ts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import * as NodeServices from "@effect/platform-node/NodeServices";
import * as FileSystem from "effect/FileSystem";

import * as ServerConfig from "../config.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as VcsProcess from "../vcs/VcsProcess.ts";
import * as GitManager from "./GitManager.ts";
import * as GitWorkflowService from "./GitWorkflowService.ts";
import * as VcsProvisioningService from "../vcs/VcsProvisioningService.ts";
import * as GitVcsDriver from "../vcs/GitVcsDriver.ts";
import * as VcsDriverRegistry from "../vcs/VcsDriverRegistry.ts";

it.live(
  "rejects recycled Git queries and mutations before repository detection, including aliases and legacy requests",
  () =>
    Effect.gen(function* () {
      const f = yield* accessFixture();
      const workflow = yield* GitWorkflowService.GitWorkflowService;
      const provisioning = yield* VcsProvisioningService.make.pipe(
        Effect.provide(VcsDriverRegistry.layer.pipe(Layer.provide(VcsProcess.layer))),
      );
      const alias = f.path.join(f.root, "git-alias");
      yield* f.fs.symlink(f.slot, alias);
      expect(
        (yield* workflow
          .listRefs({ cwd: alias })
          .pipe(Effect.provideService(WorkspaceOperation, f.context))).refs.some(
          (ref) => ref.current && ref.name === "access-owner",
        ),
      ).toBe(true);
      const foreign = yield* f.recycle;
      for (const context of [f.context, undefined]) {
        const requests: ReadonlyArray<Effect.Effect<unknown, GitManagerServiceError>> = [
          workflow.listRefs({ cwd: alias }),
          workflow.isRepository(alias),
          workflow.createRef({ cwd: alias, refName: "must-not-create", switchRef: true }),
        ];
        expect(
          yield* provisioning
            .initRepository({ cwd: f.slot })
            .pipe(Effect.provideService(WorkspaceOperation, context), Effect.result),
        ).toMatchObject({ _tag: "Failure", failure: { cause: { _tag: "TreehouseLeaseError" } } });
        for (const effect of requests) {
          expect(
            yield* effect.pipe(Effect.provideService(WorkspaceOperation, context), Effect.result),
          ).toMatchObject({ _tag: "Failure", failure: { cause: { _tag: "TreehouseLeaseError" } } });
        }
      }
      expect(yield* f.git({ cwd: f.slot, args: ["branch", "--show-current"] })).toBe(
        "foreign-owner",
      );
      expect(
        (yield* workflow
          .listRefs({ cwd: alias })
          .pipe(Effect.provideService(WorkspaceOperation, foreign))).refs.some(
          (ref) => ref.current && ref.name === "foreign-owner",
        ),
      ).toBe(true);
      expect(yield* workflow.isRepository(f.cwd)).toBe(true);
      expect(yield* workflow.hasRefNamed({ cwd: f.cwd, refName: "must-not-create" })).toBe(false);
    }).pipe(
      Effect.provide(
        GitWorkflowService.layer.pipe(
          Layer.provide(VcsDriverRegistry.layer.pipe(Layer.provide(VcsProcess.layer))),
          Layer.provide(GitVcsDriver.layer),
          Layer.provide(Layer.mock(GitManager.GitManager)({})),
          Layer.provideMerge(treehouseLayer),
        ),
      ),
    ),
);

it.layer(NodeServices.layer)("GitWorkflowService", (it) => {
  it.effect("finds a name among the refs of every namespace", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const cwd = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-ref-named-" });
      const workflow = yield* GitWorkflowService.GitWorkflowService;
      const driver = yield* GitVcsDriver.GitVcsDriver;
      for (const args of [
        ["init", "-b", "main"],
        ["-c", "user.email=t@t", "-c", "user.name=t", "commit", "--allow-empty", "-m", "init"],
        ["update-ref", "refs/remotes/upstream/feature/topic", "HEAD"],
        ["symbolic-ref", "refs/remotes/origin/alias", "refs/heads/main"],
        ["update-ref", "refs/mirror/origin/mirrored", "HEAD"],
      ]) {
        yield* driver.execute({ operation: "test.setupRepo", cwd, args });
      }
      const found = yield* Effect.forEach(
        ["main", "feature/topic", "alias", "mirrored", "t3/renamed", "feature/*"],
        (refName) => workflow.hasRefNamed({ cwd, refName }),
      );
      assert.deepEqual(found, [true, true, true, true, false, false]);
    }).pipe(
      Effect.scoped,
      Effect.provide(
        GitWorkflowService.layer.pipe(
          Layer.provide(
            Layer.mock(VcsDriverRegistry.VcsDriverRegistry)({
              resolve: () => Effect.succeed({ kind: "git" } as VcsDriverRegistry.VcsDriverHandle),
            }),
          ),
          Layer.provide(Layer.mock(GitManager.GitManager)({})),
          Layer.provideMerge(GitVcsDriver.layer),
          Layer.provide(ServerSettings.layerTest()),
          Layer.provideMerge(VcsProcess.layer),
          Layer.provideMerge(
            ServerConfig.layerTest(process.cwd(), { prefix: "t3-ref-named-" }).pipe(
              Layer.provide(NodeServices.layer),
            ),
          ),
          Layer.provideMerge(NodeServices.layer),
        ),
      ),
    ),
  );
});

import { assert, describe, expect, it } from "@effect/vitest";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import {
  CheckpointScopeId,
  ContextTransferId,
  EventId,
  NodeId,
  MessageId,
  ProjectId,
  ProviderInstanceId,
  ProviderThreadId,
  RunId,
  ThreadId,
  type OrchestrationV2AppThread,
} from "@t3tools/contracts";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as CheckpointStore from "../checkpointing/CheckpointStore.ts";
import * as CheckpointService from "../orchestration-v2/CheckpointService.ts";
import * as Sqlite from "../persistence/Sqlite.ts";
import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as EventStore from "../orchestration-v2/EventStore.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as ThreadForkService from "../orchestration-v2/ThreadForkService.ts";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as ProjectService from "../project/ProjectService.ts";
import * as WorkspaceAccess from "../workspace/WorkspaceAccess.ts";
import * as PlatformError from "effect/PlatformError";
import * as Result from "effect/Result";
import * as Exit from "effect/Exit";
import * as Scope from "effect/Scope";
import * as ServerConfig from "../config.ts";
import { fixture, testLayer } from "./TreehouseWorktrees.testkit.ts";
import * as TreehouseLeases from "../workspace/treehouseLeases.ts";
import * as TreehouseWorktrees from "./TreehouseWorktrees.ts";
import { makeGitVcsDriverCore } from "./GitVcsDriverCore.ts";
import { removeLinkedWorktree } from "./worktreeRemoval.ts";

const provide = <A, E, R>(effect: Effect.Effect<A, E, R>) => effect.pipe(Effect.provide(testLayer));

const historyLayer = EventSink.layer.pipe(
  Layer.provideMerge(
    Layer.merge(EventStore.layer, ProjectionStore.layer).pipe(
      Layer.provideMerge(Sqlite.layerMemory),
    ),
  ),
  Layer.provideMerge(testLayer),
);

const returnedFamily = Effect.fn("test.returnedTreehouseFamily")(function* () {
  const f = yield* fixture();
  yield* f.driver.createWorktree({
    cwd: f.cwd,
    refName: f.parentSha,
    newRefName: "retained-family",
    path: null,
  });
  const sink = yield* EventSink.EventSinkV2;
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const now = yield* DateTime.now;
  const ownerId = ThreadId.make("retained-owner");
  const owner: OrchestrationV2AppThread = {
    id: ownerId,
    projectId: ProjectId.make("retained-project"),
    title: "Retained owner",
    createdBy: "user",
    creationSource: "web",
    providerInstanceId: ProviderInstanceId.make("codex"),
    modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "test" },
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: "retained-family",
    worktreePath: f.slot,
    activeProviderThreadId: null,
    lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: ownerId },
    forkedFrom: null,
    createdAt: now,
    updatedAt: now,
    archivedAt: null,
    settledOverride: null,
    settledAt: null,
    lastVisitedAt: null,
    deletedAt: null,
  };
  const children = ["retained-first", "retained-second"].map(
    (id, index): OrchestrationV2AppThread => ({
      ...owner,
      id: ThreadId.make(id),
      createdBy: "agent",
      creationSource: "provider",
      archivedAt: index === 1 ? now : null,
      lineage: { parentThreadId: ownerId, relationshipToParent: "subagent", rootThreadId: ownerId },
    }),
  );
  for (const thread of [owner, ...children]) {
    yield* sink.write({
      events: [
        {
          id: EventId.make(`create-${thread.id}`),
          type: "thread.created",
          threadId: thread.id,
          occurredAt: now,
          payload: thread,
        },
      ],
    });
  }
  yield* sink.write({
    events: [
      {
        id: EventId.make("delete-owner"),
        type: "thread.deleted",
        threadId: ownerId,
        occurredAt: now,
        payload: { ...owner, deletedAt: now },
      },
    ],
  });
  const treehouse = yield* TreehouseWorktrees.TreehouseWorktrees.pipe(
    Effect.provide(TreehouseWorktrees.layer),
  );
  const threadIds = [ownerId, ...children.map((thread) => thread.id)];
  assert.isNull(
    yield* treehouse.returnIdle({
      cwd: f.cwd,
      path: f.slot,
      branch: owner.branch!,
      headSha: f.parentSha,
      threadIds,
      recheck: Effect.succeed(null),
    }),
  );
  return { f, sink, projections, now, owner, children, thread: children[0]!, threadIds, treehouse };
});

const resumeFamily = Effect.fn("test.resumeTreehouseFamily")(function* (
  family: Effect.Success<ReturnType<typeof returnedFamily>>,
) {
  const { f, thread, treehouse } = family;
  yield* f.driver.createWorktree({
    cwd: f.cwd,
    refName: f.parentSha,
    newRefName: "unrelated-occupant",
    path: null,
  });
  yield* f.fs.writeFileString(f.path.join(f.root, "next-slot"), "");
  const scope = yield* Scope.make();
  yield* Effect.addFinalizer((exit) => Scope.close(scope, exit));
  const resumed = yield* treehouse.resume({
    cwd: f.cwd,
    path: f.slot,
    branch: thread.branch!,
    threadId: thread.id,
    preparationScope: scope,
  });
  return { path: resumed, scope };
});

describe("Treehouse worktrees", () => {
  it.live(
    "keeps native allocation and removal by default, ignoring retired repository selectors",
    () =>
      Effect.gen(function* () {
        const f = yield* fixture({ treehouseEnabled: false });
        yield* f.git({ args: ["config", "t3.worktree-manager", "treehouse"] });
        yield* f.git({ args: ["config", "t3.treehouse-binary", f.binary] });
        const nativePath = f.path.join(f.root, "native");
        const created = yield* f.driver.createWorktree({
          cwd: f.cwd,
          refName: "main",
          newRefName: "native-branch",
          path: nativePath,
        });
        assert.strictEqual(created.worktree.path, nativePath);
        assert.isFalse(yield* f.fs.exists(f.path.join(f.root, "calls")));
        assert.strictEqual(yield* f.git({ args: ["rev-parse", "HEAD"] }), f.parentSha);
        yield* f.driver.removeWorktree({ cwd: f.cwd, path: nativePath });
        assert.isFalse(yield* f.fs.exists(nativePath));
        assert.strictEqual(
          yield* f.git({ args: ["rev-parse", "refs/heads/native-branch"] }),
          f.baseSha,
        );
      }).pipe(provide),
  );
  it.live(
    "applies host enablement to unrelated repositories without repository configuration",
    () =>
      Effect.gen(function* () {
        for (let index = 0; index < 2; index++) {
          const f = yield* fixture();
          const created = yield* f.driver.createWorktree({
            cwd: f.cwd,
            refName: f.parentSha,
            path: null,
          });
          assert.strictEqual(created.worktree.path, f.slot);
          assert.strictEqual(
            yield* f.git({ cwd: f.slot, args: ["rev-parse", "HEAD"] }),
            f.parentSha,
          );
          assert.strictEqual(yield* f.git({ args: ["branch", "--show-current"] }), "parent");
        }
      }).pipe(provide),
  );
  it.live.each(["relative/treehouse", "/missing-t3-test/treehouse"])(
    "refuses unusable host binary %s without creating a native worktree",
    (binary) =>
      Effect.gen(function* () {
        const f = yield* fixture();
        yield* f.settings.updateSettings({ treehouseBinaryPath: binary });
        const result = yield* f.driver
          .createWorktree({ cwd: f.cwd, refName: "main", path: null })
          .pipe(Effect.result);
        assert.isTrue(Result.isFailure(result));
        if (Result.isFailure(result)) assert.include(result.failure.message, "Settings");
        assert.strictEqual(
          (yield* f.git({ args: ["worktree", "list", "--porcelain"] }))
            .split("\n")
            .filter((line) => line.startsWith("worktree ")).length,
          1,
        );
      }).pipe(provide),
  );
  it.live.each(["parent", "commit", "origin/main", "HEAD", "HEAD~1"])(
    "resolves %s in the caller checkout before creating a branch",
    (ref) =>
      Effect.gen(function* () {
        const f = yield* fixture();
        const created = yield* f.driver.createWorktree({
          cwd: f.cwd,
          refName: ref === "commit" ? f.parentSha : ref,
          newRefName: "pablosaez/child",
          path: null,
        });
        assert.strictEqual(created.worktree.path, f.slot);
        assert.strictEqual(
          yield* f.git({ cwd: f.slot, args: ["rev-parse", "HEAD"] }),
          ref === "HEAD~1" ? f.baseSha : f.parentSha,
        );
        assert.strictEqual(
          yield* f.git({ cwd: f.slot, args: ["branch", "--show-current"] }),
          "pablosaez/child",
        );
        assert.strictEqual(
          yield* f.fs.readFileString(f.path.join(f.slot, "file")),
          ref === "HEAD~1" ? "main\n" : "parent\n",
        );
        assert.deepEqual(yield* f.fs.readDirectory(f.path.join(f.root, "pool")), [
          "1",
          "treehouse-state.json",
        ]);
      }).pipe(provide),
  );
  it.live(
    "preserves an existing branch checkout and returns the exact unbound lease after restart",
    () =>
      Effect.gen(function* () {
        const f = yield* fixture();
        yield* f.driver.createWorktree({ cwd: f.cwd, refName: "main", path: null });
        assert.strictEqual(
          yield* f.git({ cwd: f.slot, args: ["branch", "--show-current"] }),
          "main",
        );
        const initialLease = yield* f.leases.read(f.slot);
        assert.isNotNull(initialLease);
        const restarted = yield* makeGitVcsDriverCore();
        const lock = f.path.join(f.registry, "lifecycle.lock");
        yield* f.fs.makeDirectory(lock);
        assert.isTrue(
          Result.isFailure(
            yield* restarted.removeWorktree({ cwd: f.cwd, path: f.slot }).pipe(Effect.result),
          ),
        );
        yield* f.fs.remove(lock, { recursive: true });
        yield* restarted.removeWorktree({ cwd: f.cwd, path: f.slot, force: true });
        assert.isTrue(yield* f.fs.exists(f.slot));
        const calls = yield* f.fs.readFileString(f.path.join(f.root, "calls"));
        assert.include(
          calls,
          `return ${f.slot} --if-lease-id ${initialLease!.record.lease.lease_id}`,
        );
        assert.notInclude(calls, "--force");
        assert.isTrue(Result.isFailure(yield* f.leases.read(f.slot).pipe(Effect.result)));
      }).pipe(provide),
  );
  it.live.each(["standalone", "thread handoff"] as const)(
    "protects preparation until completion of %s creation",
    (handoff) =>
      Effect.gen(function* () {
        const f = yield* fixture();
        const scope = yield* Scope.make();
        yield* Effect.addFinalizer((exit) => Scope.close(scope, exit));
        const claimed = yield* Deferred.make<void>();
        const finish = yield* Deferred.make<void>();
        const creation = yield* f.driver
          .createWorktree(
            { cwd: f.cwd, refName: f.parentSha, path: null },
            {
              ...(handoff === "thread handoff" ? { preparationScope: scope } : {}),
              progress: {
                onWorktreeClaimed: () =>
                  Deferred.succeed(claimed, undefined).pipe(Effect.andThen(Deferred.await(finish))),
              },
            },
          )
          .pipe(Effect.forkChild());
        yield* Deferred.await(claimed);
        const duringPreparation = yield* f.driver
          .removeWorktree({ cwd: f.cwd, path: f.slot })
          .pipe(Effect.result);
        yield* Deferred.succeed(finish, undefined);
        yield* Fiber.join(creation);
        assert.isTrue(Result.isFailure(duringPreparation), "preparation must block return");
        if (handoff === "thread handoff") {
          assert.isTrue(
            Result.isFailure(
              yield* f.driver.removeWorktree({ cwd: f.cwd, path: f.slot }).pipe(Effect.result),
            ),
            "binding handoff must block return",
          );
          yield* f.leases.bindThread({ path: f.slot, threadId: "launch-thread", sequence: 1 });
          yield* Scope.close(scope, Exit.void);
          yield* f.leases.assertThread({ path: f.slot, threadId: "launch-thread" });
          yield* f.leases.commitThread({
            path: null,
            threadId: "launch-thread",
            deleted: false,
            sequence: 2,
          });
        }
        yield* f.driver.removeWorktree({ cwd: f.cwd, path: f.slot });
        assert.isTrue(Result.isFailure(yield* f.leases.read(f.slot).pipe(Effect.result)));
      }).pipe(provide),
  );
  it.live.each(["same slot", "different slot"] as const)(
    "resumes a returned family on a new identity (%s) without adopting a recycled lease",
    (location) =>
      Effect.gen(function* () {
        const f = yield* fixture();
        const treehouse = yield* TreehouseWorktrees.TreehouseWorktrees.pipe(
          Effect.provide(TreehouseWorktrees.layer),
        );
        yield* f.driver.createWorktree({
          cwd: f.cwd,
          refName: f.parentSha,
          newRefName: "feature",
          path: null,
        });
        const original = yield* f.leases.read(f.slot);
        for (const threadId of ["parent-thread", "child-thread"])
          yield* f.leases.bindThread({ path: f.slot, threadId, sequence: 1 });
        assert.isNull(
          yield* treehouse.returnIdle({
            cwd: f.cwd,
            path: f.slot,
            branch: "feature",
            headSha: f.parentSha,
            threadIds: ["parent-thread", "child-thread"],
            recheck: Effect.succeed(null),
          }),
        );
        if (location === "different slot") {
          yield* f.driver.createWorktree({
            cwd: f.cwd,
            refName: f.parentSha,
            newRefName: "unrelated",
            path: null,
          });
          yield* f.fs.writeFileString(f.path.join(f.root, "next-slot"), "");
        }
        yield* f.settings.updateSettings({
          treehouseWorktreesEnabled: false,
          treehouseBinaryPath: "/invalid-t3-test/binary",
        });
        const target = location === "same slot" ? f.slot : f.path.join(f.root, "pool", "2", "repo");
        const resume = (threadId: string) =>
          treehouse.resume({
            cwd: f.cwd,
            path: f.slot,
            branch: "feature",
            threadId,
            preparationScope: yieldScope,
          });
        const yieldScope = yield* Effect.scope;
        assert.strictEqual(yield* resume("parent-thread"), target);
        const newLease = yield* f.leases.read(target);
        assert.notEqual(newLease!.record.lease.lease_id, original!.record.lease.lease_id);
        yield* f.leases.bindThread({ path: target, threadId: "parent-thread", sequence: 2 });
        assert.strictEqual(yield* resume("child-thread"), target);
        yield* f.leases.bindThread({ path: target, threadId: "child-thread", sequence: 3 });
        yield* f.leases.assertThread({ path: target, threadId: "child-thread" });
        assert.strictEqual(
          yield* f.git({ cwd: target, args: ["branch", "--show-current"] }),
          "feature",
        );
        assert.strictEqual(yield* f.git({ cwd: target, args: ["rev-parse", "HEAD"] }), f.parentSha);
        if (location === "different slot") {
          assert.strictEqual(
            yield* f.git({ cwd: f.slot, args: ["branch", "--show-current"] }),
            "unrelated",
          );
          assert.isTrue(
            Result.isFailure(
              yield* f.leases
                .assertThread({ path: f.slot, threadId: "child-thread" })
                .pipe(Effect.result),
            ),
          );
        }
      }).pipe(provide),
  );

  it.live.each([
    "provider",
    "terminal",
    "preparation",
    "another thread",
    "untracked",
    "ignored",
    "tracked",
    "branch",
    "repository",
  ] as const)("retains an idle lease protected by %s", (protection) =>
    Effect.gen(function* () {
      const f = yield* fixture();
      const scope = yield* Scope.make();
      yield* Effect.addFinalizer((exit) => Scope.close(scope, exit));
      yield* f.driver.createWorktree(
        { cwd: f.cwd, refName: f.parentSha, newRefName: "feature", path: null },
        protection === "preparation" ? { preparationScope: scope } : undefined,
      );
      yield* f.leases.bindThread({ path: f.slot, threadId: "owner", sequence: 1 });
      if (protection === "provider" || protection === "terminal") {
        const release = yield* f.leases.pinUse({
          path: f.slot,
          threadId: "owner",
          kind: protection,
        });
        yield* Effect.addFinalizer(() => release);
      }
      if (protection === "another thread")
        yield* f.leases.bindThread({ path: f.slot, threadId: "other", sequence: 2 });
      if (protection === "tracked")
        yield* f.fs.writeFileString(f.path.join(f.slot, "file"), "keep edits\n");
      if (protection === "untracked")
        yield* f.fs.writeFileString(f.path.join(f.slot, "notes"), "keep notes\n");
      if (protection === "ignored") {
        yield* f.fs.writeFileString(
          f.path.join(f.cwd, ".git", "info", "exclude"),
          "ignored-file\n",
        );
        yield* f.fs.writeFileString(f.path.join(f.slot, "ignored-file"), "keep ignored\n");
      }
      if (protection === "branch")
        yield* f.git({ cwd: f.slot, args: ["switch", "-c", "user-branch"] });
      const treehouse = yield* TreehouseWorktrees.TreehouseWorktrees.pipe(
        Effect.provide(TreehouseWorktrees.layer),
      );
      const result = yield* treehouse
        .returnIdle({
          cwd: protection === "repository" ? f.slot : f.cwd,
          path: f.slot,
          branch: "feature",
          headSha: f.parentSha,
          threadIds: ["owner"],
          recheck: Effect.succeed(null),
        })
        .pipe(Effect.result);
      assert.isTrue(Result.isFailure(result) || result.success !== null);
      assert.isNotNull(yield* f.leases.read(f.slot));
      assert.notInclude(yield* f.fs.readFileString(f.path.join(f.root, "calls")), "return ");
      if (protection === "preparation") {
        yield* Scope.close(scope, Exit.void);
        assert.isNull(
          yield* treehouse.returnIdle({
            cwd: f.cwd,
            path: f.slot,
            branch: "feature",
            headSha: f.parentSha,
            threadIds: ["owner"],
            recheck: Effect.succeed(null),
          }),
        );
      }
    }).pipe(provide),
  );

  it.live.each(["external return", "durable marking"] as const)(
    "blocks retry and resume after an uncertain %s",
    (failure) =>
      Effect.gen(function* () {
        const f = yield* fixture();
        yield* f.driver.createWorktree({
          cwd: f.cwd,
          refName: f.parentSha,
          newRefName: "feature",
          path: null,
        });
        yield* f.leases.bindThread({ path: f.slot, threadId: "owner", sequence: 1 });
        if (failure === "external return")
          yield* f.fs.writeFileString(f.path.join(f.root, "fail-return"), "");
        const treehouse = yield* TreehouseWorktrees.TreehouseWorktrees.pipe(
          Effect.provide(TreehouseWorktrees.layer),
          Effect.provideService(FileSystem.FileSystem, {
            ...f.fs,
            rename: (from, to) =>
              Effect.gen(function* () {
                if (
                  failure === "durable marking" &&
                  to.startsWith(f.path.join(f.registry, "leases")) &&
                  (yield* f.fs.readFileString(from)).includes('"returned":true')
                )
                  return yield* PlatformError.systemError({
                    _tag: "PermissionDenied",
                    module: "FileSystem",
                    method: "rename",
                    pathOrDescriptor: to,
                  });
                yield* f.fs.rename(from, to);
              }),
          }),
        );
        const request = {
          cwd: f.cwd,
          path: f.slot,
          branch: "feature",
          headSha: f.parentSha,
          threadIds: ["owner"],
          recheck: Effect.succeed(null),
        };
        assert.isTrue(Result.isFailure(yield* treehouse.returnIdle(request).pipe(Effect.result)));
        assert.isTrue(Result.isFailure(yield* treehouse.returnIdle(request).pipe(Effect.result)));
        assert.isTrue(
          Result.isFailure(
            yield* treehouse
              .resume({
                cwd: f.cwd,
                path: f.slot,
                branch: "feature",
                threadId: "owner",
                preparationScope: yield* Effect.scope,
              })
              .pipe(Effect.result),
          ),
        );
        const evidence = yield* f.leases.releasedThread({ path: f.slot, threadId: "owner" });
        assert.isFalse(evidence!.record.returned);
        const calls = yield* f.fs.readFileString(f.path.join(f.root, "calls"));
        assert.strictEqual(
          calls.split("\n").filter((line) => line.startsWith("return ")).length,
          1,
        );
        assert.strictEqual(calls.split("\n").filter((line) => line.startsWith("get ")).length, 1);
      }).pipe(provide),
  );

  it.live.each(["allocation failure", "interruption"] as const)(
    "does not allocate twice after resume %s",
    (failure) =>
      Effect.gen(function* () {
        const f = yield* fixture();
        yield* f.driver.createWorktree({
          cwd: f.cwd,
          refName: f.parentSha,
          newRefName: "feature",
          path: null,
        });
        yield* f.leases.bindThread({ path: f.slot, threadId: "owner", sequence: 1 });
        const original = yield* TreehouseWorktrees.TreehouseWorktrees.pipe(
          Effect.provide(TreehouseWorktrees.layer),
        );
        yield* original.returnIdle({
          cwd: f.cwd,
          path: f.slot,
          branch: "feature",
          headSha: f.parentSha,
          threadIds: ["owner"],
          recheck: Effect.succeed(null),
        });
        if (failure === "allocation failure")
          yield* f.fs.writeFileString(f.path.join(f.root, "fail-get"), "");
        const persisted = yield* Deferred.make<void>();
        const treehouse = yield* TreehouseWorktrees.TreehouseWorktrees.pipe(
          Effect.provide(TreehouseWorktrees.layer),
          Effect.provideService(FileSystem.FileSystem, {
            ...f.fs,
            rename: (from, to) =>
              Effect.gen(function* () {
                const pause =
                  failure === "interruption" &&
                  to.startsWith(f.path.join(f.registry, "leases")) &&
                  (yield* f.fs.readFileString(from)).includes('"resuming":true');
                yield* f.fs.rename(from, to);
                if (pause) {
                  yield* Deferred.succeed(persisted, undefined);
                  return yield* Effect.never;
                }
              }),
          }),
        );
        const request = {
          cwd: f.cwd,
          path: f.slot,
          branch: "feature",
          threadId: "owner",
          preparationScope: yield* Effect.scope,
        };
        if (failure === "interruption") {
          const operation = yield* treehouse.resume(request).pipe(Effect.forkChild());
          yield* Deferred.await(persisted);
          yield* Fiber.interrupt(operation);
        } else
          assert.isTrue(Result.isFailure(yield* treehouse.resume(request).pipe(Effect.result)));
        assert.isTrue(Result.isFailure(yield* original.resume(request).pipe(Effect.result)));
        const calls = yield* f.fs.readFileString(f.path.join(f.root, "calls"));
        assert.strictEqual(
          calls.split("\n").filter((line) => line.startsWith("get ")).length,
          failure === "interruption" ? 1 : 2,
        );
        const evidence = yield* f.leases.releasedThread({ path: f.slot, threadId: "owner" });
        assert.isTrue(evidence!.record.release!.resuming);
        assert.isUndefined(evidence!.record.release!.replacement);
      }).pipe(provide),
  );

  it.live("retains failed allocations without native fallback", () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      const failGet = f.path.join(f.root, "fail-get");
      yield* f.fs.writeFileString(failGet, "");
      assert.isTrue(
        Result.isFailure(
          yield* f.driver
            .createWorktree({ cwd: f.cwd, refName: "main", path: null })
            .pipe(Effect.result),
        ),
      );
      assert.strictEqual(
        (yield* f.git({ args: ["worktree", "list", "--porcelain"] }))
          .split("\n")
          .filter((line) => line.startsWith("worktree ")).length,
        1,
      );
      yield* f.fs.remove(failGet);
      assert.isTrue(
        Result.isFailure(
          yield* f.driver
            .createWorktree({ cwd: f.cwd, refName: f.parentSha, newRefName: "parent", path: null })
            .pipe(Effect.result),
        ),
      );
      assert.isNotNull(yield* f.leases.read(f.slot));
      yield* f.fs.writeFileString(f.path.join(f.root, "fail-return"), "");
      assert.isTrue(
        Result.isFailure(
          yield* f.driver
            .removeWorktree({ cwd: f.cwd, path: f.slot, force: true })
            .pipe(Effect.result),
        ),
      );
      assert.isNotNull(yield* f.leases.read(f.slot));
      assert.include(yield* f.git({ args: ["worktree", "list", "--porcelain"] }), f.slot);
    }).pipe(provide),
  );
  it.live(
    "keeps shared references and live uses protected after disablement, then allows explicit return",
    () =>
      Effect.gen(function* () {
        const f = yield* fixture();
        yield* f.driver.createWorktree({ cwd: f.cwd, refName: f.parentSha, path: null });
        yield* f.leases.bindThread({ path: f.slot, threadId: "parent-thread", sequence: 1 });
        yield* f.leases.bindThread({ path: f.slot, threadId: "child-thread", sequence: 2 });
        const releaseProvider = yield* f.leases.pinUse({
          path: f.slot,
          threadId: "parent-thread",
          kind: "provider",
        });
        const releaseTerminal = yield* f.leases.pinUse({
          path: f.slot,
          threadId: "child-thread",
          kind: "terminal",
        });
        yield* f.settings.updateSettings({
          treehouseWorktreesEnabled: false,
          treehouseBinaryPath: "/missing-t3-test/treehouse",
        });
        const nativePath = f.path.join(f.root, "native-after-disable");
        yield* f.driver.createWorktree({ cwd: f.cwd, refName: f.parentSha, path: nativePath });
        assert.isTrue(yield* f.fs.exists(nativePath));
        yield* f.driver.removeWorktree({ cwd: f.cwd, path: nativePath });
        yield* f.leases.commitThread({
          path: f.slot,
          threadId: "parent-thread",
          deleted: true,
          sequence: 3,
        });
        const restarted = yield* makeGitVcsDriverCore();
        assert.isTrue(
          Result.isFailure(
            yield* restarted.removeWorktree({ cwd: f.cwd, path: f.slot }).pipe(Effect.result),
          ),
        );
        yield* f.leases.commitThread({
          path: null,
          threadId: "child-thread",
          deleted: false,
          sequence: 4,
        });
        assert.isTrue(
          Result.isFailure(
            yield* restarted.removeWorktree({ cwd: f.cwd, path: f.slot }).pipe(Effect.result),
          ),
        );
        yield* releaseProvider;
        assert.isTrue(
          Result.isFailure(
            yield* restarted.removeWorktree({ cwd: f.cwd, path: f.slot }).pipe(Effect.result),
          ),
        );
        yield* releaseTerminal;
        yield* restarted.removeWorktree({ cwd: f.cwd, path: f.slot, force: true });
        // Reacquire the same physical path, then prove old identities cannot be rebound or resumed.
        yield* f.settings.updateSettings({
          treehouseWorktreesEnabled: true,
          treehouseBinaryPath: f.binary,
        });
        yield* f.driver.createWorktree({ cwd: f.cwd, refName: f.parentSha, path: null });
        assert.isTrue(
          Result.isFailure(
            yield* f.leases
              .assertThread({ path: f.slot, threadId: "parent-thread", requireBinding: false })
              .pipe(Effect.result),
          ),
        );
        assert.isTrue(
          Result.isFailure(
            yield* f.leases
              .bindThread({ path: f.slot, threadId: "child-thread", sequence: 5 })
              .pipe(Effect.result),
          ),
        );
        yield* f.leases.bindThread({ path: f.slot, threadId: "new-thread", sequence: 6 });
        yield* f.leases.assertThread({ path: f.slot, threadId: "new-thread" });
      }).pipe(provide),
  );
  it.live("refuses unknown leases, missing pooled paths, and raw linked-worktree cleanup", () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      yield* f.driver.createWorktree({ cwd: f.cwd, refName: f.parentSha, path: null });
      const statePath = f.path.join(f.root, "pool", "treehouse-state.json");
      const snapshot = yield* f.fs.readFileString(statePath);
      yield* f.fs.writeFileString(statePath, snapshot.replace('"version":4', '"version":5'));
      assert.isTrue(Result.isFailure(yield* f.leases.read(f.slot).pipe(Effect.result)));
      yield* f.fs.writeFileString(statePath, snapshot);
      const raw = yield* removeLinkedWorktree({
        operation: "test",
        cwd: f.cwd,
        worktreePath: f.slot,
        force: true,
        execute: f.driver.execute,
        run: (args) => f.git({ args: [...args] }),
      }).pipe(Effect.result);
      assert.isTrue(Result.isFailure(raw));
      assert.isTrue(yield* f.fs.exists(f.slot));
      yield* f.fs.remove(f.path.join(f.registry, "leases"), { recursive: true });
      assert.isTrue(
        Result.isFailure(
          yield* f.driver.removeWorktree({ cwd: f.cwd, path: f.slot }).pipe(Effect.result),
        ),
      );
      yield* f.fs.remove(f.slot, { recursive: true });
      yield* f.fs.remove(statePath);
      assert.isTrue(
        Result.isFailure(
          yield* f.driver
            .createWorktree({ cwd: f.cwd, refName: "main", path: f.slot })
            .pipe(Effect.result),
        ),
      );
      assert.isFalse(yield* f.fs.exists(f.slot));
    }).pipe(provide),
  );
  it.live("allows manually leased roots but refuses T3-owned slots from another home", () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      yield* f.driver.createWorktree({ cwd: f.cwd, refName: f.parentSha, path: null });
      const foreignHome = yield* TreehouseLeases.TreehouseLeaseStore.pipe(
        Effect.provide(TreehouseLeases.layer),
        Effect.provideService(ServerConfig.ServerConfig, {
          stateDir: f.path.join(f.root, "other-home"),
        } as ServerConfig.ServerConfig["Service"]),
      );
      assert.isTrue(
        Result.isFailure(
          yield* foreignHome
            .assertThread({ path: f.slot, threadId: "manual-thread", requireBinding: false })
            .pipe(Effect.result),
        ),
      );
      const owned = yield* f.leases.read(f.slot);
      if (owned === null) return yield* Effect.die("missing fixture lease");
      const statePath = f.path.join(f.root, "pool", "treehouse-state.json");
      const snapshot = yield* f.fs.readFileString(statePath);
      yield* f.fs.writeFileString(
        statePath,
        snapshot.replace(owned.record.lease.lease_holder, "operator:manual"),
      );
      yield* foreignHome.assertThread({
        path: f.slot,
        threadId: "manual-thread",
        requireBinding: false,
      });
      assert.isNull(
        yield* foreignHome.operationGeneration({ path: f.slot, threadId: "manual-thread" }),
      );
    }).pipe(provide),
  );

  it.live("binds threads to externally leased slots without claiming them", () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      yield* f.driver.createWorktree({ cwd: f.cwd, refName: f.parentSha, path: null });
      const owned = yield* f.leases.read(f.slot);
      if (owned === null) return yield* Effect.die("missing fixture lease");
      const statePath = f.path.join(f.root, "pool", "treehouse-state.json");
      const snapshot = yield* f.fs.readFileString(statePath);
      yield* f.fs.writeFileString(
        statePath,
        snapshot.replace(owned.record.lease.lease_holder, "agent-review"),
      );
      const otherHome = yield* TreehouseLeases.TreehouseLeaseStore.pipe(
        Effect.provide(TreehouseLeases.layer),
        Effect.provideService(ServerConfig.ServerConfig, {
          stateDir: f.path.join(f.root, "other-home"),
        } as ServerConfig.ServerConfig["Service"]),
      );
      yield* otherHome.bindThread({ path: f.slot, threadId: "review-thread", sequence: 1 });
      yield* otherHome.assertThread({ path: f.slot, threadId: "review-thread" });
      const treehouse = yield* TreehouseWorktrees.TreehouseWorktrees.pipe(
        Effect.provide(TreehouseWorktrees.layer),
      );
      assert.isFalse(yield* treehouse.remove({ cwd: f.cwd, path: f.slot }));
      assert.include(yield* f.fs.readFileString(statePath), "agent-review");
    }).pipe(provide),
  );

  it.live("preserves metadata edits without claiming a returned or recycled slot", () =>
    Effect.gen(function* () {
      const family = yield* returnedFamily();
      const { f, sink, projections, thread, now } = family;
      for (const recycled of [false, true]) {
        if (recycled) {
          yield* f.driver.createWorktree({
            cwd: f.cwd,
            refName: f.parentSha,
            newRefName: "unrelated-occupant",
            path: null,
          });
        }
        const current = yield* projections.getThread(thread.id);
        yield* sink.write({
          events: [
            {
              id: EventId.make(`metadata-${recycled}`),
              type: "thread.metadata-updated",
              threadId: thread.id,
              occurredAt: now,
              payload: { ...current, title: `Edited ${recycled}`, lastVisitedAt: now },
            },
          ],
        });
        assert.strictEqual((yield* projections.getThread(thread.id)).title, `Edited ${recycled}`);
        assert.isTrue(
          Result.isFailure(
            yield* f.leases.assertThread({ path: f.slot, threadId: thread.id }).pipe(Effect.result),
          ),
        );
      }
      assert.strictEqual(
        yield* f.git({ cwd: f.slot, args: ["branch", "--show-current"] }),
        "unrelated-occupant",
      );
    }).pipe(Effect.scoped, Effect.provide(historyLayer)),
  );

  it.live(
    "keeps availability passive and shares one activation across concurrent explicit actions",
    () =>
      Effect.gen(function* () {
        const family = yield* returnedFamily();
        const { f, thread, projections, now } = family;
        const access = yield* WorkspaceAccess.WorkspaceAccess.pipe(
          Effect.provide(WorkspaceAccess.layer),
        );
        const projectLookup = Layer.mock(ProjectService.ProjectService)({
          getById: () =>
            Effect.succeedSome({
              id: thread.projectId,
              title: "Test",
              workspaceRoot: f.cwd,
              defaultModelSelection: null,
              scripts: [],
              createdAt: DateTime.formatIso(now),
              updatedAt: DateTime.formatIso(now),
              deletedAt: null,
            }),
        });
        const before = yield* f.fs.readFileString(f.path.join(f.root, "calls"));
        expect(
          yield* access.availability({ threadId: thread.id }).pipe(Effect.provide(projectLookup)),
        ).toEqual({ state: "released", cwd: null });
        expect(yield* f.fs.readFileString(f.path.join(f.root, "calls"))).toBe(before);
        yield* f.driver.createWorktree({
          cwd: f.cwd,
          refName: f.parentSha,
          newRefName: "unrelated-occupant",
          path: null,
        });
        yield* f.fs.writeFileString(f.path.join(f.root, "next-slot"), "");
        const [first, second] = yield* Effect.all(
          [access.activate({ threadId: thread.id }), access.activate({ threadId: thread.id })],
          { concurrency: "unbounded" },
        ).pipe(Effect.provide(Layer.merge(projectLookup, IdAllocator.layer)));
        expect(first).toEqual(second);
        expect(first.cwd).not.toBe(f.slot);
        expect((yield* projections.getThread(thread.id)).worktreePath).toBe(first.cwd);
        const calls = yield* f.fs.readFileString(f.path.join(f.root, "calls"));
        expect(calls.split("\n").filter((line) => line.startsWith("get "))).toHaveLength(3);
        const owned = yield* f.leases.read(first.cwd);
        expect(yield* f.leases.assertReturnable(owned!).pipe(Effect.flip)).toMatchObject({
          reason: "in-use",
        });
      }).pipe(Effect.scoped, Effect.provide(historyLayer)),
  );

  it.live(
    "relocates checkpoint capture and history restore without changing unrelated scopes",
    () =>
      Effect.gen(function* () {
        const family = yield* returnedFamily();
        const { f, sink, projections, thread, now } = family;
        const runId = RunId.make("resumed-checkpoint-run");
        const scopes = ["historical", "current", "other-workspace"].map((kind) => ({
          id: CheckpointScopeId.make(`${kind}-scope`),
          threadId: thread.id,
          runId: kind === "current" ? runId : RunId.make(`${kind}-run`),
          nodeId: NodeId.make(`${kind}-node`),
          parentScopeId: null,
          providerThreadId: null,
          kind: "root_run" as const,
          ordinalWithinParent: 0,
          advancesAppRunCount: true,
          cwd: kind === "other-workspace" ? f.path.join(f.root, "other-workspace") : f.slot,
          createdAt: now,
        }));
        yield* sink.write({
          events: scopes.map((scope) => ({
            id: EventId.make(`create-${scope.id}`),
            type: "checkpoint-scope.created" as const,
            threadId: thread.id,
            occurredAt: now,
            payload: scope,
          })),
        });
        const { path: replacement, scope: preparation } = yield* resumeFamily(family);
        assert.isNotNull(replacement);
        yield* sink.rebindWorktree({
          threadId: thread.id,
          familyThreadIds: family.threadIds,
          previousPath: f.slot,
          path: replacement!,
          branch: thread.branch!,
          eventId: EventId.make("rebind-checkpoints"),
          occurredAt: now,
        });
        yield* Scope.close(preparation, Exit.void);
        const records = yield* projections.getThreadRecords(thread.id, ["checkpointScopes"]);
        const current = records.checkpointScopes.find((scope) => scope.runId === runId)!;
        const historical = records.checkpointScopes.find((scope) => scope.id === scopes[0]!.id)!;
        assert.strictEqual(
          records.checkpointScopes.find((scope) => scope.id === scopes[2]!.id)?.cwd,
          scopes[2]!.cwd,
        );
        const captured: string[] = [];
        const restored: string[] = [];
        const checkpoints = yield* CheckpointService.CheckpointServiceV2.pipe(
          Effect.provide(
            CheckpointService.layer.pipe(
              Layer.provide(
                Layer.mergeAll(
                  IdAllocator.layer,
                  Layer.mock(CheckpointStore.CheckpointStore)({
                    isGitRepository: () => Effect.succeed(true),
                    hasCheckpointRef: () => Effect.succeed(false),
                    captureCheckpoint: (input) =>
                      Effect.sync(() => {
                        captured.push(input.cwd);
                      }),
                    restoreCheckpoint: (input) =>
                      Effect.sync(() => {
                        restored.push(input.cwd);
                        return true;
                      }),
                  }),
                ),
              ),
              Layer.provide(NodeCrypto.layer),
            ),
          ),
        );
        yield* checkpoints.captureBaseline({ scope: current, ordinalWithinScope: 0 });
        yield* checkpoints.capture({
          scope: current,
          ordinalWithinScope: 1,
          runId,
          nodeId: current.nodeId,
          appRunOrdinal: 1,
          capturedAt: now,
        });
        const historicalCheckpoint = yield* checkpoints.capture({
          scope: historical,
          ordinalWithinScope: 1,
          runId: historical.runId,
          nodeId: historical.nodeId,
          appRunOrdinal: 0,
          capturedAt: now,
        });
        yield* checkpoints.restore({ scope: historical, checkpoint: historicalCheckpoint });
        assert.deepStrictEqual(captured, [replacement, replacement, replacement]);
        assert.deepStrictEqual(restored, [replacement]);
      }).pipe(Effect.scoped, Effect.provide(historyLayer)),
  );

  it.live("retains a deleted owner's association when its sharing siblings relocate", () =>
    Effect.gen(function* () {
      const family = yield* returnedFamily();
      const { f, sink, projections, thread, now, owner, children, threadIds } = family;
      const { path: replacement, scope: preparation } = yield* resumeFamily(family);
      assert.isNotNull(replacement);
      yield* sink.rebindWorktree({
        threadId: thread.id,
        familyThreadIds: threadIds,
        previousPath: f.slot,
        path: replacement!,
        branch: thread.branch!,
        eventId: EventId.make("rebind-family"),
        occurredAt: now,
      });
      yield* Scope.close(preparation, Exit.void);
      const tombstone = yield* projections.getThread(owner.id);
      assert.strictEqual(tombstone.worktreePath, replacement);
      assert.deepStrictEqual(tombstone.deletedAt, now);
      for (const child of children) {
        const rebound = yield* projections.getThread(child.id);
        assert.strictEqual(rebound.worktreePath, replacement);
        assert.deepStrictEqual(rebound.lineage, child.lineage);
        assert.deepStrictEqual(rebound.archivedAt, child.archivedAt);
        yield* f.leases.assertThread({ path: replacement!, threadId: child.id });
      }
      for (const child of children) {
        const current = yield* projections.getThread(child.id);
        yield* sink.write({
          events: [
            {
              id: EventId.make(`delete-${child.id}`),
              type: "thread.deleted",
              threadId: child.id,
              occurredAt: now,
              payload: { ...current, deletedAt: now },
            },
          ],
        });
      }
      yield* f.driver.removeWorktree({ cwd: f.cwd, path: replacement! });
    }).pipe(Effect.scoped, Effect.provide(historyLayer)),
  );

  it.live(
    "keeps conversation history through idle return, durable rebind, and later deletion",
    () =>
      Effect.gen(function* () {
        const f = yield* fixture();
        yield* f.driver.createWorktree({ cwd: f.cwd, refName: f.parentSha, path: null });
        const sink = yield* EventSink.EventSinkV2;
        const now = yield* DateTime.now;
        yield* f.git({ cwd: f.slot, args: ["switch", "-c", "conversation-branch"] });
        const threadId = ThreadId.make("treehouse-thread");
        const thread: OrchestrationV2AppThread = {
          id: threadId,
          projectId: ProjectId.make("treehouse-project"),
          title: "Treehouse",
          createdBy: "user",
          creationSource: "web",
          providerInstanceId: ProviderInstanceId.make("codex"),
          modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "test" },
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: "conversation-branch",
          worktreePath: f.slot,
          branchPullRequest: null,
          activeOrderKey: null,
          activeProviderThreadId: null,
          lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
          forkedFrom: null,
          createdAt: now,
          updatedAt: now,
          archivedAt: null,
          settledOverride: null,
          settledAt: null,
          lastVisitedAt: null,
          deletedAt: null,
        };
        yield* sink.write({
          events: [
            {
              id: EventId.make("treehouse-created"),
              type: "thread.created",
              threadId,
              occurredAt: now,
              payload: thread,
            },
            {
              id: EventId.make("history-message"),
              type: "message.updated",
              threadId,
              occurredAt: now,
              payload: {
                id: MessageId.make("history-message"),
                threadId,
                runId: null,
                nodeId: null,
                role: "user",
                text: "Keep this conversation",
                attachments: [],
                streaming: false,
                createdBy: "user",
                creationSource: "web",
                createdAt: now,
                updatedAt: now,
              },
            },
          ],
        });
        yield* f.leases.assertThread({ path: f.slot, threadId });
        assert.isTrue(
          Result.isFailure(
            yield* f.driver.removeWorktree({ cwd: f.cwd, path: f.slot }).pipe(Effect.result),
          ),
        );
        const treehouse = yield* TreehouseWorktrees.TreehouseWorktrees.pipe(
          Effect.provide(TreehouseWorktrees.layer),
        );
        assert.isNull(
          yield* treehouse.returnIdle({
            cwd: f.cwd,
            path: f.slot,
            branch: "conversation-branch",
            headSha: f.parentSha,
            threadIds: [threadId],
            recheck: Effect.succeed(null),
          }),
        );
        const scope = yield* Scope.make();
        yield* Effect.addFinalizer((exit) => Scope.close(scope, exit));
        const resumed = yield* treehouse.resume({
          cwd: f.cwd,
          path: f.slot,
          branch: "conversation-branch",
          threadId,
          preparationScope: scope,
        });
        assert.strictEqual(resumed, f.slot);
        yield* sink.rebindWorktree({
          threadId,
          previousPath: f.slot,
          path: resumed!,
          branch: "conversation-branch",
          eventId: EventId.make("treehouse-rebound"),
          occurredAt: now,
        });
        yield* Scope.close(scope, Exit.void);
        const projections = yield* ProjectionStore.ProjectionStoreV2;
        const retained = yield* projections.getThreadRecords(threadId, ["messages"]);
        assert.strictEqual(retained.thread.deletedAt, null);
        assert.strictEqual(retained.thread.branch, "conversation-branch");
        assert.strictEqual(retained.messages[0]?.text, "Keep this conversation");
        yield* f.leases.assertThread({ path: resumed!, threadId });
        yield* sink.write({
          events: [
            {
              id: EventId.make("treehouse-deleted"),
              type: "thread.deleted",
              threadId,
              occurredAt: now,
              payload: { ...thread, deletedAt: now },
            },
          ],
        });
        yield* f.driver.removeWorktree({ cwd: f.cwd, path: f.slot });
      }).pipe(
        Effect.provide(
          EventSink.layer.pipe(
            Layer.provideMerge(
              Layer.merge(EventStore.layer, ProjectionStore.layer).pipe(
                Layer.provideMerge(Sqlite.layerMemory),
              ),
            ),
            Layer.provideMerge(testLayer),
          ),
        ),
      ),
  );

  it.live("rejects an inherited fork of a recycled lease without blocking a fresh thread", () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      yield* f.driver.createWorktree({ cwd: f.cwd, refName: f.parentSha, path: null });
      const sink = yield* EventSink.EventSinkV2;
      const fork = yield* ThreadForkService.ThreadForkServiceV2;
      const now = yield* DateTime.now;
      const parentId = ThreadId.make("fork-parent");
      const parent: OrchestrationV2AppThread = {
        id: parentId,
        projectId: ProjectId.make("fork-project"),
        title: "parent",
        createdBy: "user",
        creationSource: "web",
        providerInstanceId: ProviderInstanceId.make("codex"),
        modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "test" },
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: "old-branch",
        worktreePath: f.slot,
        activeProviderThreadId: null,
        lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: parentId },
        forkedFrom: null,
        createdAt: now,
        updatedAt: now,
        archivedAt: null,
        settledOverride: null,
        settledAt: null,
        lastVisitedAt: null,
        deletedAt: null,
      };
      const writeThread = (thread: OrchestrationV2AppThread) =>
        sink.write({
          events: [
            {
              id: EventId.make(`create-${thread.id}`),
              type: "thread.created",
              threadId: thread.id,
              occurredAt: now,
              payload: thread,
            },
          ],
        });
      yield* writeThread(parent);
      const runId = RunId.make("fork-run");
      const plan = (targetThreadId: ThreadId) =>
        fork.plan({
          sourceProjection: { thread: parent },
          sourceRun: {
            id: runId,
            threadId: parentId,
            ordinal: 1,
            providerInstanceId: parent.providerInstanceId,
            modelSelection: parent.modelSelection,
            providerThreadId: ProviderThreadId.make("fork-provider"),
            userMessageId: MessageId.make("fork-message"),
            rootNodeId: null,
            activeAttemptId: null,
            status: "completed",
            queuePosition: null,
            requestedAt: now,
            startedAt: now,
            completedAt: now,
            checkpointId: null,
            contextHandoffId: null,
          },
          sourceProviderThread: undefined,
          canonicalSourcePoint: { threadId: parentId, runId },
          transferId: ContextTransferId.make(`transfer-${targetThreadId}`),
          targetThreadId,
          createdBy: "user",
          creationSource: "web",
          createdAt: now,
        });
      const valid = yield* plan(ThreadId.make("valid-fork"));
      yield* writeThread(valid.targetThread);
      yield* f.leases.assertThread({ path: f.slot, threadId: valid.targetThread.id });
      const owned = yield* f.leases.read(f.slot);
      assert.isNotNull(owned);
      const recycled = { ...owned!.record.lease, lease_id: "b".repeat(32) };
      const pool = f.path.join(f.root, "pool");
      yield* f.fs.writeFileString(
        f.path.join(pool, "treehouse-state.json"),
        JSON.stringify({ version: 4, worktrees: [{ ...recycled, leased: true }] }),
      );
      yield* f.leases.register({ lease: recycled, binary: owned!.record.binary, pool });
      const stale = yield* plan(ThreadId.make("stale-fork"));
      const result = yield* writeThread(stale.targetThread).pipe(Effect.result);
      assert.isTrue(Result.isFailure(result), "fork must not adopt the recycled lease");
      assert.isTrue(
        Result.isFailure(
          yield* f.leases
            .assertThread({ path: f.slot, threadId: stale.targetThread.id })
            .pipe(Effect.result),
        ),
      );
      const fresh = {
        ...parent,
        id: ThreadId.make("fresh-thread"),
        createdBy: "agent" as const,
        branch: "new-branch",
      };
      yield* writeThread(fresh);
      yield* f.leases.assertThread({ path: f.slot, threadId: fresh.id });
    }).pipe(
      Effect.provide(
        EventSink.layer.pipe(
          Layer.provide(
            Layer.merge(EventStore.layer, ProjectionStore.layer).pipe(
              Layer.provideMerge(Sqlite.layerMemory),
            ),
          ),
          Layer.provideMerge(ThreadForkService.layer),
          Layer.provideMerge(testLayer),
        ),
      ),
    ),
  );

  it.effect(
    "queues concurrent lifecycle requests instead of treating the local lock as foreign",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const config = yield* ServerConfig.ServerConfig;
        const registry = path.join(config.stateDir, "treehouse-leases");
        yield* fs.makeDirectory(registry, { recursive: true });
        const canonical = yield* fs.realPath(registry);
        const lock = path.join(registry, "lifecycle.lock");
        let held = false;
        const leases = yield* TreehouseLeases.TreehouseLeaseStore.pipe(
          Effect.provide(TreehouseLeases.layer),
          Effect.provideService(FileSystem.FileSystem, {
            ...fs,
            realPath: (target) =>
              target === registry ? Effect.succeed(canonical) : fs.realPath(target),
            makeDirectory: (target, options) =>
              target === registry
                ? Effect.void
                : target !== lock
                  ? fs.makeDirectory(target, options)
                  : held
                    ? Effect.fail(
                        PlatformError.systemError({
                          module: "FileSystem",
                          method: "makeDirectory",
                          _tag: "AlreadyExists",
                          pathOrDescriptor: target,
                        }),
                      )
                    : Effect.sync(() => {
                        held = true;
                      }),
            remove: (target, options) =>
              target === lock
                ? Effect.sync(() => {
                    held = false;
                  })
                : fs.remove(target, options),
          }),
        );
        const entered = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const first = yield* leases
          .withLock({
            path: registry,
            effect: Deferred.succeed(entered, undefined).pipe(
              Effect.andThen(Deferred.await(release)),
            ),
          })
          .pipe(Effect.forkChild({ startImmediately: true }));
        yield* Deferred.await(entered);
        const second = yield* leases
          .withLock({ path: registry, effect: Effect.succeed("second") })
          .pipe(Effect.forkChild({ startImmediately: true }));
        yield* Deferred.succeed(release, undefined);
        yield* Fiber.join(first);
        assert.strictEqual(yield* Fiber.join(second), "second");
      }).pipe(provide),
  );
});

import { assert, describe, it } from "@effect/vitest";
import {
  CommandId,
  DEFAULT_SERVER_SETTINGS,
  EnvironmentId,
  EventId,
  NodeId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Result from "effect/Result";
import { TestClock } from "effect/testing";
import * as StorageCleanup from "../storageCleanup.ts";
import * as GitVcsDriver from "../vcs/GitVcsDriver.ts";
import * as GitManager from "../git/GitManager.ts";
import * as TerminalManager from "../terminal/Manager.ts";
import { ServerActivation } from "../serverActivation.ts";
import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as ProjectStore from "../orchestration-v2/ProjectStore.ts";
import * as WorktreeBranchReconciliation from "../workspace/WorktreeBranchReconciliation.ts";
import * as ProviderAdapterRegistry from "../orchestration-v2/ProviderAdapterRegistry.ts";
import * as ProviderReplayHarness from "../orchestration-v2/testkit/ProviderReplayHarness.ts";
import * as ThreadManagement from "../orchestration-v2/ThreadManagementService.ts";
import * as Sqlite from "../persistence/Sqlite.ts";
import * as GitWorkflow from "../git/GitWorkflowService.ts";
import * as Project from "../project/ProjectService.ts";
import * as Setup from "../project/ProjectSetupScriptRunner.ts";
import * as Broadcaster from "../vcs/VcsStatusBroadcaster.ts";
import { fixture, testLayer } from "../vcs/TreehouseWorktrees.testkit.ts";
import * as WorktreeMcpService from "./WorktreeMcpService.ts";
import type { McpInvocationScope } from "./McpInvocationContext.ts";

const database = Sqlite.layerMemory;
const runtime = Layer.mergeAll(
  database,
  ProjectionStore.layer.pipe(Layer.provide(database)),
  ProjectStore.layer.pipe(Layer.provide(database)),
  ProviderReplayHarness.layerWithRegistry(
    { name: "worktree-reconciliation" },
    ProviderAdapterRegistry.layerFromAdapters([]),
    { databaseLayer: database, runEffectWorker: false },
  ),
).pipe(Layer.provideMerge(testLayer));

const prepare = Effect.fn("test.prepareWorktreeReconciliation")(function* ({
  beforeDispatch,
  branchDrift = true,
}: {
  branchDrift?: boolean;
  beforeDispatch?: (command: {
    readonly type: string;
    readonly threadId?: ThreadId | undefined;
  }) => Effect.Effect<void>;
} = {}) {
  const f = yield* fixture();
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const events = yield* EventSink.EventSinkV2;
  const threadId = ThreadId.make("reconciliation-owner");
  const childId = ThreadId.make("reconciliation-child");
  const projectId = ProjectId.make("reconciliation-project");
  const projects = yield* ProjectStore.ProjectStoreV2;
  yield* projects.apply({
    sequence: 1,
    eventId: EventId.make("reconciliation-project-created"),
    aggregateKind: "project",
    aggregateId: projectId,
    occurredAt: "2026-01-01T00:00:00.000Z",
    commandId: null,
    causationEventId: null,
    correlationId: null,
    metadata: {},
    type: "project.created",
    payload: {
      projectId,
      title: "Fixture",
      workspaceRoot: f.cwd,
      defaultModelSelection: null,
      scripts: [],
      createdAt: "2026-01-01T00:00:00.000Z",
      updatedAt: "2026-01-01T00:00:00.000Z",
    },
  });
  yield* f.driver
    .createWorktree({ cwd: f.cwd, refName: f.parentSha, newRefName: "temporary", path: null })
    .pipe(Effect.scoped);
  yield* orchestrator.dispatch({
    type: "thread.create",
    commandId: CommandId.make("create-reconciliation-owner"),
    threadId,
    projectId,
    title: "Finished review",
    modelSelection: { instanceId: ProviderInstanceId.make("fixture"), model: "fixture" },
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: "temporary",
    worktreePath: f.slot,
    createdBy: "user",
    creationSource: "web",
  });
  const owner = yield* projections.getThread(threadId);
  yield* events.write({
    events: [
      {
        id: EventId.make("create-reconciliation-child"),
        type: "thread.created",
        threadId: childId,
        occurredAt: yield* DateTime.now,
        payload: {
          ...owner,
          id: childId,
          lineage: {
            ...owner.lineage,
            rootThreadId: threadId,
            parentThreadId: threadId,
            relationshipToParent: "subagent",
          },
        },
      },
    ],
  });
  for (const id of [threadId, childId])
    yield* orchestrator.dispatch({
      type: "thread.settle",
      commandId: CommandId.make(`settle-${id}`),
      threadId: id,
    });
  if (branchDrift) yield* f.git({ cwd: f.slot, args: ["switch", "-c", "review"] });
  yield* f.fs.writeFileString(f.path.join(f.cwd, ".git/info/exclude"), "retained.fixture\n");
  yield* f.fs.writeFileString(
    f.path.join(f.slot, "retained.fixture"),
    "synthetic retained artifact\n",
  );
  const owned = yield* f.leases.read(f.slot);
  if (owned === null) return assert.fail("Fixture must have an owned lease");
  const scope: McpInvocationScope = {
    environmentId: EnvironmentId.make("fixture-environment"),
    requestNamespace: "fixture-request",
    thread: {
      threadId,
      providerSessionId: "fixture-session",
      providerInstanceId: ProviderInstanceId.make("fixture"),
    },
    client: undefined,
    capabilities: new Set(["worktree", "orchestration"]),
    issuedAt: 1,
  };
  const guardedEngine: Orchestrator.OrchestratorV2["Service"] = {
    ...orchestrator,
    dispatch: (command) =>
      Effect.gen(function* () {
        if (beforeDispatch) yield* beforeDispatch(command);
        return yield* orchestrator.dispatch(command);
      }),
  };
  const reconciliation = yield* WorktreeBranchReconciliation.WorktreeBranchReconciliation.pipe(
    Effect.provide(WorktreeBranchReconciliation.layer),
    Effect.provideService(Orchestrator.OrchestratorV2, guardedEngine),
  );
  const service = yield* WorktreeMcpService.WorktreeMcpService.pipe(
    Effect.provide(
      WorktreeMcpService.layer.pipe(
        Layer.provide(
          Layer.mergeAll(
            Layer.succeed(
              WorktreeBranchReconciliation.WorktreeBranchReconciliation,
              reconciliation,
            ),
            Layer.mock(ThreadManagement.ThreadManagementService)({
              dispatch: guardedEngine.dispatch,
              getShellSnapshot: orchestrator.getShellSnapshot,
            }),
            Layer.mock(Project.ProjectService)({
              getById: () =>
                Effect.succeedSome({
                  id: projectId,
                  title: "Fixture",
                  workspaceRoot: f.cwd,
                  scripts: [],
                  defaultModelSelection: null,
                  createdAt: "2026-01-01T00:00:00Z",
                  updatedAt: "2026-01-01T00:00:00Z",
                  deletedAt: null,
                }),
            }),
            Layer.mock(GitWorkflow.GitWorkflowService)({}),
            Layer.mock(Setup.ProjectSetupScriptRunner)({}),
            Layer.mock(Broadcaster.VcsStatusBroadcaster)({}),
          ),
        ),
      ),
    ),
  );
  return {
    ...f,
    service,
    reconciliation,
    scope,
    orchestrator,
    projections,
    threadId,
    childId,
    input: {
      threadId,
      expected: {
        worktreePath: f.slot,
        recordedBranch: "temporary",
        branch: branchDrift ? "review" : "temporary",
        headSha: f.parentSha,
        leaseId: owned.record.lease.lease_id,
        threadIds: [threadId, childId],
      },
    },
  };
});

const retirementCleanup = Effect.fn("test.retirementCleanup")(function* () {
  const context = yield* Layer.build(
    StorageCleanup.layer.pipe(
      Layer.provide(GitVcsDriver.layer),
      Layer.provide(
        Layer.mergeAll(
          Layer.mock(GitManager.GitManager)({ invalidateStatus: () => Effect.void }),
          Layer.mock(TerminalManager.TerminalManager)({
            subscribeMetadata: () => Effect.succeed(() => {}),
          }),
        ),
      ),
    ),
  ).pipe(Effect.provideService(ServerActivation, Effect.never));
  return Context.get(context, StorageCleanup.StorageCleanup);
});

describe("worktree branch reconciliation", () => {
  it.effect.each([
    "completed",
    "immediate",
    "failed",
    "cancelled",
    "unfinished",
    "missing-result",
    "shared",
  ] as const)(
    "removes finished isolated checkout data after the configured deadline (%s)",
    (state) =>
      Effect.gen(function* () {
        const f = yield* prepare({ branchDrift: false });
        const events = yield* EventSink.EventSinkV2;
        const now = yield* DateTime.now;
        const childId = state === "shared" ? f.childId : ThreadId.make("isolated-finished-child");
        let childPath = f.slot;
        if (state !== "shared") {
          yield* f.fs.writeFileString(f.path.join(f.root, "next-slot"), "");
          const created = yield* f.driver
            .createWorktree({
              cwd: f.cwd,
              refName: f.parentSha,
              newRefName: "finished-child",
              path: null,
            })
            .pipe(Effect.scoped);
          childPath = created.worktree.path;
          const owner = yield* f.projections.getThread(f.threadId);
          yield* events.write({
            events: [
              {
                id: EventId.make("isolated-child-created"),
                type: "thread.created",
                threadId: childId,
                occurredAt: now,
                payload: {
                  ...owner,
                  id: childId,
                  branch: "finished-child",
                  worktreePath: childPath,
                  settledAt: null,
                  settledOverride: null,
                  lineage: {
                    rootThreadId: f.threadId,
                    parentThreadId: f.threadId,
                    relationshipToParent: "subagent",
                  },
                },
              },
            ],
          });
        }
        yield* events.write({
          events: [
            {
              id: EventId.make("task-result-saved"),
              type: "subagent.updated",
              threadId: f.threadId,
              occurredAt: now,
              payload: {
                id: NodeId.make("finished-task"),
                threadId: f.threadId,
                runId: null,
                parentNodeId: NodeId.make("parent-node"),
                origin: "app_owned",
                createdBy: "agent",
                driver: ProviderDriverKind.make("codex"),
                providerInstanceId: ProviderInstanceId.make("fixture"),
                providerThreadId: null,
                childThreadId: childId,
                nativeTaskRef: null,
                prompt: "Synthetic isolated task",
                title: "Finished task",
                model: "fixture",
                status:
                  state === "unfinished"
                    ? "running"
                    : state === "failed" || state === "cancelled"
                      ? state
                      : "completed",
                result:
                  state === "missing-result" || state === "unfinished" ? null : "Saved answer",
                startedAt: now,
                completedAt: state === "unfinished" ? null : now,
                updatedAt: now,
              },
            },
          ],
        });
        yield* f.settings.updateSettings({
          storageCleanup: {
            ...DEFAULT_SERVER_SETTINGS.storageCleanup,
            worktreeAfterDays: null,
            worktreeSettledAfterDays: state === "immediate" ? 0 : 1,
          },
        });
        yield* f.orchestrator.dispatch({
          type: "thread.unsettle",
          commandId: CommandId.make("parent-continues"),
          threadId: f.threadId,
          reason: "user",
        });
        const cleanup = yield* retirementCleanup();
        if (state === "immediate") {
          assert.equal((yield* cleanup.runNow).counts.removed, 1);
          assert.isFalse(yield* f.fs.exists(childPath));
          return;
        }
        yield* TestClock.adjust(59_000);
        assert.equal((yield* cleanup.runNow).returnedCount, 0);
        yield* TestClock.adjust(86_400_000 - 59_001);
        assert.equal((yield* cleanup.runNow).counts.removed, 0);
        yield* TestClock.adjust(1);
        const report = yield* cleanup.runNow;
        const removed = state !== "unfinished" && state !== "shared";
        assert.equal(report.counts.removed, removed ? 1 : 0);
        assert.equal(yield* f.fs.exists(childPath), !removed);
        if (removed) {
          assert.equal(
            report.entries.find((entry) => entry.outcome === "removed")?.path,
            childPath,
          );
          assert.isNull((yield* f.projections.getThread(childId)).settledAt);
          assert.equal(
            yield* f.git({ args: ["rev-parse", "refs/heads/finished-child"] }),
            f.parentSha,
          );
          assert.equal((yield* f.projections.getThread(f.threadId)).worktreePath, f.slot);
          assert.isNotNull(yield* f.leases.read(f.slot));
          const retained = yield* f.projections.getThreadRecords(f.threadId, ["subagents"]);
          assert.equal(
            retained.subagents.find((task) => task.childThreadId === childId)?.result,
            state === "missing-result" ? null : "Saved answer",
          );
        }
      }).pipe(Effect.provide(runtime)),
  );

  it.effect.each(["settled", "idle", "changed-project-root", "deleted-family-member"] as const)(
    "removes an expired owned %s checkout while retaining its history and Git refs",
    (state) =>
      Effect.gen(function* () {
        const f = yield* prepare();
        const settled = state === "settled" || state === "deleted-family-member";
        if (state === "deleted-family-member") {
          const child = yield* f.projections.getThread(f.childId);
          const deletedAt = yield* DateTime.now;
          yield* (yield* EventSink.EventSinkV2).write({
            events: [
              {
                id: EventId.make("finished-sharing-child-deleted"),
                type: "thread.deleted",
                threadId: f.childId,
                occurredAt: deletedAt,
                payload: { ...child, deletedAt },
              },
            ],
          });
        }
        if (!settled) {
          for (const threadId of [f.threadId, f.childId])
            yield* f.orchestrator.dispatch({
              type: "thread.unsettle",
              commandId: CommandId.make(`keep-open-${threadId}`),
              threadId,
              reason: "user",
            });
        }
        let changedRoot: string | undefined;
        if (state === "changed-project-root") {
          changedRoot = yield* f.fs.makeTempDirectoryScoped();
          yield* f.git({ cwd: changedRoot, args: ["init", "-b", "main"] });
          yield* f.fs.writeFileString(
            f.path.join(changedRoot, "current-project-proof"),
            "keep current project",
          );
          const projectId = (yield* f.projections.getThread(f.threadId)).projectId;
          yield* (yield* ProjectStore.ProjectStoreV2).apply({
            sequence: 2,
            eventId: EventId.make("project-root-moves"),
            aggregateKind: "project",
            aggregateId: projectId,
            occurredAt: "2026-01-02T00:00:00.000Z",
            commandId: null,
            causationEventId: null,
            correlationId: null,
            metadata: {},
            type: "project.meta-updated",
            payload: {
              projectId,
              workspaceRoot: changedRoot,
              updatedAt: "2026-01-02T00:00:00.000Z",
            },
          });
        }
        yield* f.fs.writeFileString(f.path.join(f.slot, "file"), "discard expired tracked edits");
        yield* f.settings.updateSettings({
          storageCleanup: {
            ...DEFAULT_SERVER_SETTINGS.storageCleanup,
            worktreeAfterDays: settled ? null : 1,
            worktreeSettledAfterDays: settled ? 0 : null,
          },
        });
        yield* TestClock.adjust(settled ? 61_000 : 86_461_000);
        const cleanup = yield* retirementCleanup();
        const repaired = yield* cleanup.runNow;
        assert.equal(
          repaired.counts.removed,
          1,
          repaired.entries.map((entry) => entry.reason).join("\n"),
        );
        assert.equal((yield* f.projections.getThread(f.threadId)).branch, "review");
        assert.equal(
          (yield* f.projections.getThread(f.childId)).branch,
          state === "deleted-family-member" ? "temporary" : "review",
        );
        assert.isTrue(
          (yield* f.leases.releasedThread({ path: f.slot, threadId: f.childId }))?.record.returned,
        );
        assert.equal(repaired.returnedCount, 0);
        for (const branch of ["temporary", "review"])
          assert.equal(yield* f.git({ args: ["rev-parse", `refs/heads/${branch}`] }), f.parentSha);
        assert.isFalse(yield* f.fs.exists(f.slot));
        assert.equal((yield* f.projections.getThread(f.threadId)).worktreePath, f.slot);
        assert.equal(
          (yield* f.leases.releasedThread({ path: f.slot, threadId: f.threadId }))?.record.release
            ?.projectPath,
          f.cwd,
        );
        if (changedRoot !== undefined)
          assert.equal(
            yield* f.fs.readFileString(f.path.join(changedRoot, "current-project-proof")),
            "keep current project",
          );
        if (!settled) assert.isNull((yield* f.projections.getThread(f.threadId)).settledAt);
      }).pipe(Effect.provide(runtime)),
  );
  it.effect(
    "synchronizes the owned family without checking out, returning or losing ignored files",
    () =>
      Effect.gen(function* () {
        const f = yield* prepare();
        const before = yield* f.fs.readFileString(f.path.join(f.root, "calls"));
        const result = yield* f.service.reconcileBranch({ scope: f.scope, input: f.input });
        assert.sameMembers([...result.updatedThreadIds], [f.threadId, f.childId]);
        for (const id of [f.threadId, f.childId])
          assert.equal((yield* f.projections.getThread(id)).branch, "review");
        assert.equal(yield* f.git({ cwd: f.slot, args: ["branch", "--show-current"] }), "review");
        assert.equal(yield* f.git({ cwd: f.slot, args: ["rev-parse", "HEAD"] }), f.parentSha);
        assert.equal(yield* f.git({ args: ["rev-parse", "refs/heads/temporary"] }), f.parentSha);
        assert.equal(
          yield* f.fs.readFileString(f.path.join(f.slot, "retained.fixture")),
          "synthetic retained artifact\n",
        );
        assert.equal(yield* f.fs.readFileString(f.path.join(f.root, "calls")), before);
        assert.equal((yield* f.leases.read(f.slot))?.record.returned, false);
        assert.deepEqual(
          (yield* f.service.reconcileBranch({ scope: f.scope, input: f.input })).updatedThreadIds,
          [],
        );
      }).pipe(Effect.provide(runtime)),
  );

  it.effect("preserves a concurrent edit and safely retries stopped family synchronization", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      let conflictTarget: ThreadId | undefined;
      let inject = true;
      const f = yield* prepare({
        beforeDispatch: (command) =>
          Effect.gen(function* () {
            if (
              !inject ||
              conflictTarget === undefined ||
              command.type !== "thread.metadata.update" ||
              command.threadId !== conflictTarget
            )
              return;
            inject = false;
            yield* orchestrator
              .dispatch({
                type: "thread.metadata.update",
                commandId: CommandId.make("concurrent-user-edit"),
                threadId: conflictTarget,
                title: "Newer user title",
              })
              .pipe(Effect.orDie);
          }),
      });
      conflictTarget = f.childId;
      const before = yield* f.fs.readFileString(f.path.join(f.root, "calls"));
      const failed = yield* f.service
        .reconcileBranch({ scope: f.scope, input: f.input })
        .pipe(Effect.result);
      if (Result.isSuccess(failed))
        return assert.fail("Concurrent metadata must stop reconciliation");
      assert.equal(failed.failure.code, "operation_failed");
      const interrupted = yield* f.projections.getThread(f.childId);
      assert.equal(interrupted.title, "Newer user title");
      assert.equal(interrupted.branch, "temporary");
      yield* f.service.reconcileBranch({ scope: f.scope, input: f.input });
      for (const id of [f.threadId, f.childId])
        assert.equal((yield* f.projections.getThread(id)).branch, "review");
      assert.equal((yield* f.projections.getThread(f.childId)).title, "Newer user title");
      assert.deepEqual(
        (yield* f.service.reconcileBranch({ scope: f.scope, input: f.input })).updatedThreadIds,
        [],
      );
      assert.equal(yield* f.git({ cwd: f.slot, args: ["rev-parse", "HEAD"] }), f.parentSha);
      assert.equal(yield* f.fs.readFileString(f.path.join(f.root, "calls")), before);
      assert.equal((yield* f.leases.read(f.slot))?.record.returned, false);
    }).pipe(Effect.provide(runtime)),
  );

  it.effect.each([
    "dirty",
    "new-commit",
    "branch-changed",
    "unsettled",
    "capability",
    "provider-pin",
    "family-changed",
    "lease-changed",
  ] as const)("refuses changed inspection: %s", (change) =>
    Effect.gen(function* () {
      const f = yield* prepare();
      if (change === "dirty")
        yield* f.fs.writeFileString(f.path.join(f.slot, "untracked"), "keep me");
      if (change === "new-commit")
        yield* f.git({ cwd: f.slot, args: ["commit", "--allow-empty", "-m", "new work"] });
      if (change === "branch-changed")
        yield* f.git({ cwd: f.slot, args: ["switch", "-c", "other"] });
      if (change === "unsettled")
        yield* f.orchestrator.dispatch({
          type: "thread.unsettle",
          reason: "user",
          commandId: CommandId.make("reopen-owner"),
          threadId: f.threadId,
        });
      if (change === "provider-pin") {
        const release = yield* f.leases.pinUse({
          path: f.slot,
          threadId: f.threadId,
          kind: "provider",
        });
        yield* Effect.addFinalizer(() => release);
      }
      if (change === "family-changed") f.input.expected.threadIds = [f.threadId];
      if (change === "lease-changed") f.input.expected.leaseId = "f".repeat(32);
      const result = yield* f.service
        .reconcileBranch({
          scope:
            change === "capability"
              ? { ...f.scope, capabilities: new Set(["worktree"] as const) }
              : f.scope,
          input: f.input,
        })
        .pipe(Effect.result);
      if (Result.isSuccess(result)) return assert.fail("Changed inspection must be refused");
      assert.equal(
        result.failure.code,
        change === "capability" ? "capability_denied" : "invalid_request",
      );
      for (const id of [f.threadId, f.childId])
        assert.equal((yield* f.projections.getThread(id)).branch, "temporary");
      assert.equal(
        yield* f.git({ cwd: f.slot, args: ["branch", "--show-current"] }),
        change === "branch-changed" ? "other" : "review",
      );
      assert.equal(
        yield* f.fs.readFileString(f.path.join(f.slot, "retained.fixture")),
        "synthetic retained artifact\n",
      );
      assert.equal((yield* f.leases.read(f.slot))?.record.returned, false);
    }).pipe(Effect.provide(runtime)),
  );
});

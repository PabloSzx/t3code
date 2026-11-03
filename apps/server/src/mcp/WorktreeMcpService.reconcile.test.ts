import { assert, describe, it } from "@effect/vitest";
import {
  CommandId,
  EnvironmentId,
  EventId,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Result from "effect/Result";
import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
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
  ProviderReplayHarness.layerWithRegistry(
    { name: "worktree-reconciliation" },
    ProviderAdapterRegistry.layerFromAdapters([]),
    { databaseLayer: database, runEffectWorker: false },
  ),
).pipe(Layer.provideMerge(testLayer));

const prepare = Effect.fn("test.prepareWorktreeReconciliation")(function* ({
  beforeDispatch,
}: {
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
  yield* f.git({ cwd: f.slot, args: ["switch", "-c", "review"] });
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
  const service = yield* WorktreeMcpService.WorktreeMcpService.pipe(
    Effect.provide(
      WorktreeMcpService.layer.pipe(
        Layer.provide(
          Layer.mergeAll(
            Layer.mock(ThreadManagement.ThreadManagementService)({
              dispatch: (command) =>
                Effect.gen(function* () {
                  if (beforeDispatch) yield* beforeDispatch(command);
                  return yield* orchestrator.dispatch(command);
                }),
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
        branch: "review",
        headSha: f.parentSha,
        leaseId: owned.record.lease.lease_id,
        threadIds: [threadId, childId],
      },
    },
  };
});

describe("worktree branch reconciliation", () => {
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

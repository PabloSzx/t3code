import { expect, it } from "@effect/vitest";
import {
  EventId,
  ProjectId,
  ProviderInstanceId,
  WorkspaceOperation,
  type Project,
} from "@t3tools/contracts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Scope from "effect/Scope";
import * as Exit from "effect/Exit";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";

import { accessFixture, testLayer } from "../vcs/TreehouseWorktrees.testkit.ts";
import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as EventStore from "../orchestration-v2/EventStore.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as Sqlite from "../persistence/Sqlite.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as ExternalLauncher from "../process/externalLauncher.ts";
import * as WorkspaceActions from "./WorkspaceActions.ts";
import * as WorkspaceLifecycle from "./WorkspaceLifecycle.ts";

const fixture = Effect.fn("WorkspaceActionsTest.fixture")(function* () {
  const f = yield* accessFixture();
  const sink = yield* EventSink.EventSinkV2;
  const now = yield* DateTime.now;
  const projectId = ProjectId.make("workspace-project");
  const project: Project = {
    id: projectId,
    title: "Workspace",
    workspaceRoot: f.cwd,
    scripts: [],
    defaultModelSelection: null,
    createdAt: DateTime.formatIso(now),
    updatedAt: DateTime.formatIso(now),
    deletedAt: null,
  };
  yield* sink.write({
    events: [
      {
        id: EventId.make("workspace-created"),
        threadId: f.context.threadId,
        occurredAt: now,
        type: "thread.created",
        payload: {
          id: f.context.threadId,
          projectId,
          title: "History stays readable",
          createdBy: "user",
          creationSource: "web",
          providerInstanceId: ProviderInstanceId.make("codex"),
          modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "test" },
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: "access-owner",
          worktreePath: f.slot,
          activeProviderThreadId: null,
          lineage: {
            rootThreadId: f.context.threadId,
            parentThreadId: null,
            relationshipToParent: null,
          },
          forkedFrom: null,
          createdAt: now,
          updatedAt: now,
          archivedAt: null,
          settledOverride: null,
          settledAt: null,
          lastVisitedAt: null,
          deletedAt: null,
        },
      },
    ],
  });
  const unexpected = () => Effect.die("Unexpected project mutation");
  const projects = ProjectService.ProjectService.of({
    getById: () => Effect.succeedSome(project),
    getByWorkspaceRoot: () => Effect.succeedSome(project),
    snapshot: Effect.succeed({ projects: [project], updatedAt: project.updatedAt }),
    getShell: unexpected,
    listShells: unexpected,
    create: unexpected,
    bootstrap: unexpected,
    update: unexpected,
    delete: unexpected,
  });
  const launched: string[] = [];
  const layer = WorkspaceActions.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        IdAllocator.layer,
        Layer.succeed(ProjectService.ProjectService, projects),
        Layer.succeed(
          ExternalLauncher.ExternalLauncher,
          ExternalLauncher.ExternalLauncher.of({
            resolveAvailableEditors: () => Effect.succeed(["vscode"]),
            // @effect-diagnostics-next-line effectSucceedWithVoid:off -- The optional reveal-kind contract requires undefined rather than void.
            resolveFileManagerRevealKind: () => Effect.succeed(undefined),
            launchBrowser: () => Effect.die("Unexpected browser"),
            launchEditor: (input) =>
              Effect.sync(() => {
                launched.push(input.cwd);
              }),
          }),
        ),
      ),
    ),
  );
  const scope = yield* Effect.acquireRelease(Scope.make(), (scope) =>
    Scope.close(scope, Exit.void),
  );
  const services = yield* Layer.build(layer).pipe(Effect.provideService(Scope.Scope, scope));
  const actions = Context.get(services, WorkspaceActions.WorkspaceActions);
  return { ...f, actions, scope, launched };
});
const layer = EventSink.layer.pipe(
  Layer.provideMerge(
    Layer.merge(EventStore.layer, ProjectionStore.layer).pipe(
      Layer.provideMerge(Sqlite.layerMemory),
    ),
  ),
  Layer.provideMerge(testLayer),
  Layer.provideMerge(WorkspaceLifecycle.layer),
);

it.effect(
  "publishes released availability without activity edits or passive allocation, then reserves the exact resumed family",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      const queue = yield* Queue.unbounded<unknown>();
      yield* f.actions.availability({ threadId: f.context.threadId }).pipe(
        Stream.runForEach((value) => Queue.offer(queue, value)),
        Effect.forkScoped,
      );
      expect(yield* Queue.take(queue)).toMatchObject({
        state: "active",
        cwd: f.slot,
        context: f.context,
      });
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const before = yield* projections.getThread(f.context.threadId);
      yield* f.release;
      expect(yield* Queue.take(queue)).toEqual({ state: "unavailable", cwd: null });
      expect(yield* Queue.take(queue)).toEqual({ state: "released", cwd: null });
      expect((yield* projections.getThread(f.context.threadId)).updatedAt).toEqual(
        before.updatedAt,
      );
      const calls = yield* f.fs.readFileString(f.path.join(f.root, "calls"));
      expect(
        yield* f.actions
          .prepare({ threadId: f.context.threadId, owner: "viewer", activate: false })
          .pipe(Effect.result),
      ).toMatchObject({ _tag: "Failure" });
      expect(yield* f.fs.readFileString(f.path.join(f.root, "calls"))).toBe(calls);
      yield* f.driver.createWorktree({
        cwd: f.cwd,
        refName: f.baseSha,
        newRefName: "unrelated",
        path: null,
      });
      yield* f.fs.writeFileString(f.path.join(f.root, "next-slot"), "");
      yield* f.settings.updateSettings({
        treehouseWorktreesEnabled: false,
        treehouseBinaryPath: "/missing/new-binary",
      });
      const reservation = yield* f.actions
        .prepare({ threadId: f.context.threadId, owner: "operator" })
        .pipe(Effect.provideService(WorkspaceOperation, f.context));
      expect(reservation.workspace.cwd).toBe(f.path.join(f.root, "pool", "2", "repo"));
      expect(reservation.workspace.context.generation).not.toBe(f.context.generation);
      expect(
        yield* f.git({ cwd: reservation.workspace.cwd, args: ["branch", "--show-current"] }),
      ).toBe("access-owner");
      expect(reservation.reservationId).not.toBeNull();
      if (reservation.reservationId === null) return;
      expect(
        yield* f.actions
          .complete({ reservationId: reservation.reservationId, owner: "foreign-session" })
          .pipe(Effect.result),
      ).toMatchObject({ _tag: "Failure" });
      const release = f.treehouse.returnIdle({
        cwd: f.cwd,
        path: reservation.workspace.cwd,
        branch: "access-owner",
        headSha: f.parentSha,
        threadIds: [f.context.threadId],
        recheck: Effect.succeed(null),
      });
      expect(yield* release.pipe(Effect.flip)).toMatchObject({ cause: { reason: "in-use" } });
      yield* f.actions.complete({ reservationId: reservation.reservationId, owner: "operator" });
      expect(yield* release).toBeNull();
    }).pipe(Effect.provide(layer)),
);

it.effect.each(["ack", "expiry", "shutdown"] as const)(
  "holds remote handoff through %s without a mounted-window pin",
  (ending) =>
    Effect.gen(function* () {
      const f = yield* fixture();
      const remote = yield* f.actions.openEditor({
        owner: "viewer",
        input: {
          editor: "vscode",
          cwd: `${f.slot}/file`,
          workspace: { threadId: f.context.threadId, root: f.slot },
          remote: { host: "test-host", isFile: true },
          activate: false,
        },
      });
      expect(remote.url).toContain("test-host");
      expect(f.launched).toEqual([]);
      expect(yield* f.release.pipe(Effect.flip)).toMatchObject({ cause: { reason: "in-use" } });
      if (remote.reservationId === null) throw new Error("Pooled handoff must be reserved");
      if (ending === "ack")
        yield* f.actions.complete({ owner: "viewer", reservationId: remote.reservationId });
      else if (ending === "expiry") yield* TestClock.adjust("30 seconds");
      else yield* Scope.close(f.scope, Exit.void);
      expect(yield* f.release).toBeNull();
    }).pipe(Effect.provide(layer)),
);

it.effect("admits at most sixteen concurrent reservations per owner", () =>
  Effect.gen(function* () {
    const f = yield* fixture();
    const attempts = yield* Effect.forEach(
      Array.from({ length: 17 }),
      () =>
        f.actions
          .prepare({ owner: "busy", threadId: f.context.threadId, activate: false })
          .pipe(Effect.result),
      { concurrency: "unbounded" },
    );
    const reserved = attempts.flatMap((attempt) =>
      attempt._tag === "Success" && attempt.success.reservationId !== null
        ? [attempt.success.reservationId]
        : [],
    );
    expect(reserved).toHaveLength(16);
    // Another owner still has its own admissions while this one is full.
    const other = yield* f.actions.prepare({
      owner: "other",
      threadId: f.context.threadId,
      activate: false,
    });
    expect(other.reservationId).not.toBeNull();
    for (const reservationId of reserved)
      yield* f.actions.complete({ owner: "busy", reservationId });
    // Completed reservations return their admissions.
    const again = yield* f.actions.prepare({
      owner: "busy",
      threadId: f.context.threadId,
      activate: false,
    });
    expect(again.reservationId).not.toBeNull();
  }).pipe(Effect.provide(layer)),
);

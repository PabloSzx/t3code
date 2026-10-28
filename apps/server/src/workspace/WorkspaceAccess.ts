import { ThreadId, WorkspaceOperation, type ActiveWorkspace, type WorkspaceAvailability } from "@t3tools/contracts";
export type { ActiveWorkspace, WorkspaceAvailability } from "@t3tools/contracts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";

import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as TreehouseWorktrees from "../vcs/TreehouseWorktrees.ts";
import * as TreehouseLeases from "./treehouseLeases.ts";
import { withWorkspaceLease } from "./workspaceLease.ts";

export class WorkspaceAccessError extends Schema.TaggedError<WorkspaceAccessError>()(
  "WorkspaceAccessError",
  { threadId: ThreadId, cause: Schema.Defect() },
) {
  override get message(): string {
    return "This conversation's workspace is unavailable. Resume it with an explicit workspace action; do not use its saved checkout path.";
  }
}
type ThreadLookup = ProjectionStore.ProjectionStoreV2 | ProjectService.ProjectService;
export type ActivationDependencies =
  | ThreadLookup
  | EventSink.EventSinkV2
  | IdAllocator.IdAllocatorV2
  | ServerSettings.ServerSettingsService
  | Scope.Scope;

export interface WorkspaceUseOptions<A, E, R> {
  readonly targets: readonly string[];
  readonly effect: Effect.Effect<A, E, R>;
}
export interface WorkspaceThreadUseOptions<A, E, R> extends WorkspaceUseOptions<A, E, R> {
  readonly threadId: ThreadId;
  readonly cwd: string;
}
export class WorkspaceAccess extends Context.Service<
  WorkspaceAccess,
  {
    readonly availability: (input: {
      readonly threadId: ThreadId;
    }) => Effect.Effect<WorkspaceAvailability, WorkspaceAccessError, ThreadLookup>;
    readonly activate: (input: {
      readonly threadId: ThreadId;
    }) => Effect.Effect<ActiveWorkspace, WorkspaceAccessError, ActivationDependencies>;
    readonly useThread: <A, E, R>(
      input: WorkspaceThreadUseOptions<A, E, R>,
    ) => Effect.Effect<A, E | TreehouseLeases.TreehouseLeaseError, R>;
    readonly use: <A, E, R>(
      input: WorkspaceUseOptions<A, E, R>,
    ) => Effect.Effect<A, E | TreehouseLeases.TreehouseLeaseError, R>;
  }
>()("t3/workspace/WorkspaceAccess") {}

const make = Effect.gen(function* () {
  const path = yield* Path.Path;
  const fs = yield* FileSystem.FileSystem;
  const leases = yield* TreehouseLeases.TreehouseLeaseStore;
  const threadWorkspace = Effect.fn("WorkspaceAccess.threadWorkspace")(function* (
    threadId: ThreadId,
  ) {
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const { thread } = yield* projections.getThreadRecords(threadId, []);
    if (thread.deletedAt !== null)
      return yield* new TreehouseLeases.TreehouseLeaseError({ path: threadId, reason: "missing" });
    if (thread.worktreePath !== null) return { thread, cwd: thread.worktreePath };
    const projects = yield* ProjectService.ProjectService;
    const project = yield* projects.getById(thread.projectId);
    if (Option.isNone(project))
      return yield* new TreehouseLeases.TreehouseLeaseError({ path: threadId, reason: "missing" });
    return { thread, cwd: project.value.workspaceRoot };
  });
  const availability = Effect.fn("WorkspaceAccess.availability")(
    function* (input: {
      readonly threadId: ThreadId;
    }): Effect.fn.Return<
      WorkspaceAvailability,
      | TreehouseLeases.TreehouseLeaseError
      | ProjectionStore.ProjectionStoreV2Error
      | ProjectService.ProjectOperationError,
      ThreadLookup
    > {
      const { cwd } = yield* threadWorkspace(input.threadId);
      const released = yield* leases.releasedThread({ path: cwd, threadId: input.threadId });
      if (released !== null)
        return {
          state:
            released.record.returned && !released.record.release?.resuming
              ? "released"
              : "unavailable",
          cwd: null,
        };
      const generation = yield* leases.operationGeneration({ path: cwd, threadId: input.threadId });
      return {
        state: generation === null ? "native" : "active",
        cwd,
        context: { threadId: input.threadId, generation },
      };
    },
    (effect, input) =>
      effect.pipe(
        Effect.mapError(
          (cause) =>
            new WorkspaceAccessError({
              threadId: input.threadId,
              cause,
            }),
        ),
      ),
  );
  const activate = Effect.fn("WorkspaceAccess.activate")(
    function* (input: { readonly threadId: ThreadId }) {
      const initial = yield* threadWorkspace(input.threadId);
      const preparationScope = yield* Effect.scope;
      return yield* withWorkspaceLease(
        path.resolve(initial.cwd),
        Effect.gen(function* () {
          const { thread, cwd } = yield* threadWorkspace(input.threadId);
          const released = yield* leases.releasedThread({ path: cwd, threadId: input.threadId });
          if (released !== null) {
            if (thread.branch === null)
              return yield* new TreehouseLeases.TreehouseLeaseError({
                path: cwd,
                reason: "stale-binding",
              });
            const projects = yield* ProjectService.ProjectService;
            const project = yield* projects.getById(thread.projectId);
            if (Option.isNone(project))
              return yield* new TreehouseLeases.TreehouseLeaseError({
                path: cwd,
                reason: "missing",
              });
            const treehouse = yield* TreehouseWorktrees.TreehouseWorktrees.pipe(
              Effect.provide(TreehouseWorktrees.layer),
            );
            const resumed = yield* treehouse.resume({
              cwd: project.value.workspaceRoot,
              path: cwd,
              branch: thread.branch,
              threadId: input.threadId,
              preparationScope,
            });
            if (resumed === null)
              return yield* new TreehouseLeases.TreehouseLeaseError({
                path: cwd,
                reason: "stale-binding",
              });
            const sink = yield* EventSink.EventSinkV2;
            const ids = yield* IdAllocator.IdAllocatorV2;
            yield* sink.rebindWorktree({
              threadId: input.threadId,
              previousPath: cwd,
              familyThreadIds:
                released.record.release?.threadIds.map((id) => ThreadId.make(id)) ?? [],
              path: resumed,
              branch: thread.branch,
              eventId: yield* ids.allocate.event({ threadId: input.threadId }),
              occurredAt: yield* DateTime.now,
            });
          }
          const available = yield* availability(input);
          if (available.cwd === null)
            return yield* new TreehouseLeases.TreehouseLeaseError({ path: cwd, reason: "missing" });
          // Preparation remains pinned through family binding and acquisition of the operation pin.
          yield* Effect.acquireRelease(
            leases.pinOperation({ targets: [available.cwd], context: available.context }),
            (release) => release,
          ).pipe(Effect.provideService(Scope.Scope, preparationScope), Effect.asVoid);
          return available;
        }),
      );
    },
    (effect, input) =>
      effect.pipe(
        Effect.provideService(FileSystem.FileSystem, fs),
        Effect.provideService(Path.Path, path),
        // Allocation is trusted lifecycle work, not a continuation of a stale ordinary request.
        Effect.provideService(WorkspaceOperation, undefined),
        Effect.mapError((cause) => new WorkspaceAccessError({ threadId: input.threadId, cause })),
      ),
  );
  const use: WorkspaceAccess["Service"]["use"] = Effect.fn("WorkspaceAccess.use")(
    function* (input) {
      const context = yield* WorkspaceOperation;
      return yield* Effect.acquireUseRelease(
        leases.pinOperation({
          targets: input.targets,
          ...(context === undefined ? {} : { context }),
        }),
        () => input.effect,
        (release) => release,
      );
    },
  );
  const useThread: WorkspaceAccess["Service"]["useThread"] = Effect.fn("WorkspaceAccess.useThread")(
    function* (input) {
      const captured = yield* WorkspaceOperation;
      const context =
        captured?.threadId === input.threadId
          ? captured
          : {
              threadId: input.threadId,
              generation: yield* leases.operationGeneration({
                path: input.cwd,
                threadId: input.threadId,
              }),
            };
      return yield* use({ targets: [input.cwd, ...input.targets], effect: input.effect }).pipe(
        Effect.provideService(WorkspaceOperation, context),
      );
    },
  );
  return WorkspaceAccess.of({ availability, activate, use, useThread });
});
export const layer = Layer.effect(WorkspaceAccess, make).pipe(Layer.provide(TreehouseLeases.layer));

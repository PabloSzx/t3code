import {
  buildRemoteOpenUrl,
  WorkspaceOperation,
  WorkspaceUnavailableError,
  type ThreadId,
  type WorkspaceAvailability,
  type WorkspaceEditorInput,
  type WorkspaceReservation,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";

import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as ExternalLauncher from "../process/externalLauncher.ts";
import * as WorkspaceAccess from "./WorkspaceAccess.ts";
import * as WorkspaceLifecycle from "./WorkspaceLifecycle.ts";
import * as TreehouseLeases from "./treehouseLeases.ts";

const RESERVATION_MS = 30_000;
type Dependencies = Exclude<WorkspaceAccess.ActivationDependencies, Scope.Scope>;
export interface PrepareWorkspaceActionOptions {
  readonly threadId: ThreadId;
  readonly owner: string;
  readonly activate?: boolean;
}
export interface CompleteWorkspaceActionOptions {
  readonly reservationId: string;
  readonly owner: string;
}
export interface OpenWorkspaceEditorOptions {
  readonly input: WorkspaceEditorInput;
  readonly owner: string;
}
export class WorkspaceActions extends Context.Service<
  WorkspaceActions,
  {
    readonly availability: (input: {
      readonly threadId: ThreadId;
    }) => Stream.Stream<WorkspaceAvailability>;
    readonly prepare: (
      input: PrepareWorkspaceActionOptions,
    ) => Effect.Effect<WorkspaceReservation, WorkspaceUnavailableError>;
    readonly complete: (
      input: CompleteWorkspaceActionOptions,
    ) => Effect.Effect<void, WorkspaceUnavailableError>;
    readonly openEditor: (options: OpenWorkspaceEditorOptions) => Effect.Effect<
      {
        readonly url: string | null;
        readonly reservationId: string | null;
      },
      WorkspaceUnavailableError | ExternalLauncher.ExternalLauncherError
    >;
  }
>()("t3/workspace/WorkspaceActions") {}

const make = Effect.gen(function* () {
  const access = yield* WorkspaceAccess.WorkspaceAccess;
  const leases = yield* TreehouseLeases.TreehouseLeaseStore.pipe(
    Effect.provide(TreehouseLeases.layer),
  );
  const lifecycle = yield* WorkspaceLifecycle.WorkspaceLifecycle;
  const path = yield* Path.Path;
  const crypto = yield* Crypto.Crypto;
  const lifetime = yield* Scope.Scope;
  const dependencies = yield* Effect.context<Dependencies | ExternalLauncher.ExternalLauncher>();
  const reservations = new Map<
    string,
    {
      readonly owner: string;
      readonly stop: Deferred.Deferred<void>;
      readonly closed: Deferred.Deferred<void>;
    }
  >();
  const admissions = new Map<string, number>();
  let totalAdmissions = 0;
  const unavailable = () => new WorkspaceUnavailableError({ reason: "unavailable" });
  const prepare = Effect.fn("WorkspaceActions.prepare")(
    function* (input: PrepareWorkspaceActionOptions) {
      // Admission is counted synchronously before any await, so concurrent prepares cannot all
      // pass the cap; each admission is returned when its attempt ends without a reservation.
      const owned = admissions.get(input.owner) ?? 0;
      if (totalAdmissions >= 1024 || owned >= 16) return yield* unavailable();
      admissions.set(input.owner, owned + 1);
      totalAdmissions += 1;
      const releaseAdmission = Effect.sync(() => {
        const remaining = (admissions.get(input.owner) ?? 1) - 1;
        if (remaining === 0) admissions.delete(input.owner);
        else admissions.set(input.owner, remaining);
        totalAdmissions -= 1;
      });
      let transferred = false;
      return yield* Effect.uninterruptibleMask((restore) =>
        Effect.gen(function* () {
          const scope = yield* Scope.make();
          const acquire =
            input.activate !== false
              ? access.activate({ threadId: input.threadId })
              : Effect.gen(function* () {
                  const active = yield* access.availability({ threadId: input.threadId });
                  if (active.cwd === null) return yield* unavailable();
                  yield* Effect.acquireRelease(
                    leases.pinOperation({ targets: [active.cwd], context: active.context }),
                    (release) => release,
                  ).pipe(Effect.asVoid);
                  return active;
                });
          const workspace = yield* restore(
            acquire.pipe(Effect.provideService(Scope.Scope, scope)),
          ).pipe(
            Effect.onExit((exit) =>
              Exit.isFailure(exit) ? Scope.close(scope, exit) : Effect.void,
            ),
          );
          if (workspace.state === "native") {
            yield* Scope.close(scope, Exit.void);
            return { workspace, reservationId: null };
          }
          transferred = true;
          const reservationId = yield* crypto.randomUUIDv4().pipe(Effect.orDie);
          const stop = yield* Deferred.make<void>();
          const closed = yield* Deferred.make<void>();
          reservations.set(reservationId, { owner: input.owner, stop, closed });
          yield* Deferred.await(stop).pipe(
            Effect.timeoutOrElse({ duration: RESERVATION_MS, orElse: () => Effect.void }),
            Effect.ensuring(
              Scope.close(scope, Exit.void).pipe(
                Effect.andThen(Effect.sync(() => reservations.delete(reservationId))),
                Effect.andThen(releaseAdmission),
                Effect.andThen(Deferred.succeed(closed, undefined)),
              ),
            ),
            Effect.interruptible,
            Effect.forkIn(lifetime, { startImmediately: true }),
          );
          return { workspace, reservationId };
        }),
      ).pipe(Effect.ensuring(Effect.suspend(() => (transferred ? Effect.void : releaseAdmission))));
    },
    Effect.catchTags({
      WorkspaceAccessError: (cause) =>
        Effect.fail(new WorkspaceUnavailableError({ reason: "unavailable", cause })),
      TreehouseLeaseError: (cause) =>
        Effect.fail(new WorkspaceUnavailableError({ reason: "unavailable", cause })),
    }),
    Effect.provide(dependencies),
  );
  const complete = Effect.fn("WorkspaceActions.complete")(function* (
    input: CompleteWorkspaceActionOptions,
  ) {
    const entry = reservations.get(input.reservationId);
    if (entry === undefined || entry.owner !== input.owner) return yield* unavailable();
    yield* Deferred.succeed(entry.stop, undefined);
    yield* Deferred.await(entry.closed);
  });
  const availability = (input: { readonly threadId: ThreadId }) =>
    lifecycle.changes.pipe(
      Stream.mapEffect(() =>
        access.availability(input).pipe(
          Effect.provide(dependencies),
          Effect.catchTags({
            WorkspaceAccessError: () =>
              Effect.succeed<WorkspaceAvailability>({ state: "unavailable", cwd: null }),
          }),
        ),
      ),
      Stream.changesWith((left, right) => JSON.stringify(left) === JSON.stringify(right)),
    );
  const openEditor = Effect.fn("WorkspaceActions.openEditor")(function* ({
    input,
    owner,
  }: OpenWorkspaceEditorOptions) {
    const captured = yield* WorkspaceOperation;
    let target = input.cwd;
    let reservation: WorkspaceReservation | undefined;
    if (input.workspace !== undefined) {
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const { thread } = yield* projections
        .getThreadRecords(input.workspace.threadId, [])
        .pipe(
          Effect.mapError(
            (cause) => new WorkspaceUnavailableError({ reason: "unavailable", cause }),
          ),
        );
      let root = thread.worktreePath;
      if (root === null) {
        const projects = yield* ProjectService.ProjectService;
        const project = yield* projects
          .getById(thread.projectId)
          .pipe(
            Effect.mapError(
              (cause) => new WorkspaceUnavailableError({ reason: "unavailable", cause }),
            ),
          );
        if (Option.isNone(project)) return yield* unavailable();
        root = project.value.workspaceRoot;
      }
      if (path.resolve(root) !== path.resolve(input.workspace.root)) return yield* unavailable();
      reservation = yield* prepare({
        owner,
        threadId: input.workspace.threadId,
        activate: input.remote === undefined || input.activate === true,
      });
      const relative = path.relative(root, target);
      if (
        relative === "" ||
        (!relative.startsWith(`..${path.sep}`) && relative !== ".." && !path.isAbsolute(relative))
      )
        target = path.join(reservation.workspace.cwd, relative);
    }
    const prepared = reservation;
    const release =
      prepared?.reservationId == null
        ? Effect.void
        : complete({ owner, reservationId: prepared.reservationId });
    const operation = Effect.gen(function* () {
      if (input.remote !== undefined) {
        // Position suffixes are editor syntax, not part of the filesystem target.
        const file = target.replace(/:(\d+)(?::\d+)?$/, "");
        return yield* access
          .use({
            targets: [file],
            effect: Effect.gen(function* () {
              const remote = input.remote!;
              const url = buildRemoteOpenUrl({
                editor: input.editor,
                host: remote.host,
                ...(remote.username === undefined ? {} : { username: remote.username }),
                ...(remote.isFile === undefined ? {} : { isFile: remote.isFile }),
                absolutePath: target,
              });
              if (url === undefined) return yield* unavailable();
              return { url, reservationId: prepared?.reservationId ?? null };
            }),
          })
          .pipe(
            Effect.catchTags({
              TreehouseLeaseError: (cause) =>
                Effect.fail(new WorkspaceUnavailableError({ reason: "unavailable", cause })),
            }),
          );
      }
      const launcher = yield* ExternalLauncher.ExternalLauncher;
      yield* launcher.launchEditor({ ...input, cwd: target });
      yield* release;
      return { url: null, reservationId: null };
    }).pipe(Effect.onError(() => release.pipe(Effect.ignore)));
    return yield* operation.pipe(
      Effect.provideService(WorkspaceOperation, prepared?.workspace.context ?? captured),
    );
  }, Effect.provide(dependencies));
  return WorkspaceActions.of({ availability, prepare, complete, openEditor });
});
export const layer = Layer.effect(WorkspaceActions, make).pipe(
  Layer.provide(WorkspaceAccess.layer),
);

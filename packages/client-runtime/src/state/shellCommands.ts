import {
  WS_METHODS,
  WorkspaceUnavailableError,
  type EnvironmentId,
  type ThreadId,
  type WorkspaceAvailability,
  type WorkspaceEditorResult,
  type WorkspaceOperationContext,
  type WorkspaceReservation,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import { AsyncResult, Atom } from "effect/reactivity";
import type {
  AtomCommandResult,
  AtomCommandWorkspaceBinding,
  EnvironmentRpcTarget,
} from "./runtime.ts";
import { hasWorkspaceTargets, rebaseWorkspaceInput } from "./workspaceTargets.ts";

import {
  createEnvironmentRpcCommand,
  createEnvironmentRpcSubscriptionAtomFamily,
} from "./runtime.ts";
import type { EnvironmentRegistry } from "../connection/registry.ts";

export interface ThreadWorkspaceCommandOptions<W, A, E> {
  readonly target: W;
  readonly source: {
    readonly environmentId: EnvironmentId;
    readonly threadId: ThreadId;
    readonly root: string;
  } | null;
  readonly availability: WorkspaceAvailability | null;
  /**
   * `captured` never resumes a returned workspace, for replays and queued work.
   * `activate` resumes it for a new user action.
   */
  readonly mode: "activate" | "captured";
  readonly binding?: AtomCommandWorkspaceBinding<W>;
  readonly run: (target: W) => Promise<AtomCommandResult<A, E>>;
  readonly prepare: () => Promise<AtomCommandResult<WorkspaceReservation, unknown>>;
  readonly complete: (reservationId: string) => Promise<unknown>;
}

function defaultBinding<W>(target: W): AtomCommandWorkspaceBinding<W> | null {
  if (target === null || typeof target !== "object" || !("environmentId" in target)) return null;
  if (!("input" in target)) return null;
  const request = target as EnvironmentRpcTarget<unknown>;
  if (request.workspace !== undefined || !hasWorkspaceTargets(request.input)) return null;
  const cwd =
    typeof request.input === "object" && request.input !== null && "cwd" in request.input
      ? (request.input as { readonly cwd?: unknown }).cwd
      : undefined;
  return {
    environmentId: request.environmentId,
    cwd: typeof cwd === "string" ? cwd : null,
    bind: (current) => current,
  };
}

const unavailableResult = <A>(cause?: unknown) =>
  AsyncResult.failure<A, WorkspaceUnavailableError>(
    Cause.fail(
      new WorkspaceUnavailableError({
        reason: "unavailable",
        ...(cause === undefined ? {} : { cause }),
      }),
    ),
  );

export async function runThreadWorkspaceCommand<W, A, E>(
  options: ThreadWorkspaceCommandOptions<W, A, E>,
): Promise<AtomCommandResult<A, E | WorkspaceUnavailableError>> {
  const { target, source, availability } = options;
  if (source === null) return options.run(target);
  const explicit = options.binding;
  const binding = explicit ?? defaultBinding(target);
  if (binding === null || binding.environmentId !== source.environmentId)
    return options.run(target);
  const attach = (context: WorkspaceOperationContext, cwd: string): W => {
    if (explicit !== undefined) {
      const rebased =
        binding.cwd === null
          ? cwd
          : (rebaseWorkspaceInput({ input: { cwd: binding.cwd }, from: source.root, to: cwd })
              .cwd as string);
      return explicit.bind(target, { context, cwd: rebased }) as W;
    }
    const request = target as EnvironmentRpcTarget<unknown>;
    return {
      ...target,
      input: rebaseWorkspaceInput({ input: request.input, from: source.root, to: cwd }),
      workspace: context,
    } as W;
  };
  // A workspace that is native or still active needs no allocation, so it uses its current
  // generation and keeps the action's own authorization; only a returned one needs prepare.
  if (availability?.state === "native" || availability?.state === "active") {
    if (availability.cwd == null) return unavailableResult();
    return options.run(attach(availability.context, availability.cwd));
  }
  if (options.mode === "captured") return unavailableResult();
  const prepared = await options.prepare();
  if (!AsyncResult.isSuccess(prepared)) return unavailableResult(Cause.squash(prepared.cause));
  const { workspace, reservationId } = prepared.value;
  try {
    return await options.run(attach(workspace.context, workspace.cwd));
  } finally {
    if (reservationId !== null) await options.complete(reservationId);
  }
}

export interface WorkspaceEditorHandoffOptions<E> {
  readonly run: () => Promise<AtomCommandResult<WorkspaceEditorResult, E>>;
  readonly complete: (reservationId: string) => Promise<unknown>;
  readonly openUrl: (url: string) => Promise<boolean>;
}

export async function runWorkspaceEditorHandoff<E>(
  options: WorkspaceEditorHandoffOptions<E>,
): Promise<AtomCommandResult<WorkspaceEditorResult, E | WorkspaceUnavailableError>> {
  // Start before the request, so transport delay can only shorten the server's 30-second window.
  const started = performance.now();
  const result = await options.run();
  if (!AsyncResult.isSuccess(result)) return result;
  const { url, reservationId } = result.value;
  try {
    if (url !== null && (performance.now() - started >= 25_000 || !(await options.openUrl(url))))
      return AsyncResult.failure(
        Cause.fail(new WorkspaceUnavailableError({ reason: "unavailable" })),
      );
    return result;
  } catch {
    return AsyncResult.failure(
      Cause.fail(new WorkspaceUnavailableError({ reason: "unavailable" })),
    );
  } finally {
    if (reservationId !== null) await options.complete(reservationId);
  }
}

export function createShellEnvironmentAtoms<R, E>(
  runtime: Atom.AtomRuntime<EnvironmentRegistry | R, E>,
) {
  return {
    workspaceAvailability: createEnvironmentRpcSubscriptionAtomFamily(runtime, {
      label: "environment-data:workspace:availability",
      tag: WS_METHODS.workspaceAvailability,
      idleTtlMs: 0,
    }),
    prepareWorkspaceAction: createEnvironmentRpcCommand(runtime, {
      label: "environment-data:workspace:prepare-action",
      tag: WS_METHODS.workspacePrepareAction,
    }),
    completeWorkspaceAction: createEnvironmentRpcCommand(runtime, {
      label: "environment-data:workspace:complete-action",
      tag: WS_METHODS.workspaceCompleteAction,
    }),
    openInEditor: createEnvironmentRpcCommand(runtime, {
      label: "environment-data:shell:open-in-editor",
      tag: WS_METHODS.workspaceOpenEditor,
    }),
  };
}

export interface ResumeWorkspaceOptions {
  readonly availability: WorkspaceAvailability | null;
  readonly prepare: () => Promise<AtomCommandResult<WorkspaceReservation, unknown>>;
  readonly complete: (reservationId: string) => Promise<unknown>;
}

/**
 * Resumes a returned workspace before an explicit navigation such as opening a file or terminal.
 * Mounting the destination then reads through the resumed generation. Active and native
 * workspaces need nothing, so passive mounts never reach this.
 */
export async function resumeWorkspaceForAction(options: ResumeWorkspaceOptions): Promise<boolean> {
  const { availability } = options;
  if (availability === null || availability.state === "native" || availability.state === "active")
    return true;
  const prepared = await options.prepare();
  if (!AsyncResult.isSuccess(prepared)) return false;
  if (prepared.value.reservationId !== null) await options.complete(prepared.value.reservationId);
  return true;
}

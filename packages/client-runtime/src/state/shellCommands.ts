import {
  WS_METHODS,
  WorkspaceUnavailableError,
  type EnvironmentId,
  type ThreadId,
  type WorkspaceAvailability,
  type WorkspaceEditorResult,
  type WorkspaceReservation,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import { AsyncResult, Atom } from "effect/reactivity";
import type { AtomCommandResult, EnvironmentRpcTarget } from "./runtime.ts";
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
  readonly mode: "activate" | "captured";
  readonly run: (target: W) => Promise<AtomCommandResult<A, E>>;
  readonly prepare: () => Promise<AtomCommandResult<WorkspaceReservation, unknown>>;
  readonly complete: (reservationId: string) => Promise<unknown>;
}
export async function runThreadWorkspaceCommand<W, A, E>(
  options: ThreadWorkspaceCommandOptions<W, A, E>,
): Promise<AtomCommandResult<A, E | WorkspaceUnavailableError>> {
  const { target, source, availability } = options;
  if (
    source === null ||
    target === null ||
    typeof target !== "object" ||
    !("environmentId" in target) ||
    !("input" in target)
  )
    return options.run(target);
  const request = target as EnvironmentRpcTarget<unknown>;
  if (
    request.environmentId !== source.environmentId ||
    request.workspace !== undefined ||
    !hasWorkspaceTargets(request.input)
  )
    return options.run(target);
  if (options.mode === "captured" || availability?.state === "native") {
    if (availability?.cwd == null)
      return AsyncResult.failure(
        Cause.fail(new WorkspaceUnavailableError({ reason: "unavailable" })),
      );
    return options.run({ ...target, workspace: availability.context });
  }
  const prepared = await options.prepare();
  if (!AsyncResult.isSuccess(prepared))
    return AsyncResult.failure(
      Cause.fail(
        new WorkspaceUnavailableError({
          reason: "unavailable",
          cause: Cause.squash(prepared.cause),
        }),
      ),
    );
  const { workspace, reservationId } = prepared.value;
  try {
    return await options.run({
      ...target,
      input: rebaseWorkspaceInput({ input: request.input, from: source.root, to: workspace.cwd }),
      workspace: workspace.context,
    });
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

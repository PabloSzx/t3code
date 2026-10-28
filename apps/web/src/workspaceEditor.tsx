import { use } from "react";
import * as Cause from "effect/Cause";
import { WorkspaceUnavailableError, AuthOrchestrationOperateScope, type EnvironmentId, type WorkspaceEditorInput, type WorkspaceEditorResult } from "@t3tools/contracts";
import { runWorkspaceEditorHandoff } from "@t3tools/client-runtime/state/shell";
import { AsyncResult } from "effect/reactivity";
import { shellEnvironment } from "./state/shell";
import { useAtomCommand } from "./state/use-atom-command";
import { readEnvironmentScope } from "./state/session";
import { openRemoteEditorUrl, useRemoteOpenState } from "./remoteOpen";

import { WorkspaceContext } from "./workspaceContext";

export function useWorkspaceEditorAction(environmentId: EnvironmentId | null, options: { readonly isFile?: boolean } = {}) {
  const source = use(WorkspaceContext);
  const remote = useRemoteOpenState(environmentId);
  const run = useAtomCommand(shellEnvironment.openInEditor, { reportFailure: false });
  const complete = useAtomCommand(shellEnvironment.completeWorkspaceAction, { reportFailure: false });
  return (target: { readonly environmentId: EnvironmentId; readonly input: WorkspaceEditorInput }) => runWorkspaceEditorHandoff({
    run: () => remote.mode === "remote-unavailable"
      ? Promise.resolve(AsyncResult.failure<WorkspaceEditorResult, WorkspaceUnavailableError>(Cause.fail(new WorkspaceUnavailableError({ reason: "unavailable" }))))
      : run({
      ...target,
      input: {
        ...target.input,
        ...(source?.environmentId === target.environmentId ? { workspace: { threadId: source.threadId, root: source.root } } : {}),
        ...(remote.mode === "remote-links" ? {
          remote: { host: remote.host.host, ...(remote.host.username === undefined ? {} : { username: remote.host.username }), isFile: options.isFile ?? true },
          activate: readEnvironmentScope(target.environmentId, AuthOrchestrationOperateScope),
        } : {}),
      },
    }),
    complete: (reservationId) => complete({ environmentId: target.environmentId, input: { reservationId } }),
    openUrl: openRemoteEditorUrl,
  });
}

import { RegistryContext } from "@effect/atom-react";
import {
  type AtomCommand,
  type AtomCommandOptions,
  type AtomCommandResult,
  runAtomCommand,
} from "@t3tools/client-runtime/state/runtime";
import { useContext } from "react";
import { WS_METHODS, type WorkspaceUnavailableError } from "@t3tools/contracts";
import { runThreadWorkspaceCommand } from "@t3tools/client-runtime/state/shell";
import { WorkspaceContext, useWorkspaceAvailability } from "../workspaceContext";
import { shellEnvironment } from "./shell";

export function useAtomCommand<A, E, W>(
  command: AtomCommand<W, A, E>,
  options?: string | AtomCommandOptions,
): (value: W) => Promise<AtomCommandResult<A, E | WorkspaceUnavailableError>> {
  const registry = useContext(RegistryContext);
  const source = useContext(WorkspaceContext);
  const availability = useWorkspaceAvailability();
  const label = typeof options === "string" ? options : (options?.label ?? command.label);
  const reportFailure = typeof options === "string" ? true : (options?.reportFailure ?? true);
  const reportDefect = typeof options === "string" ? true : (options?.reportDefect ?? true);

  return (value: W) => {
    const scoped =
      command.rpcTag === WS_METHODS.workspaceOpenEditor
        ? command
        : {
            ...command,
            run: (_registry: typeof registry, target: W) => {
              const workspaceBinding = command.workspaceBinding?.(target);
              return runThreadWorkspaceCommand({
                target,
                source,
                availability,
                // Queued status refreshes and worktree removal never resume a returned workspace.
                mode:
                  command.workspaceMode ??
                  (command.rpcTag === WS_METHODS.vcsRefreshStatus ||
                  command.rpcTag === WS_METHODS.vcsRemoveWorktree ||
                  (command.rpcTag === undefined && command.workspaceBinding === undefined)
                    ? "captured"
                    : "activate"),
                ...(workspaceBinding === undefined ? {} : { binding: workspaceBinding }),
                run: (next) => command.run(registry, next),
                prepare: () =>
                  shellEnvironment.prepareWorkspaceAction.run(registry, {
                    environmentId: source!.environmentId,
                    input: { threadId: source!.threadId },
                  }),
                complete: (reservationId) =>
                  shellEnvironment.completeWorkspaceAction.run(registry, {
                    environmentId: source!.environmentId,
                    input: { reservationId },
                  }),
              });
            },
          };
    return runAtomCommand(registry, scoped, value, { label, reportFailure, reportDefect });
  };
}

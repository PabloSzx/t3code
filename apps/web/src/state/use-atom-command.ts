import { RegistryContext } from "@effect/atom-react";
import {
  type AtomCommand,
  type AtomCommandOptions,
  type AtomCommandResult,
  runAtomCommand,
} from "@t3tools/client-runtime/state/runtime";
import { useCallback, useContext } from "react";
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

  return useCallback((value: W) => {
    const scoped = command.rpcTag === WS_METHODS.workspaceOpenEditor ? command : {
      ...command,
      run: (_registry: typeof registry, target: W) => runThreadWorkspaceCommand({
        target, source, availability,
        mode: command.rpcTag === undefined || command.rpcTag === WS_METHODS.projectsWriteFile || command.rpcTag === WS_METHODS.vcsRefreshStatus || command.rpcTag === WS_METHODS.vcsRemoveWorktree ? "captured" : "activate",
        run: (next) => command.run(registry, next),
        prepare: () => shellEnvironment.prepareWorkspaceAction.run(registry, { environmentId: source!.environmentId, input: { threadId: source!.threadId } }),
        complete: (reservationId) => shellEnvironment.completeWorkspaceAction.run(registry, { environmentId: source!.environmentId, input: { reservationId } }),
      }),
    };
    return runAtomCommand(registry, scoped, value, { label, reportFailure, reportDefect });
  }, [availability, command, label, registry, reportDefect, reportFailure, source]);
}

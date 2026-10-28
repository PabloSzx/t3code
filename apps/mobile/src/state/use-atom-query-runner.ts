import { RegistryContext } from "@effect/atom-react";
import {
  executeAtomQuery,
  scopeWorkspaceQuery,
  type AtomQueryOptions,
  type AtomCommandResult,
} from "@t3tools/client-runtime/state/runtime";
import * as Cause from "effect/Cause";
import { WorkspaceUnavailableError } from "@t3tools/contracts";
import { useWorkspaceAvailability, useWorkspaceSource } from "./workspace-access";
import { AsyncResult, type Atom } from "effect/reactivity";
import { useContext } from "react";

export function useAtomQueryRunner<T, A, E>(
  family: (target: T) => Atom.Atom<AsyncResult.AsyncResult<A, E>>,
  options?: string | AtomQueryOptions,
): (target: T) => Promise<AtomCommandResult<A, E | WorkspaceUnavailableError>> {
  const registry = useContext(RegistryContext);
  const source = useWorkspaceSource();
  const availability = useWorkspaceAvailability();
  const explicitLabel = typeof options === "string" ? options : options?.label;
  const reportFailure = typeof options === "string" ? true : (options?.reportFailure ?? true);
  const reportDefect = typeof options === "string" ? true : (options?.reportDefect ?? true);
  const refresh = typeof options === "string" ? false : (options?.refresh ?? false);

  return (target: T) => {
    const original = family(target);
    const atom = source === null ? original : scopeWorkspaceQuery({ atom: original, environmentId: source.environmentId, threadId: source.threadId, root: source.root, availability });
    if (atom === null) return Promise.resolve(AsyncResult.failure(Cause.fail(new WorkspaceUnavailableError({ reason: "unavailable" }))));
    return executeAtomQuery(registry, atom, {
      label: explicitLabel ?? atom.label?.[0] ?? "atom query",
      reportFailure,
      reportDefect,
      refresh,
    });
  };
}

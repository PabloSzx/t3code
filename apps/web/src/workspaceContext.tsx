import { createContext, use } from "react";
import { useAtomValue } from "@effect/atom-react";
import type { EnvironmentId, ThreadId, WorkspaceAvailability } from "@t3tools/contracts";
import { scopeWorkspaceQuery } from "@t3tools/client-runtime/state/runtime";
import { AsyncResult, Atom } from "effect/reactivity";
import * as Option from "effect/Option";
import { shellEnvironment } from "./state/shell";

export interface WorkspaceSource {
  readonly environmentId: EnvironmentId;
  readonly threadId: ThreadId;
  readonly root: string;
}
export const WorkspaceContext = createContext<WorkspaceSource | null>(null);
const NO_AVAILABILITY = Atom.make<AsyncResult.AsyncResult<WorkspaceAvailability, never>>(AsyncResult.initial());

export function useWorkspaceSource() {
  return use(WorkspaceContext);
}

export function useWorkspaceAvailability(override?: WorkspaceSource | null) {
  const inherited = use(WorkspaceContext);
  const source = override === undefined ? inherited : override;
  const result = useAtomValue(source === null ? NO_AVAILABILITY : shellEnvironment.workspaceAvailability({
    environmentId: source.environmentId, input: { threadId: source.threadId },
  }));
  return source === null ? null : Option.getOrNull(AsyncResult.value(result));
}

export function useScopedWorkspaceQuery<A, E>(atom: Atom.Atom<AsyncResult.AsyncResult<A, E>> | null) {
  const source = use(WorkspaceContext);
  const availability = useWorkspaceAvailability();
  return source === null ? atom : scopeWorkspaceQuery({ atom, environmentId: source.environmentId, threadId: source.threadId, root: source.root, availability });
}

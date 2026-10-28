import { createContext, use, useContext, type ReactNode } from "react";
import { RegistryContext, useAtomValue } from "@effect/atom-react";
import type { EnvironmentId, ThreadId, WorkspaceAvailability } from "@t3tools/contracts";
import { scopeWorkspaceQuery } from "@t3tools/client-runtime/state/runtime";
import { resumeWorkspaceForAction } from "@t3tools/client-runtime/state/shell";
import { AsyncResult, Atom } from "effect/reactivity";
import * as Option from "effect/Option";
import { shellEnvironment } from "./shell";

export interface WorkspaceSource {
  readonly environmentId: EnvironmentId;
  readonly threadId: ThreadId;
  readonly root: string;
}
export const WorkspaceContext = createContext<WorkspaceSource | null>(null);
const EMPTY = Atom.make<AsyncResult.AsyncResult<WorkspaceAvailability, never>>(
  AsyncResult.initial(),
);
export function useWorkspaceSource() {
  return use(WorkspaceContext);
}
export function useWorkspaceAvailability() {
  const source = useWorkspaceSource();
  const value = useAtomValue(
    source === null
      ? EMPTY
      : shellEnvironment.workspaceAvailability({
          environmentId: source.environmentId,
          input: { threadId: source.threadId },
        }),
  );
  return source === null ? null : Option.getOrNull(AsyncResult.value(value));
}
export function useScopedWorkspaceQuery<A, E>(
  atom: Atom.Atom<AsyncResult.AsyncResult<A, E>> | null,
) {
  const source = useWorkspaceSource();
  const availability = useWorkspaceAvailability();
  return source === null
    ? atom
    : scopeWorkspaceQuery({
        atom,
        environmentId: source.environmentId,
        threadId: source.threadId,
        root: source.root,
        availability,
      });
}
export function WorkspaceProvider({
  source,
  children,
}: {
  readonly source: WorkspaceSource | null;
  readonly children: ReactNode;
}) {
  return <WorkspaceContext value={source}>{children}</WorkspaceContext>;
}

/** Returns a callback that resumes this view's returned workspace before an explicit action. */
export function useResumeWorkspace() {
  const registry = useContext(RegistryContext);
  const source = use(WorkspaceContext);
  const availability = useWorkspaceAvailability();
  return () =>
    source === null
      ? Promise.resolve(true)
      : resumeWorkspaceForAction({
          availability,
          prepare: () =>
            shellEnvironment.prepareWorkspaceAction.run(registry, {
              environmentId: source.environmentId,
              input: { threadId: source.threadId },
            }),
          complete: (reservationId) =>
            shellEnvironment.completeWorkspaceAction.run(registry, {
              environmentId: source.environmentId,
              input: { reservationId },
            }),
        });
}

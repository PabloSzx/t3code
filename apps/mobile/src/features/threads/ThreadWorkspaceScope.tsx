import { useMemo, type ComponentType, type ReactNode } from "react";
import { useThreadSelection } from "../../state/use-thread-selection";
import { useSelectedThreadWorktreePath } from "../../state/use-thread-detail";
import { WorkspaceProvider } from "../../state/workspace-access";
import { resolvePreferredThreadWorktreePath } from "../terminal/terminalLaunchContext";

export function ThreadWorkspaceScope({ children }: { readonly children: ReactNode }) {
  const { selectedThread, selectedThreadProject } = useThreadSelection();
  const detail = useSelectedThreadWorktreePath();
  const root =
    resolvePreferredThreadWorktreePath({
      threadShellWorktreePath: selectedThread?.worktreePath ?? null,
      threadDetailWorktreePath: detail,
    }) ?? selectedThreadProject?.workspaceRoot;
  const environmentId = selectedThread?.environmentId;
  const threadId = selectedThread?.id;
  const source = useMemo(
    () =>
      environmentId !== undefined && threadId !== undefined && root !== undefined
        ? { environmentId, threadId, root }
        : null,
    [environmentId, threadId, root],
  );
  return <WorkspaceProvider source={source}>{children}</WorkspaceProvider>;
}

export function withThreadWorkspace<Props extends object>(Content: ComponentType<Props>) {
  return function ThreadWorkspaceScreen(props: Props) {
    return (
      <ThreadWorkspaceScope>
        <Content {...props} />
      </ThreadWorkspaceScope>
    );
  };
}

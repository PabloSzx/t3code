import { useMemo } from "react";

import { useSelectedThreadWorktreePath } from "./use-thread-detail";
import { useThreadSelection } from "./use-thread-selection";
import { useWorkspaceAvailability, useWorkspaceSource } from "./workspace-access";
import { resolvePreferredThreadWorktreePath } from "../features/terminal/terminalLaunchContext";

export function useSelectedThreadWorktree() {
  const { selectedThread, selectedThreadProject } = useThreadSelection();
  const detailWorktreePath = useSelectedThreadWorktreePath();
  const source = useWorkspaceSource();
  const availability = useWorkspaceAvailability();

  const selectedThreadWorktreePath = useMemo(
    () =>
      resolvePreferredThreadWorktreePath({
        threadShellWorktreePath: selectedThread?.worktreePath ?? null,
        threadDetailWorktreePath: detailWorktreePath,
      }),
    [detailWorktreePath, selectedThread?.worktreePath],
  );
  // A returnable worktree's recorded path is only a recovery hint: never fall back to the
  // project root for it, and expose it only while availability confirms it.
  const guarded =
    selectedThreadWorktreePath !== null &&
    source !== null &&
    selectedThread !== null &&
    source.environmentId === selectedThread.environmentId &&
    source.threadId === selectedThread.id;
  const usable = !guarded || availability?.cwd != null;

  return {
    selectedThreadWorktreePath: usable ? selectedThreadWorktreePath : null,
    selectedThreadCwd: usable
      ? (selectedThreadWorktreePath ?? selectedThreadProject?.workspaceRoot ?? null)
      : null,
  };
}

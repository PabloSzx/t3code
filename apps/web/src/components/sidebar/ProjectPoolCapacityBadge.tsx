import { useAtomValue } from "@effect/atom-react";
import { AsyncResult } from "effect/reactivity";
import type { SidebarProjectSnapshot } from "../../sidebarProjectGrouping";
import { serverEnvironment } from "../../state/server";
import { vcsEnvironment } from "../../state/vcs";
import { Tooltip, TooltipPopup, TooltipTrigger } from "../ui/tooltip";

type Project = Pick<SidebarProjectSnapshot, "environmentId" | "workspaceRoot" | "memberProjects">;

export function ProjectPoolCapacityBadge({ project }: { project: Project }) {
  const config = useAtomValue(serverEnvironment.configValueAtom(project.environmentId));
  if (
    config?.environment.capabilities.treehousePoolCapacity !== true ||
    !config.settings.treehouseWorktreesEnabled
  )
    return null;
  return <PoolCapacity project={project} />;
}

function PoolCapacity({ project }: { project: Project }) {
  const result = useAtomValue(
    vcsEnvironment.poolCapacity({
      environmentId: project.environmentId,
      input: { cwd: project.workspaceRoot },
    }),
  );
  const capacity = AsyncResult.isSuccess(result) ? result.value : null;
  if (AsyncResult.isSuccess(result) && capacity === null) return null;
  const environment = project.memberProjects.find(
    (member) =>
      member.environmentId === project.environmentId &&
      member.workspaceRoot === project.workspaceRoot,
  )?.environmentLabel;
  const description = capacity
    ? `${capacity.leased} of ${capacity.max_trees} worktrees leased. ${capacity.unleased} unleased checkouts, not verified idle. ${capacity.unallocated} slots not yet allocated.`
    : AsyncResult.isFailure(result)
      ? "Worktree capacity unavailable. Check the host's Treehouse CLI supports capacity --json."
      : "Reading worktree capacity.";
  return (
    <Tooltip>
      <TooltipTrigger
        render={
          <span
            role="img"
            aria-label={description}
            className="shrink-0 text-3xs tabular-nums text-sidebar-muted-foreground"
          />
        }
      >
        {capacity ? `${capacity.leased}/${capacity.max_trees}` : "?/?"}
      </TooltipTrigger>
      <TooltipPopup>
        <div className="flex max-w-72 flex-col gap-1">
          <span>{description}</span>
          <span className="text-xs opacity-75">
            {environment ? `${environment} · ` : ""}
            {project.workspaceRoot}
          </span>
          {capacity ? <span className="text-xs opacity-75">Pool: {capacity.pool}</span> : null}
        </div>
      </TooltipPopup>
    </Tooltip>
  );
}

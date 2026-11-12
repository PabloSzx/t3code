import {
  WorktreeMcpFailure,
  OrchestratorMcpEnvironmentTarget,
  OrchestratorMcpFailure,
  VcsListRefsInput,
  VcsListRefsResult,
  WorktreeMcpHandoffInput,
  WorktreeMcpHandoffResult,
  WorktreeMcpStatusResult,
  WorktreeMcpReconcileBranchInput,
  WorktreeMcpReconcileBranchResult,
  ThreadId,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as ProjectService from "../../../project/ProjectService.ts";
import * as ThreadManagementService from "../../../orchestration-v2/ThreadManagementService.ts";
import { Tool, Toolkit } from "effect/ai";

import * as McpInvocationContext from "../../McpInvocationContext.ts";
import * as WorktreeMcpService from "../../WorktreeMcpService.ts";
import * as ProcessRunner from "../../../processRunner.ts";

const dependencies = [
  McpInvocationContext.McpInvocationContext,
  ThreadManagementService.ThreadManagementService,
  WorktreeMcpService.WorktreeMcpService,
];

const WorktreeToolFailure = Schema.Union([WorktreeMcpFailure, OrchestratorMcpFailure]);

const WorktreeHandoffTool = Tool.make("t3_worktree_handoff", {
  description:
    "Change THIS thread's checkout, preserving its conversation. Create a new worktree on branch, or attach existingWorktreePath with its actual branch. Read t3_worktree_status first and pass expectedWorktreePath when replacing a checkout. Old files, edits and branches stay untouched; uncommitted edits do not move. Foreign leases and other threads' checkouts are refused. Call last: the live session ends. continuationPrompt resumes this conversation there next turn; omitted, it waits. Do not create a new conversation just to change checkout.",
  parameters: WorktreeMcpHandoffInput,
  success: WorktreeMcpHandoffResult,
  failure: WorktreeToolFailure,
  failureMode: "return",
  dependencies,
})
  .annotate(Tool.Title, "Hand off thread to a git worktree")
  .annotate(Tool.Readonly, false)
  .annotate(Tool.Destructive, true)
  .annotate(Tool.Idempotent, false)
  .annotate(Tool.OpenWorld, true);

const WorktreeReconcileBranchTool = Tool.make("t3_worktree_reconcile_branch", {
  description:
    "Reconcile a user-approved, inspected Treehouse root and its settled shared subagents to the actual Git branch. Metadata only; no checkout, reset, slot return or session stop. Inspect the full family, branch/HEAD and lease first. Identity or activity changes block requests. Failure may leave partial metadata: inspect before retrying. Normal cleanup guards remain.",
  parameters: Schema.Struct({
    environmentId: OrchestratorMcpEnvironmentTarget,
    ...WorktreeMcpReconcileBranchInput.fields,
  }),
  success: WorktreeMcpReconcileBranchResult,
  failure: WorktreeToolFailure,
  failureMode: "return",
  dependencies: [...dependencies, ProcessRunner.ProcessRunner],
})
  .annotate(Tool.Readonly, false)
  .annotate(Tool.Destructive, true);

const WorktreeStatusTool = Tool.make("t3_worktree_status", {
  description:
    "Read this thread's saved checkout binding, even if its directory is missing: attached, path, branch, project root, and startFromOrigin default. This is metadata, not proof the checkout is usable. Check before t3_worktree_handoff.",
  // No `parameters`: Tool.make defaults to Tool.EmptyParams, which serializes
  // to a top-level `type: "object"` JSON Schema. An explicit empty
  // Schema.Struct({}) serializes to `anyOf: [object, array]`, which is not a
  // valid MCP tool input schema and makes clients reject the whole server.
  success: WorktreeMcpStatusResult,
  failure: WorktreeToolFailure,
  failureMode: "return",
  dependencies,
})
  .annotate(Tool.Title, "Get thread worktree status")
  .annotate(Tool.Readonly, true)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, true)
  .annotate(Tool.OpenWorld, false);

const WorktreeListTool = Tool.make("t3_worktree_list", {
  description:
    "List branch refs and checkout paths for a thread's workspace. Omit `threadId` for this thread. Detached worktrees with no branch are left out.",
  parameters: Schema.Struct({
    environmentId: OrchestratorMcpEnvironmentTarget,
    threadId: Schema.optional(ThreadId),
    query: VcsListRefsInput.fields.query,
    cursor: VcsListRefsInput.fields.cursor,
    limit: VcsListRefsInput.fields.limit,
    refKind: VcsListRefsInput.fields.refKind,
    includeMatchingRemoteRefs: VcsListRefsInput.fields.includeMatchingRemoteRefs,
  }),
  success: VcsListRefsResult,
  failure: OrchestratorMcpFailure,
  failureMode: "return",
  dependencies: [
    McpInvocationContext.McpInvocationContext,
    ThreadManagementService.ThreadManagementService,
    ProjectService.ProjectService,
    WorktreeMcpService.WorktreeMcpService,
    FileSystem.FileSystem,
    Path.Path,
  ],
})
  .annotate(Tool.Readonly, true)
  .annotate(Tool.Destructive, false);
export const WorktreeToolkit = Toolkit.make(
  WorktreeHandoffTool,
  WorktreeReconcileBranchTool,
  WorktreeStatusTool,
  WorktreeListTool,
);

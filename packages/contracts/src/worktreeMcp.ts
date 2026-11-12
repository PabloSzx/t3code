import * as Schema from "effect/Schema";

import { ThreadId, TrimmedNonEmptyString } from "./baseSchemas.ts";

/** Uses the managed worktree folder; handoff detaches the provider, ending this turn and resuming there next run. */
export const WorktreeMcpHandoffInput = Schema.Struct({
  branch: TrimmedNonEmptyString,
  expectedWorktreePath: Schema.optional(
    Schema.NullOr(TrimmedNonEmptyString).annotate({
      description:
        "Current saved path from t3_worktree_status. Required to replace an attached checkout. Null: project root.",
    }),
  ),
  existingWorktreePath: Schema.optional(
    TrimmedNonEmptyString.annotate({
      description:
        "Attach this existing project checkout without switching its Git branch. branch must match. Omit baseRef and startFromOrigin. Local edits are not copied or discarded.",
    }),
  ),
  baseRef: Schema.optional(
    TrimmedNonEmptyString.annotate({
      description:
        "New checkout only. Default: this thread's saved branch, else the project branch.",
    }),
  ),
  startFromOrigin: Schema.optional(
    Schema.Boolean.annotate({
      description:
        "True: fetch, start from the remote ref; check `startedFromOrigin`. Default: server setting.",
    }),
  ),
  runSetupScript: Schema.optional(
    Schema.Boolean.annotate({
      description: "Default true for a new checkout, false when attaching an existing one.",
    }),
  ),
  continuationPrompt: Schema.optional(
    TrimmedNonEmptyString.check(Schema.isMaxLength(120_000)).annotate({
      description: "Work to resume in the worktree next turn. Omitted: thread waits.",
    }),
  ),
});
export type WorktreeMcpHandoffInput = typeof WorktreeMcpHandoffInput.Type;

export const WorktreeMcpSetupScriptStatus = Schema.Union([
  Schema.Struct({
    status: Schema.Literal("started"),
    scriptName: TrimmedNonEmptyString,
    terminalId: TrimmedNonEmptyString,
  }),
  Schema.Struct({
    status: Schema.Literal("no-script"),
  }),
  Schema.Struct({
    status: Schema.Literal("skipped"),
  }),
  Schema.Struct({
    status: Schema.Literal("failed"),
    detail: Schema.String,
  }),
]);
export type WorktreeMcpSetupScriptStatus = typeof WorktreeMcpSetupScriptStatus.Type;

export const WorktreeMcpContinuationStatus = Schema.Union([
  Schema.Struct({
    status: Schema.Literal("scheduled"),
    delivery: Schema.Literals(["started", "queued", "steered", "restarted"]),
  }),
  Schema.Struct({
    status: Schema.Literal("skipped"),
  }),
  Schema.Struct({
    status: Schema.Literal("failed"),
    detail: Schema.String,
  }),
]);
export type WorktreeMcpContinuationStatus = typeof WorktreeMcpContinuationStatus.Type;

export const WorktreeMcpHandoffResult = Schema.Struct({
  worktreePath: TrimmedNonEmptyString,
  branch: TrimmedNonEmptyString,
  baseRef: TrimmedNonEmptyString,
  startedFromOrigin: Schema.Boolean,
  setupScript: WorktreeMcpSetupScriptStatus,
  continuation: WorktreeMcpContinuationStatus,
  note: Schema.String,
});
export type WorktreeMcpHandoffResult = typeof WorktreeMcpHandoffResult.Type;

export const WorktreeMcpStatusResult = Schema.Struct({
  attached: Schema.Boolean,
  worktreePath: Schema.NullOr(TrimmedNonEmptyString),
  branch: Schema.NullOr(TrimmedNonEmptyString),
  projectWorkspaceRoot: TrimmedNonEmptyString,
  defaultStartFromOrigin: Schema.Boolean.annotate({
    description: "`startFromOrigin` default for `t3_worktree_handoff`.",
  }),
});
export type WorktreeMcpStatusResult = typeof WorktreeMcpStatusResult.Type;

export const WorktreeMcpReconcileBranchInput = Schema.Struct({
  threadId: ThreadId,
  expected: Schema.Struct({
    worktreePath: TrimmedNonEmptyString,
    recordedBranch: TrimmedNonEmptyString,
    branch: TrimmedNonEmptyString,
    headSha: Schema.String.check(Schema.isPattern(/^(?:[a-f0-9]{40}|[a-f0-9]{64})$/)),
    leaseId: Schema.String.check(Schema.isPattern(/^[a-f0-9]{32}$/)),
    threadIds: Schema.Array(ThreadId).check(Schema.isMinLength(1), Schema.isMaxLength(100)),
  }),
});
export type WorktreeMcpReconcileBranchInput = typeof WorktreeMcpReconcileBranchInput.Type;

export const WorktreeMcpReconcileBranchResult = Schema.Struct({
  worktreePath: TrimmedNonEmptyString,
  branch: TrimmedNonEmptyString,
  headSha: Schema.String,
  threadIds: Schema.Array(ThreadId),
  updatedThreadIds: Schema.Array(ThreadId),
});
export type WorktreeMcpReconcileBranchResult = typeof WorktreeMcpReconcileBranchResult.Type;

export class WorktreeMcpFailure extends Schema.TaggedError<WorktreeMcpFailure>()(
  "WorktreeMcpFailure",
  {
    code: Schema.Literals([
      "capability_denied",
      "thread_not_found",
      "project_not_found",
      "already_in_worktree",
      "handoff_in_progress",
      "invalid_request",
      "operation_failed",
      "thread_credential_required",
    ]),
    message: Schema.String,
  },
) {}

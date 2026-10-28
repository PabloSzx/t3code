import * as Context from "effect/Context";
import * as Schema from "effect/Schema";

import { ThreadId } from "./baseSchemas.ts";
import { LaunchEditorInput } from "./editor.ts";

export const WORKSPACE_OPERATION_HEADER = "x-t3-workspace-operation";
export const WorkspaceOperationContext = Schema.Struct({
  threadId: ThreadId,
  generation: Schema.NullOr(Schema.String.check(Schema.isPattern(/^[a-f0-9]{32}$/))),
});
export type WorkspaceOperationContext = typeof WorkspaceOperationContext.Type;

export const ActiveWorkspace = Schema.Struct({
  state: Schema.Literals(["native", "active"]),
  cwd: Schema.String,
  context: WorkspaceOperationContext,
});
export type ActiveWorkspace = typeof ActiveWorkspace.Type;

export const WorkspaceAvailability = Schema.Union([
  ActiveWorkspace,
  Schema.Struct({ state: Schema.Literals(["released", "unavailable"]), cwd: Schema.Null }),
]);
export type WorkspaceAvailability = typeof WorkspaceAvailability.Type;
export const WorkspaceAvailabilityInput = Schema.Struct({ threadId: ThreadId });
export const WorkspaceReservation = Schema.Struct({
  workspace: ActiveWorkspace,
  reservationId: Schema.NullOr(Schema.String),
});
export type WorkspaceReservation = typeof WorkspaceReservation.Type;
export const WorkspaceCompleteActionInput = Schema.Struct({ reservationId: Schema.String });
export const WorkspaceEditorInput = Schema.Struct({
  ...LaunchEditorInput.fields,
  activate: Schema.optional(Schema.Boolean),
  workspace: Schema.optional(Schema.Struct({ threadId: ThreadId, root: Schema.String })),
  remote: Schema.optional(Schema.Struct({
    host: Schema.String.check(Schema.isMinLength(1)),
    username: Schema.optional(Schema.String),
    isFile: Schema.optional(Schema.Boolean),
  })),
});
export type WorkspaceEditorInput = typeof WorkspaceEditorInput.Type;
export const WorkspaceEditorResult = Schema.Struct({
  url: Schema.NullOr(Schema.String),
  reservationId: Schema.NullOr(Schema.String),
});
export type WorkspaceEditorResult = typeof WorkspaceEditorResult.Type;

// Captured per operation, never inferred from a connection's currently selected thread.
export class WorkspaceOperation extends Context.Reference<WorkspaceOperationContext | undefined>(
  "t3/workspace/WorkspaceOperation",
  { defaultValue: () => undefined },
) {}

export class WorkspaceUnavailableError extends Schema.TaggedError<WorkspaceUnavailableError>()(
  "WorkspaceUnavailableError",
  {
    reason: Schema.Literals(["update-required", "unavailable"]),
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return this.reason === "update-required"
      ? "This pooled workspace requires an updated client with conversation workspace identity."
      : "This workspace generation is unavailable. Keep unsaved text and explicitly resume the workspace before retrying.";
  }
}

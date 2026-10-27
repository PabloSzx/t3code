import * as Context from "effect/Context";
import * as Schema from "effect/Schema";

import { ThreadId } from "./baseSchemas.ts";

export const WORKSPACE_OPERATION_HEADER = "x-t3-workspace-operation";
export const WorkspaceOperationContext = Schema.Struct({
  threadId: ThreadId,
  generation: Schema.NullOr(Schema.String.check(Schema.isPattern(/^[a-f0-9]{32}$/))),
});
export type WorkspaceOperationContext = typeof WorkspaceOperationContext.Type;

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

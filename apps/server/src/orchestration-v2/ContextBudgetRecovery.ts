import {
  CommandId,
  IDLE_COMPACTION_TEXT,
  MessageId,
  isCompactionCommandMessage,
  type OrchestrationV2Command,
  type OrchestrationV2ThreadProjection,
  type RunId,
} from "@t3tools/contracts";
import { CONTEXT_HANDOFF_BUDGET_FAILURE_CODE } from "@t3tools/provider-core/server/failure";
import { latestRootProviderFailure } from "@t3tools/shared/orchestrationV2ThreadError";

export type ContextBudgetRecoveryRecords = Pick<
  OrchestrationV2ThreadProjection,
  "thread" | "runs" | "messages" | "turnItems" | "runtimeRequests"
>;

const COMPACT_PREFIX = "context-budget-compact:";
const RESEND_PREFIX = "context-budget-resend:";

const latestRun = (runs: ContextBudgetRecoveryRecords["runs"]) =>
  runs.reduce<(typeof runs)[number] | null>(
    (latest, run) => (latest === null || run.ordinal > latest.ordinal ? run : latest),
    null,
  );

/**
 * Next recovery step after a turn whose missed history did not fit: a T3 `/compact` turn, then
 * one resend of the original message. Ids derive from the failed run, so each step dispatches
 * at most once, and a resent message never starts another recovery.
 */
export function contextBudgetRecoveryCommand(
  records: ContextBudgetRecoveryRecords,
): OrchestrationV2Command | null {
  const { thread, runs, messages, turnItems, runtimeRequests } = records;
  if (thread.deletedAt !== null || thread.archivedAt !== null) return null;
  if (runtimeRequests.some((request) => request.status === "pending")) return null;
  const run = latestRun(runs);
  if (run === null) return null;
  const message = messages.find((candidate) => candidate.id === run.userMessageId);
  if (message === undefined) return null;

  if (run.status === "failed") {
    const failure = latestRootProviderFailure(run, turnItems);
    if (failure?.code !== CONTEXT_HANDOFF_BUDGET_FAILURE_CODE) return null;
    // One recovery per message: a resent message that still does not fit keeps its error.
    if (message.id.startsWith(RESEND_PREFIX) || isCompactionCommandMessage(message)) return null;
    return {
      type: "message.dispatch",
      commandId: CommandId.make(`${COMPACT_PREFIX}${run.id}`),
      messageId: MessageId.make(`${COMPACT_PREFIX}${run.id}`),
      threadId: thread.id,
      text: IDLE_COMPACTION_TEXT,
      attachments: [],
      createdBy: "agent",
      creationSource: "server",
      idleCompaction: "onReturn",
      dispatchMode: { type: "start_immediately" },
    };
  }

  if (run.status === "completed" && message.id.startsWith(COMPACT_PREFIX)) {
    const failedRunId = message.id.slice(COMPACT_PREFIX.length) as RunId;
    const failedRun = runs.find((candidate) => candidate.id === failedRunId);
    const original = messages.find((candidate) => candidate.id === failedRun?.userMessageId);
    // Only directly after the recovery compaction: anything else in between takes over.
    if (failedRun === undefined || original === undefined) return null;
    if (failedRun.ordinal !== run.ordinal - 1) return null;
    return {
      type: "message.dispatch",
      commandId: CommandId.make(`${RESEND_PREFIX}${failedRun.id}`),
      messageId: MessageId.make(`${RESEND_PREFIX}${failedRun.id}`),
      threadId: thread.id,
      text: original.text,
      ...(original.context === undefined ? {} : { context: original.context }),
      attachments: original.attachments,
      createdBy: original.createdBy,
      creationSource: "server",
      modelSelection: failedRun.modelSelection,
      dispatchMode: { type: "start_immediately" },
    };
  }
  return null;
}

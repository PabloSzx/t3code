import { describe, expect, it } from "@effect/vitest";
import {
  MessageId,
  NodeId,
  ProjectId,
  ProviderInstanceId,
  RunId,
  ThreadId,
  TurnItemId,
  type OrchestrationV2ConversationMessage,
  type OrchestrationV2Run,
  type OrchestrationV2TurnItem,
} from "@t3tools/contracts";
import { CONTEXT_HANDOFF_BUDGET_FAILURE_CODE } from "@t3tools/provider-core/server/failure";
import * as DateTime from "effect/DateTime";

import {
  contextBudgetRecoveryCommand,
  type ContextBudgetRecoveryRecords,
} from "./ContextBudgetRecovery.ts";

const now = DateTime.makeUnsafe(Date.parse("2026-10-28T12:00:00.000Z"));
const threadId = ThreadId.make("budget-thread");
const instanceId = ProviderInstanceId.make("pi");
const modelSelection = { instanceId, model: "openai-codex/gpt-6.1-sol" };

function run(
  ordinal: number,
  messageId: string,
  status: OrchestrationV2Run["status"],
): OrchestrationV2Run {
  return {
    id: RunId.make(`run-${ordinal}`),
    threadId,
    ordinal,
    providerInstanceId: instanceId,
    modelSelection,
    providerThreadId: null,
    userMessageId: MessageId.make(messageId),
    rootNodeId: NodeId.make(`node-${ordinal}`),
    activeAttemptId: null,
    status,
    requestedAt: now,
    startedAt: now,
    completedAt: now,
    checkpointId: null,
    contextHandoffId: null,
  };
}

function message(id: string, text: string, runId: string): OrchestrationV2ConversationMessage {
  return {
    id: MessageId.make(id),
    threadId,
    runId: RunId.make(runId),
    nodeId: null,
    role: "user",
    text,
    attachments: [],
    streaming: false,
    createdBy: "user",
    creationSource: "web",
    createdAt: now,
    updatedAt: now,
  };
}

function failure(forRun: OrchestrationV2Run, code: string | null): OrchestrationV2TurnItem {
  return {
    id: TurnItemId.make(`error-${forRun.id}`),
    type: "error",
    threadId,
    runId: forRun.id,
    nodeId: forRun.rootNodeId,
    providerThreadId: null,
    providerTurnId: null,
    nativeItemRef: null,
    parentItemId: null,
    ordinal: 1,
    status: "failed",
    title: "Provider failure",
    startedAt: now,
    completedAt: now,
    updatedAt: now,
    failure: { class: "provider_error", message: "Insufficient context.", code, retryable: null },
  } as OrchestrationV2TurnItem;
}

function records(input: {
  readonly runs: ReadonlyArray<OrchestrationV2Run>;
  readonly messages: ReadonlyArray<OrchestrationV2ConversationMessage>;
  readonly turnItems?: ReadonlyArray<OrchestrationV2TurnItem>;
}): ContextBudgetRecoveryRecords {
  return {
    thread: {
      id: threadId,
      projectId: ProjectId.make("project"),
      deletedAt: null,
      archivedAt: null,
    },
    runs: input.runs,
    messages: input.messages,
    turnItems: input.turnItems ?? [],
    runtimeRequests: [],
  } as unknown as ContextBudgetRecoveryRecords;
}

describe("contextBudgetRecoveryCommand", () => {
  const failed = run(1, "user-message", "failed");
  const original = message("user-message", "Explain the deploy preview", "run-1");

  it("compacts after a recoverable budget failure, then resends the original message once", () => {
    const compact = contextBudgetRecoveryCommand(
      records({
        runs: [failed],
        messages: [original],
        turnItems: [failure(failed, CONTEXT_HANDOFF_BUDGET_FAILURE_CODE)],
      }),
    );
    expect(compact).toMatchObject({
      type: "message.dispatch",
      text: "/compact",
      createdBy: "agent",
      creationSource: "server",
      idleCompaction: "onReturn",
      contextBudgetRecoveryOfRunId: failed.id,
    });
    if (compact?.type !== "message.dispatch") throw new Error("expected a compaction dispatch");

    const compacted = run(2, compact.messageId, "completed");
    const resend = contextBudgetRecoveryCommand(
      records({
        runs: [failed, compacted],
        messages: [original, message(compact.messageId, "/compact", "run-2")],
        turnItems: [failure(failed, CONTEXT_HANDOFF_BUDGET_FAILURE_CODE)],
      }),
    );
    expect(resend).toMatchObject({
      type: "message.dispatch",
      text: "Explain the deploy preview",
      // T3 sends the copy; the user's original message keeps their attribution.
      createdBy: "agent",
      creationSource: "server",
      modelSelection,
      contextBudgetRecoveryOfRunId: failed.id,
    });
    if (resend?.type !== "message.dispatch") throw new Error("expected a resend dispatch");

    // If the resent message still does not fit, it keeps its error instead of looping.
    const resentFailed = run(3, resend.messageId, "failed");
    expect(
      contextBudgetRecoveryCommand(
        records({
          runs: [failed, compacted, resentFailed],
          messages: [
            original,
            message(compact.messageId, "/compact", "run-2"),
            message(resend.messageId, "Explain the deploy preview", "run-3"),
          ],
          turnItems: [
            failure(failed, CONTEXT_HANDOFF_BUDGET_FAILURE_CODE),
            failure(resentFailed, CONTEXT_HANDOFF_BUDGET_FAILURE_CODE),
          ],
        }),
      ),
    ).toBeNull();
  });

  it("leaves settled threads alone", () => {
    const base = records({
      runs: [failed],
      messages: [original],
      turnItems: [failure(failed, CONTEXT_HANDOFF_BUDGET_FAILURE_CODE)],
    });
    expect(
      contextBudgetRecoveryCommand({
        ...base,
        thread: { ...base.thread, settledOverride: "settled" },
      }),
    ).toBeNull();
  });

  it("leaves other failures, and providers that cannot compact, to the user", () => {
    expect(
      contextBudgetRecoveryCommand(
        records({ runs: [failed], messages: [original], turnItems: [failure(failed, null)] }),
      ),
    ).toBeNull();
    expect(
      contextBudgetRecoveryCommand(
        records({
          runs: [failed],
          messages: [original],
          turnItems: [failure(failed, "provider_failed")],
        }),
      ),
    ).toBeNull();
  });

  it("does not resend after the user moved on, or when the compaction failed", () => {
    const compactId = "context-budget-compact:run-1";
    const compaction = message(compactId, "/compact", "run-2");
    const turnItems = [failure(failed, CONTEXT_HANDOFF_BUDGET_FAILURE_CODE)];
    expect(
      contextBudgetRecoveryCommand(
        records({
          runs: [failed, run(2, compactId, "failed")],
          messages: [original, compaction],
          turnItems,
        }),
      ),
    ).toBeNull();
    // A later turn that is not the recovery compaction does not trigger a resend.
    expect(
      contextBudgetRecoveryCommand(
        records({
          runs: [failed, run(2, compactId, "completed"), run(3, "next", "completed")],
          messages: [original, compaction, message("next", "Something else", "run-3")],
          turnItems,
        }),
      ),
    ).toBeNull();
  });
});

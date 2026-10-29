import { describe, expect, it } from "@effect/vitest";
import {
  MessageId,
  NodeId,
  ProviderInstanceId,
  RunId,
  ThreadId,
  TurnItemId,
  type OrchestrationV2ServerCommand,
} from "@t3tools/contracts";
import { CONTEXT_HANDOFF_BUDGET_FAILURE_CODE } from "@t3tools/provider-core/server/failure";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";

import * as ContextBudgetRecoveryService from "./ContextBudgetRecoveryService.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as ProjectionStore from "./ProjectionStore.ts";

const threadId = ThreadId.make("budget-service-thread");
const instanceId = ProviderInstanceId.make("pi");
const now = DateTime.makeUnsafe(Date.parse("2026-10-28T12:00:00.000Z"));

const run = (ordinal: number, messageId: string, status: "failed" | "completed") => ({
  id: RunId.make(`run-${ordinal}`),
  threadId,
  ordinal,
  providerInstanceId: instanceId,
  modelSelection: { instanceId, model: "m" },
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
});
const message = (id: string, text: string) => ({
  id: MessageId.make(id),
  threadId,
  runId: null,
  nodeId: null,
  role: "user" as const,
  text,
  attachments: [],
  streaming: false,
  createdBy: "user" as const,
  creationSource: "web" as const,
  createdAt: now,
  updatedAt: now,
});
const failedRun = run(1, "original", "failed");
const budgetFailure = {
  id: TurnItemId.make("failure"),
  type: "error" as const,
  threadId,
  runId: failedRun.id,
  nodeId: failedRun.rootNodeId,
  providerThreadId: null,
  providerTurnId: null,
  nativeItemRef: null,
  parentItemId: null,
  ordinal: 1,
  status: "failed" as const,
  title: "Provider failure",
  startedAt: now,
  completedAt: now,
  updatedAt: now,
  failure: {
    class: "provider_error" as const,
    message: "Insufficient context.",
    code: CONTEXT_HANDOFF_BUDGET_FAILURE_CODE,
    retryable: null,
  },
};
const compactId = `context-budget-compact:${failedRun.id}`;

function harness() {
  return Effect.gen(function* () {
    const dispatched = yield* Queue.unbounded<OrchestrationV2ServerCommand>();
    const records = () => ({
      thread: { id: threadId, deletedAt: null, archivedAt: null, settledOverride: null },
      runs: [failedRun, run(2, compactId, "completed")],
      messages: [message("original", "Explain"), message(compactId, "/compact")],
      turnItems: [budgetFailure],
      runtimeRequests: [],
    });
    const layer = Layer.mergeAll(
      Layer.succeed(Orchestrator.OrchestratorV2, {
        streamDomainEvents: Stream.never,
        dispatch: (command: OrchestrationV2ServerCommand) =>
          Queue.offer(dispatched, command).pipe(Effect.as({ sequence: 0 } as never)),
      } as unknown as Orchestrator.OrchestratorV2["Service"]),
      Layer.succeed(ProjectionStore.ProjectionStoreV2, {
        getThreadRecords: () => Effect.sync(records),
        getContextBudgetRecoveryCandidates: Effect.succeed([threadId]),
      } as unknown as ProjectionStore.ProjectionStoreV2["Service"]),
    );
    return { dispatched, layer };
  });
}

describe("ContextBudgetRecoveryService", () => {
  it.effect("checks again when a request arrives while a check is still running", () =>
    Effect.gen(function* () {
      const entered = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      let runs = 0;
      const check = ContextBudgetRecoveryService.makeSerialCheck(() =>
        Effect.gen(function* () {
          runs += 1;
          if (runs === 1) {
            yield* Deferred.succeed(entered, undefined);
            yield* Deferred.await(release);
          }
        }),
      );
      const first = yield* check(threadId).pipe(Effect.forkChild);
      yield* Deferred.await(entered);
      // The compaction completes while the first check is still dispatching.
      yield* check(threadId);
      expect(runs).toBe(1);
      yield* Deferred.succeed(release, undefined);
      yield* Fiber.join(first);
      expect(runs).toBe(2);
    }),
  );

  it.effect("finishes a recovery step left over from before a restart", () =>
    Effect.gen(function* () {
      const h = yield* harness();
      yield* Effect.gen(function* () {
        const service = yield* ContextBudgetRecoveryService.make;
        yield* service.start();
        // No live events arrive: the startup scan alone finishes the pending resend.
        expect(yield* Queue.take(h.dispatched)).toMatchObject({ text: "Explain" });
      }).pipe(Effect.provide(h.layer), Effect.scoped);
    }),
  );
});

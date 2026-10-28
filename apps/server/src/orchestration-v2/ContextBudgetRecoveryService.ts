import * as Effect from "effect/Effect";
import * as FiberMap from "effect/FiberMap";
import * as Stream from "effect/Stream";
import type { ThreadId } from "@t3tools/contracts";

import { forkParked } from "../serverActivation.ts";
import { contextBudgetRecoveryCommand } from "./ContextBudgetRecovery.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as ProjectionStore from "./ProjectionStore.ts";

/**
 * When a turn fails because its missed history no longer fits, compacts the conversation as a
 * visible T3 Code turn and then sends the original message again, once.
 */
export const make = Effect.gen(function* () {
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const projections = yield* ProjectionStore.ProjectionStoreV2;

  const recover = Effect.fn("ContextBudgetRecoveryService.recover")(
    function* (threadId: ThreadId) {
      const records = yield* projections.getThreadRecords(threadId, [
        "runs",
        "messages",
        "runtimeRequests",
        "turnItems",
      ]);
      const command = contextBudgetRecoveryCommand(records);
      if (command !== null) yield* orchestrator.dispatch(command);
    },
    (effect, threadId) =>
      effect.pipe(
        Effect.catch((cause) =>
          Effect.logWarning("context budget recovery skipped", { threadId, cause }),
        ),
      ),
  );

  const start = Effect.fn("ContextBudgetRecoveryService.start")(function* () {
    // Keyed by thread so a burst of updates for one thread runs one check at a time.
    const checks = yield* FiberMap.make<ThreadId>();
    yield* forkParked(
      Stream.runForEach(orchestrator.streamDomainEvents, (event) =>
        event.type === "run.updated" &&
        (event.payload.status === "failed" || event.payload.status === "completed")
          ? FiberMap.run(checks, event.threadId, recover(event.threadId), { onlyIfMissing: true })
          : Effect.void,
      ),
    );
  });
  return { start };
});

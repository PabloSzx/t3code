import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import type { ThreadId } from "@t3tools/contracts";

import { forkParked } from "../serverActivation.ts";
import { contextBudgetRecoveryCommand } from "./ContextBudgetRecovery.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as ProjectionStore from "./ProjectionStore.ts";

/**
 * When a turn fails because its missed history no longer fits, compacts the conversation as a
 * visible T3 Code turn and then sends the original message again, once. Dispatch rechecks each
 * step under the thread lock, so user actions in between supersede it.
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

  const check = makeSerialCheck(recover);

  const start = Effect.fn("ContextBudgetRecoveryService.start")(function* () {
    yield* forkParked(
      Stream.runForEach(orchestrator.streamDomainEvents, (event) =>
        event.type === "run.updated" &&
        (event.payload.status === "failed" || event.payload.status === "completed")
          ? check(event.threadId).pipe(Effect.forkDetach, Effect.asVoid)
          : Effect.void,
      ),
    );
    // A restart can land between recovery steps; finish any step that is still due, without
    // holding up server startup.
    yield* projections.getContextBudgetRecoveryCandidates.pipe(
      Effect.catch((cause) =>
        Effect.logWarning("context budget recovery startup scan failed", { cause }).pipe(
          Effect.as([] as ReadonlyArray<ThreadId>),
        ),
      ),
      Effect.flatMap((pending) => Effect.forEach(pending, check, { discard: true })),
      Effect.forkDetach,
    );
  });
  return { start };
});

/**
 * Runs one check per thread at a time. A request that arrives during a check marks the thread
 * for another pass, so a state change that lands mid-check is never dropped.
 */
export function makeSerialCheck<E, R>(
  run: (threadId: ThreadId) => Effect.Effect<void, E, R>,
): (threadId: ThreadId) => Effect.Effect<void, E, R> {
  const running = new Set<ThreadId>();
  const dirty = new Set<ThreadId>();
  return (threadId) =>
    Effect.suspend(() => {
      if (running.has(threadId)) {
        dirty.add(threadId);
        return Effect.void;
      }
      running.add(threadId);
      const loop: Effect.Effect<void, E, R> = run(threadId).pipe(
        Effect.andThen(Effect.suspend(() => (dirty.delete(threadId) ? loop : Effect.void))),
      );
      return loop.pipe(Effect.ensuring(Effect.sync(() => running.delete(threadId))));
    });
}

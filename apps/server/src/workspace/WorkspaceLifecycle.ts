import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";

export class WorkspaceLifecycle extends Context.Service<
  WorkspaceLifecycle,
  {
    readonly invalidate: Effect.Effect<void>;
    readonly changes: Stream.Stream<number>;
  }
>()("t3/workspace/WorkspaceLifecycle") {}

export const layer = Layer.effect(
  WorkspaceLifecycle,
  Effect.gen(function* () {
    const revision = yield* SubscriptionRef.make(0);
    return WorkspaceLifecycle.of({
      invalidate: SubscriptionRef.update(revision, (value) => value + 1),
      changes: SubscriptionRef.changes(revision),
    });
  }),
);

// Registry-only fixtures need no subscriber; the application provides one shared lifecycle service.
export const invalidate = Effect.flatMap(Effect.serviceOption(WorkspaceLifecycle), (lifecycle) =>
  lifecycle._tag === "None" ? Effect.void : lifecycle.value.invalidate,
);

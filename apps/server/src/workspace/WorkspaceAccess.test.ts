import { ThreadId, WorkspaceOperation } from "@t3tools/contracts";
import { expect, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Result from "effect/Result";

import { fixture, testLayer } from "../vcs/TreehouseWorktrees.testkit.ts";
import * as TreehouseWorktrees from "../vcs/TreehouseWorktrees.ts";
import * as WorkspaceAccess from "./WorkspaceAccess.ts";

const activeWorkspace = Effect.fn("TestWorkspaceAccess.activeWorkspace")(function* () {
  const f = yield* fixture();
  yield* f.driver.createWorktree({
    cwd: f.cwd,
    refName: f.parentSha,
    newRefName: "access-owner",
    path: null,
  });
  const threadId = ThreadId.make("access-owner");
  yield* f.leases.bindThread({ path: f.slot, threadId, sequence: 1 });
  const generation = yield* f.leases.operationGeneration({ path: f.slot, threadId });
  const access = yield* WorkspaceAccess.WorkspaceAccess.pipe(Effect.provide(WorkspaceAccess.layer));
  return { ...f, access, context: { threadId, generation } };
});

it.live("admits active and native I/O but refuses missing, foreign and stale identities", () =>
  Effect.gen(function* () {
    const f = yield* activeWorkspace();
    const read = f.access.use({
      targets: [f.slot],
      effect: f.fs.readFileString(f.path.join(f.slot, "file")),
    });
    expect(yield* read.pipe(Effect.provideService(WorkspaceOperation, f.context))).toBe("parent\n");
    const targetRead = f.access.useThread({
      threadId: f.context.threadId,
      cwd: f.slot,
      targets: [],
      effect: f.fs.readFileString(f.path.join(f.slot, "file")),
    });
    expect(
      yield* targetRead.pipe(
        Effect.provideService(WorkspaceOperation, { ...f.context, generation: "0".repeat(32) }),
        Effect.result,
      ),
    ).toMatchObject({ _tag: "Failure", failure: { reason: "stale-binding" } });
    for (const context of [
      undefined,
      { ...f.context, threadId: ThreadId.make("foreign-thread") },
      { ...f.context, generation: "0".repeat(32) },
      { ...f.context, generation: null },
    ]) {
      const result = yield* read.pipe(
        Effect.provideService(WorkspaceOperation, context),
        Effect.result,
      );
      expect(Result.isFailure(result)).toBe(true);
      if (Result.isFailure(result))
        expect(result.failure).toMatchObject({ _tag: "TreehouseLeaseError" });
    }
    const nativeRead = f.access.use({
      targets: [f.cwd],
      effect: f.fs.readFileString(f.path.join(f.cwd, "file")),
    });
    expect(yield* nativeRead).toBe("parent\n");
    const alias = f.path.join(f.root, "lease-alias");
    yield* f.fs.makeDirectory(f.path.join(f.slot, "child"));
    yield* f.fs.symlink(f.path.join(f.slot, "child"), alias);
    const aliasRoot = `${alias}/..`;
    expect(
      yield* f.leases.operationGeneration({ path: aliasRoot, threadId: f.context.threadId }),
    ).toBe(f.context.generation);
    expect(
      yield* f.leases
        .operationGeneration({ path: aliasRoot, threadId: "foreign-thread" })
        .pipe(Effect.result),
    ).toMatchObject({ _tag: "Failure", failure: { reason: "stale-binding" } });
    expect(
      yield* nativeRead.pipe(
        Effect.provideService(WorkspaceOperation, { ...f.context, generation: null }),
        Effect.flip,
      ),
    ).toMatchObject({ reason: "stale-binding" });
    yield* f.leases.commitThread({
      threadId: f.context.threadId,
      path: null,
      deleted: false,
      sequence: 2,
    });
    expect(
      yield* f.leases.operationGeneration({ path: f.cwd, threadId: f.context.threadId }),
    ).toBeNull();
    expect(
      yield* nativeRead.pipe(
        Effect.provideService(WorkspaceOperation, { ...f.context, generation: null }),
      ),
    ).toBe("parent\n");
  }).pipe(Effect.provide(testLayer)),
);

it.live.each(["complete", "interrupt"] as const)(
  "protects admitted I/O against return and releases the pin on %s",
  (completion) =>
    Effect.gen(function* () {
      const f = yield* activeWorkspace();
      const admitted = yield* Deferred.make<void>();
      const continueWrite = yield* Deferred.make<void>();
      const operation = yield* f.access
        .use({
          targets: [f.slot],
          effect: Effect.gen(function* () {
            yield* Deferred.succeed(admitted, undefined);
            yield* Deferred.await(continueWrite);
            yield* f.fs.writeFileString(f.path.join(f.slot, "file"), "written by A\n");
          }),
        })
        .pipe(Effect.provideService(WorkspaceOperation, f.context), Effect.forkChild);
      yield* Deferred.await(admitted);
      const treehouse = yield* TreehouseWorktrees.TreehouseWorktrees.pipe(
        Effect.provide(TreehouseWorktrees.layer),
      );
      const returnIdle = treehouse.returnIdle({
        cwd: f.cwd,
        path: f.slot,
        branch: "access-owner",
        headSha: f.parentSha,
        threadIds: [f.context.threadId],
        recheck: Effect.succeed(null),
      });
      expect(yield* returnIdle.pipe(Effect.flip)).toMatchObject({
        _tag: "GitCommandError",
        cause: { _tag: "TreehouseLeaseError", reason: "in-use" },
      });
      expect(yield* f.fs.readFileString(f.path.join(f.slot, "file"))).toBe("parent\n");
      if (completion === "interrupt") {
        yield* Fiber.interrupt(operation);
      } else {
        yield* Deferred.succeed(continueWrite, undefined);
        yield* Fiber.join(operation);
        expect(yield* f.fs.readFileString(f.path.join(f.slot, "file"))).toBe("written by A\n");
        yield* f.git({ cwd: f.slot, args: ["restore", "file"] });
      }
      expect(yield* returnIdle).toBeNull();
      const denied = yield* f.access
        .use({ targets: [f.slot], effect: Effect.die("must not perform I/O") })
        .pipe(Effect.provideService(WorkspaceOperation, f.context), Effect.result);
      expect(Result.isFailure(denied)).toBe(true);
    }).pipe(Effect.provide(testLayer)),
);

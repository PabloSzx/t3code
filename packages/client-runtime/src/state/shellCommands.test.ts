import { EnvironmentId, ThreadId, WorkspaceUnavailableError, type WorkspaceReservation } from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import { AsyncResult } from "effect/reactivity";
import { expect, it } from "vite-plus/test";

import { runThreadWorkspaceCommand } from "./shellCommands.ts";

it("uses the resumed generation and keeps its reservation through the command", async () => {
  const environmentId = EnvironmentId.make("host");
  const threadId = ThreadId.make("conversation-a");
  const context = { threadId, generation: "c".repeat(32) };
  const reservation: WorkspaceReservation = { workspace: { state: "active", cwd: "/pool/new-a", context }, reservationId: "handoff" };
  const entered = Promise.withResolvers<void>();
  const finish = Promise.withResolvers<void>();
  let completed = false;
  const pending = runThreadWorkspaceCommand({
    target: { environmentId, input: { cwd: "/pool/recycled", relativePath: "file" } },
    source: { environmentId, threadId, root: "/pool/recycled" },
    availability: { state: "released", cwd: null },
    mode: "activate",
    prepare: async () => AsyncResult.success(reservation),
    run: async (target) => {
      entered.resolve();
      await finish.promise;
      expect(target).toMatchObject({ workspace: context });
      const contents = new Map([["/pool/recycled", "foreign B"], ["/pool/new-a", "retained A"]]);
      return AsyncResult.success(contents.get(target.input.cwd));
    },
    complete: async () => { completed = true; },
  });
  await entered.promise;
  expect(completed).toBe(false);
  finish.resolve();
  expect(await pending).toMatchObject({ _tag: "Success", value: "retained A" });
  expect(completed).toBe(true);
});

it("does not activate or replay a captured save after return", async () => {
  const environmentId = EnvironmentId.make("host");
  const threadId = ThreadId.make("conversation-a");
  const unexpected = async () => { throw new Error("A queued save must not activate or write"); };
  const result = await runThreadWorkspaceCommand({
    target: { environmentId, input: { cwd: "/pool/recycled", contents: "unsaved A" } },
    source: { environmentId, threadId, root: "/pool/recycled" },
    availability: { state: "released", cwd: null },
    mode: "captured", run: unexpected, prepare: unexpected, complete: unexpected,
  });
  expect(result._tag).toBe("Failure");
  if (result._tag === "Failure") expect(Cause.squash(result.cause)).toBeInstanceOf(WorkspaceUnavailableError);
});

it("does not move a command to another environment or activate native paths", async () => {
  const environmentId = EnvironmentId.make("host");
  const threadId = ThreadId.make("native-thread");
  const context = { threadId, generation: null };
  const unexpected = async () => { throw new Error("No pooled activation expected"); };
  const source = { environmentId, threadId, root: "/native" };
  const native = await runThreadWorkspaceCommand({
    target: { environmentId, input: { cwd: "/native" } }, source,
    availability: { state: "native", cwd: "/native", context }, mode: "activate",
    prepare: unexpected, complete: unexpected,
    run: async (target) => AsyncResult.success(target),
  });
  expect(native).toMatchObject({ _tag: "Success", value: { input: { cwd: "/native" }, workspace: context } });
  const foreign = await runThreadWorkspaceCommand({
    target: { environmentId: EnvironmentId.make("other"), input: { cwd: "/other" } }, source,
    availability: { state: "released", cwd: null }, mode: "activate",
    prepare: unexpected, complete: unexpected,
    run: async (target) => AsyncResult.success(target),
  });
  expect(foreign).toMatchObject({ _tag: "Success", value: { input: { cwd: "/other" } } });
});

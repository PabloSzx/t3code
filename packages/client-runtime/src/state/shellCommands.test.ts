import {
  EnvironmentId,
  ThreadId,
  WorkspaceUnavailableError,
  type WorkspaceReservation,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import { AsyncResult } from "effect/reactivity";
import { expect, it } from "vite-plus/test";

import { resumeWorkspaceForAction, runThreadWorkspaceCommand } from "./shellCommands.ts";

it("uses the resumed generation and keeps its reservation through the command", async () => {
  const environmentId = EnvironmentId.make("host");
  const threadId = ThreadId.make("conversation-a");
  const context = { threadId, generation: "c".repeat(32) };
  const reservation: WorkspaceReservation = {
    workspace: { state: "active", cwd: "/pool/new-a", context },
    reservationId: "handoff",
  };
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
      const contents = new Map([
        ["/pool/recycled", "foreign B"],
        ["/pool/new-a", "retained A"],
      ]);
      return AsyncResult.success(contents.get(target.input.cwd));
    },
    complete: async () => {
      completed = true;
    },
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
  const unexpected = async () => {
    throw new Error("A queued save must not activate or write");
  };
  const result = await runThreadWorkspaceCommand({
    target: { environmentId, input: { cwd: "/pool/recycled", contents: "unsaved A" } },
    source: { environmentId, threadId, root: "/pool/recycled" },
    availability: { state: "released", cwd: null },
    mode: "captured",
    run: unexpected,
    prepare: unexpected,
    complete: unexpected,
  });
  expect(result._tag).toBe("Failure");
  if (result._tag === "Failure")
    expect(Cause.squash(result.cause)).toBeInstanceOf(WorkspaceUnavailableError);
});

it("does not move a command to another environment or activate native paths", async () => {
  const environmentId = EnvironmentId.make("host");
  const threadId = ThreadId.make("native-thread");
  const context = { threadId, generation: null };
  const unexpected = async () => {
    throw new Error("No pooled activation expected");
  };
  const source = { environmentId, threadId, root: "/native" };
  const native = await runThreadWorkspaceCommand({
    target: { environmentId, input: { cwd: "/native" } },
    source,
    availability: { state: "native", cwd: "/native", context },
    mode: "activate",
    prepare: unexpected,
    complete: unexpected,
    run: async (target) => AsyncResult.success(target),
  });
  expect(native).toMatchObject({
    _tag: "Success",
    value: { input: { cwd: "/native" }, workspace: context },
  });
  const foreign = await runThreadWorkspaceCommand({
    target: { environmentId: EnvironmentId.make("other"), input: { cwd: "/other" } },
    source,
    availability: { state: "released", cwd: null },
    mode: "activate",
    prepare: unexpected,
    complete: unexpected,
    run: async (target) => AsyncResult.success(target),
  });
  expect(foreign).toMatchObject({ _tag: "Success", value: { input: { cwd: "/other" } } });
});

it("uses an active workspace without asking for activation authority", async () => {
  const environmentId = EnvironmentId.make("host");
  const threadId = ThreadId.make("active-pooled");
  const context = { threadId, generation: "d".repeat(32) };
  const unexpected = async () => {
    throw new Error("An active workspace must not need prepare");
  };
  const result = await runThreadWorkspaceCommand({
    target: { environmentId, input: { cwd: "/pool/a" } },
    source: { environmentId, threadId, root: "/pool/a" },
    availability: { state: "active", cwd: "/pool/a", context },
    mode: "activate",
    prepare: unexpected,
    complete: unexpected,
    run: async (target) => AsyncResult.success(target),
  });
  expect(result).toMatchObject({ _tag: "Success", value: { workspace: context } });
});

it("binds a closure-scoped action such as commit to the resumed workspace", async () => {
  const environmentId = EnvironmentId.make("host");
  const threadId = ThreadId.make("stacked-action");
  const context = { threadId, generation: "e".repeat(32) };
  let prepared = 0;
  const result = await runThreadWorkspaceCommand({
    target: { actionId: "commit-1", action: "commit" },
    source: { environmentId, threadId, root: "/pool/old" },
    availability: { state: "released", cwd: null },
    mode: "activate",
    binding: {
      environmentId,
      cwd: "/pool/old",
      bind: (input, workspace) => ({ ...input, workspace }),
    },
    prepare: async () => {
      prepared += 1;
      return AsyncResult.success({
        workspace: { state: "active", cwd: "/pool/new", context },
        reservationId: null,
      });
    },
    complete: async () => undefined,
    run: async (target) => AsyncResult.success(target),
  });
  expect(prepared).toBe(1);
  expect(result).toMatchObject({
    _tag: "Success",
    value: { actionId: "commit-1", workspace: { context, cwd: "/pool/new" } },
  });
});

it("resumes only a returned workspace before an explicit navigation", async () => {
  const threadId = ThreadId.make("navigating");
  let prepared = 0;
  const completed: string[] = [];
  const options = {
    prepare: async () => {
      prepared += 1;
      return AsyncResult.success({
        workspace: {
          state: "active" as const,
          cwd: "/pool/new",
          context: { threadId, generation: "f".repeat(32) },
        },
        reservationId: "resume",
      });
    },
    complete: async (id: string) => {
      completed.push(id);
    },
    resolveAvailability: async () => ({ state: "released" as const, cwd: null }),
  };
  expect(
    await resumeWorkspaceForAction({
      ...options,
      availability: {
        state: "active",
        cwd: "/pool/a",
        context: { threadId, generation: "a".repeat(32) },
      },
    }),
  ).toBe(true);
  expect(prepared).toBe(0);
  expect(
    await resumeWorkspaceForAction({ ...options, availability: { state: "released", cwd: null } }),
  ).toBe(true);
  expect(prepared).toBe(1);
  expect(completed).toEqual(["resume"]);
  // Unknown availability is read first, so a released workspace still resumes.
  expect(await resumeWorkspaceForAction({ ...options, availability: null })).toBe(true);
  expect(prepared).toBe(2);
});

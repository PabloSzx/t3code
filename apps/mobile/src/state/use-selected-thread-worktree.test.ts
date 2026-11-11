import { EnvironmentId, ThreadId, type WorkspaceAvailability } from "@t3tools/contracts";
import { describe, expect, it, vi } from "vite-plus/test";
import type { WorkspaceSource } from "./workspace-access";

const state = vi.hoisted(() => ({
  thread: null as {
    id: ThreadId;
    environmentId: EnvironmentId;
    worktreePath: string | null;
  } | null,
  project: null as { workspaceRoot: string } | null,
  detailWorktreePath: null as string | null,
  source: null as WorkspaceSource | null,
  availability: null as WorkspaceAvailability | null,
}));

vi.mock("./use-thread-selection", () => ({
  useThreadSelection: () => ({
    selectedThread: state.thread,
    selectedThreadProject: state.project,
  }),
}));
vi.mock("./use-thread-detail", () => ({
  useSelectedThreadWorktreePath: () => state.detailWorktreePath,
}));
vi.mock("./workspace-access", () => ({
  useWorkspaceSource: () => state.source,
  useWorkspaceAvailability: () => state.availability,
}));

import { useSelectedThreadWorktree } from "./use-selected-thread-worktree";

const environmentId = EnvironmentId.make("env");
const threadId = ThreadId.make("thread");
const worktreePath = "/repo/.worktrees/a";

function setup() {
  state.thread = { id: threadId, environmentId, worktreePath };
  state.project = { workspaceRoot: "/repo" };
  state.detailWorktreePath = null;
  state.source = { environmentId, threadId, root: worktreePath };
  state.availability = null;
}

describe("useSelectedThreadWorktree", () => {
  it.each([null, { state: "released", cwd: null }, { state: "unavailable", cwd: null }] as const)(
    "keeps unavailable workspace paths only for explicit recovery: %j",
    (availability) => {
      setup();
      state.availability = availability;
      expect(useSelectedThreadWorktree()).toEqual({
        recordedCwd: worktreePath,
        selectedThreadWorktreePath: null,
        selectedThreadCwd: null,
      });

      // A loaded detail can be the only record of the returned checkout.
      state.thread = { id: threadId, environmentId, worktreePath: null };
      state.detailWorktreePath = worktreePath;
      expect(useSelectedThreadWorktree()).toEqual({
        recordedCwd: worktreePath,
        selectedThreadWorktreePath: null,
        selectedThreadCwd: null,
      });
    },
  );

  it("exposes an available checkout and still supports native project workspaces", () => {
    setup();
    state.availability = {
      state: "active",
      cwd: worktreePath,
      context: { threadId, generation: "a".repeat(32) },
    };
    expect(useSelectedThreadWorktree()).toEqual({
      recordedCwd: worktreePath,
      selectedThreadWorktreePath: worktreePath,
      selectedThreadCwd: worktreePath,
    });

    state.thread = { id: threadId, environmentId, worktreePath: null };
    state.availability = null;
    expect(useSelectedThreadWorktree()).toEqual({
      recordedCwd: "/repo",
      selectedThreadWorktreePath: null,
      selectedThreadCwd: "/repo",
    });
  });

  it.each([
    { environmentId, threadId: ThreadId.make("another-thread"), root: worktreePath },
    { environmentId: EnvironmentId.make("another-env"), threadId, root: worktreePath },
    null,
  ])("does not hide another workspace's paths under an unrelated scope: %j", (source) => {
    setup();
    state.source = source;
    state.availability = { state: "released", cwd: null };
    expect(useSelectedThreadWorktree()).toEqual({
      recordedCwd: worktreePath,
      selectedThreadWorktreePath: worktreePath,
      selectedThreadCwd: worktreePath,
    });
  });
});

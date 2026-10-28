import type { ProjectReadFileResult } from "@t3tools/contracts";
import { EnvironmentId, ThreadId } from "@t3tools/contracts";
import { AsyncResult } from "effect/reactivity";
import { afterEach, describe, expect, it, vi } from "vite-plus/test";

const registryTasks = vi.hoisted(() => new Set<() => void>());

vi.mock("~/rpc/atomRegistry", async () => {
  const { AtomRegistry } = await import("effect/reactivity");
  return {
    appAtomRegistry: AtomRegistry.make({
      scheduleTask: (task) => {
        registryTasks.add(task);
        return () => {
          registryTasks.delete(task);
        };
      },
    }),
  };
});

import { appAtomRegistry } from "~/rpc/atomRegistry";
import { projectEnvironment } from "~/state/projects";
import { FileSaveCoordinator } from "./fileSaveCoordinator";
import {
  createProjectFileScope,
  clearProjectFileQueryData,
  confirmProjectFileQueryData,
  getOptimisticProjectFileQueryData,
  getUnsavedProjectFileQueryData,
  resolveProjectFileQueryData,
  setProjectFileQueryData,
} from "./projectFilesQueryState";

const environmentId = EnvironmentId.make("environment-project-files-query-test");
const optimisticFile = projectEnvironment.optimisticFile({
  environmentId,
  cwd: "/repo",
  relativePath: "convex.json",
});

function drainRegistryTasks(): void {
  while (registryTasks.size > 0) {
    const tasks = [...registryTasks];
    registryTasks.clear();
    for (const task of tasks) task();
  }
}

describe("project files queries", () => {
  afterEach(() => {
    clearProjectFileQueryData(environmentId, "/repo", "convex.json");
    drainRegistryTasks();
    vi.useRealTimers();
    vi.unstubAllGlobals();
  });

  it("resumes an unsaved draft after closing the preview and restoring write access", async () => {
    vi.stubGlobal("window", {});
    vi.useFakeTimers();
    const closePreview = appAtomRegistry.mount(optimisticFile);
    let canWrite = true;
    const persist = vi.fn().mockResolvedValue(AsyncResult.success(undefined));
    const onPendingChange = vi.fn();
    const makeCoordinator = () =>
      new FileSaveCoordinator({
        debounceMs: 500,
        canPersist: () => canWrite,
        persist,
        onPendingChange,
        onConfirmed: (contents) =>
          confirmProjectFileQueryData(environmentId, "/repo", "convex.json", contents),
      });
    const initial = makeCoordinator();
    setProjectFileQueryData(environmentId, "/repo", "convex.json", "unsaved draft");
    initial.change("unsaved draft");
    canWrite = false;
    initial.dispose();
    closePreview();
    await vi.runAllTimersAsync();
    drainRegistryTasks();

    expect(persist).not.toHaveBeenCalled();
    expect(onPendingChange).toHaveBeenLastCalledWith(true);
    const unsaved = getUnsavedProjectFileQueryData(environmentId, "/repo", "convex.json");
    expect(unsaved?.contents).toBe("unsaved draft");

    canWrite = true;
    const reopened = makeCoordinator();
    reopened.change(unsaved!.contents);
    await vi.advanceTimersByTimeAsync(500);

    expect(persist).toHaveBeenCalledOnce();
    expect(persist).toHaveBeenCalledWith("unsaved draft");
    expect(onPendingChange).toHaveBeenLastCalledWith(false);
    expect(getUnsavedProjectFileQueryData(environmentId, "/repo", "convex.json")).toBeNull();
    reopened.dispose();
    drainRegistryTasks();
    expect(appAtomRegistry.getNodes().has(optimisticFile)).toBe(false);
  });

  it("keeps a reopened editor's newer draft pending when the old editor's write finishes", async () => {
    vi.stubGlobal("window", {});
    vi.useFakeTimers();
    let canWrite = true;
    const saved = AsyncResult.success(undefined);
    let finishFirstWrite!: (result: typeof saved) => void;
    const firstWrite = new Promise<typeof saved>((resolve) => {
      finishFirstWrite = resolve;
    });
    const persist = vi.fn().mockReturnValueOnce(firstWrite).mockResolvedValue(saved);
    const onPendingChange = vi.fn();
    const makeCoordinator = () =>
      new FileSaveCoordinator({
        debounceMs: 500,
        canPersist: () => canWrite,
        persist,
        onPendingChange,
        onConfirmed: (contents) =>
          confirmProjectFileQueryData(environmentId, "/repo", "convex.json", contents),
      });

    const initial = makeCoordinator();
    setProjectFileQueryData(environmentId, "/repo", "convex.json", "first draft");
    initial.change("first draft");
    await vi.advanceTimersByTimeAsync(500);
    initial.dispose();

    const reopened = makeCoordinator();
    setProjectFileQueryData(environmentId, "/repo", "convex.json", "newer draft");
    reopened.change("newer draft");
    canWrite = false;
    finishFirstWrite(saved);
    await vi.runAllTimersAsync();

    expect(persist).toHaveBeenCalledOnce();
    expect(onPendingChange).toHaveBeenLastCalledWith(true);
    expect(getUnsavedProjectFileQueryData(environmentId, "/repo", "convex.json")?.contents).toBe(
      "newer draft",
    );

    canWrite = true;
    reopened.change("newer draft");
    await vi.advanceTimersByTimeAsync(500);
    expect(persist).toHaveBeenLastCalledWith("newer draft");
    expect(onPendingChange).toHaveBeenLastCalledWith(false);
    expect(getUnsavedProjectFileQueryData(environmentId, "/repo", "convex.json")).toBeNull();
    reopened.dispose();
  });

  it("keeps the latest optimistic draft when an older write finishes", () => {
    vi.stubGlobal("window", {});
    const initial = {
      relativePath: "convex.json",
      contents: '{"nodeVersion":"20"}',
      byteLength: 20,
      truncated: false,
    } satisfies ProjectReadFileResult;
    setProjectFileQueryData(environmentId, "/repo", "convex.json", '{"nodeVersion":"220"}');
    setProjectFileQueryData(environmentId, "/repo", "convex.json", '{"nodeVersion":"22"}');

    expect(getOptimisticProjectFileQueryData(environmentId, "/repo", "convex.json")?.contents).toBe(
      '{"nodeVersion":"22"}',
    );

    expect(
      confirmProjectFileQueryData(environmentId, "/repo", "convex.json", '{"nodeVersion":"220"}'),
    ).toBe(false);

    expect(resolveProjectFileQueryData(environmentId, "/repo", "convex.json", initial)).toEqual({
      relativePath: "convex.json",
      contents: '{"nodeVersion":"22"}',
      byteLength: 20,
      truncated: false,
    });

    expect(
      confirmProjectFileQueryData(environmentId, "/repo", "convex.json", '{"nodeVersion":"22"}'),
    ).toBe(true);
  });
  it("keeps a returned workspace's unsaved draft out of the checkout that reuses its path", () => {
    vi.stubGlobal("window", {});
    const threadA = ThreadId.make("draft-owner-a");
    const generationA = "a".repeat(32);
    const scopeA = createProjectFileScope({
      environmentId,
      cwd: "/pool/1/repo",
      relativePath: "notes.md",
      threadId: threadA,
      workspace: { threadId: threadA, generation: generationA },
    });
    scopeA.change("unsaved from A");

    // A is returned, then resumed in a different physical slot with a new lease generation.
    const resumedA = createProjectFileScope({
      environmentId,
      cwd: "/pool/2/repo",
      relativePath: "notes.md",
      threadId: threadA,
      workspace: { threadId: threadA, generation: "b".repeat(32) },
    });
    expect(resumedA.data()?.contents).toBe("unsaved from A");
    expect(resumedA.unsaved()).toBeNull();
    expect(resumedA.requiresRetry()).toBe(true);
    expect(resumedA.confirm("unsaved from A")).toBe(false);

    // Another thread now leases A's old slot path; it never sees A's text.
    const threadB = ThreadId.make("slot-reuser-b");
    const scopeB = createProjectFileScope({
      environmentId,
      cwd: "/pool/1/repo",
      relativePath: "notes.md",
      threadId: threadB,
      workspace: { threadId: threadB, generation: "c".repeat(32) },
    });
    expect(scopeB.data()).toBeNull();
    expect(scopeB.unsaved()).toBeNull();

    // An explicit edit after resume adopts the text for the new generation and allows saving.
    resumedA.change("unsaved from A");
    expect(resumedA.unsaved()?.contents).toBe("unsaved from A");
    expect(resumedA.requiresRetry()).toBe(false);
  });
});

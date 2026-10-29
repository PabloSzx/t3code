import { render, fakeTimers } from "../../../test/render";
import { EnvironmentId, ThreadId, type WorkspaceAvailability } from "@t3tools/contracts";
import { AsyncResult } from "effect/reactivity";
import { StrictMode, createRef, useImperativeHandle } from "react";
import { describe, expect, it, vi } from "vite-plus/test";

const state = vi.hoisted(() => ({
  write: vi.fn(),
  prepare: vi.fn(),
  complete: vi.fn(),
  confirm: vi.fn(),
  unsaved: null as string | null,
  scope: null as ReturnType<typeof import("./projectFilesQueryState").useProjectFileScope> | null,
  availability: null as WorkspaceAvailability | null,
}));
vi.mock(import("~/state/projects"), () => ({
  projectEnvironment: {
    writeFile: { label: "recover file", workspaceMode: "captured", run: state.write },
    writeFileFromEdit: { label: "edit file", workspaceMode: "activate", run: state.write },
  } as never,
}));
vi.mock(import("~/state/shell"), () => ({
  shellEnvironment: {
    prepareWorkspaceAction: { run: state.prepare },
    completeWorkspaceAction: { run: state.complete },
  } as never,
}));
vi.mock(import("~/state/session"), async (original) => ({
  ...(await original()),
  readEnvironmentScope: () => true,
}));
vi.mock(import("~/workspaceContext"), async () => {
  const { createContext } = await import("react");
  return {
    WorkspaceContext: createContext(null),
    useWorkspaceAvailability: () => state.availability,
  } as never;
});
vi.mock(
  import("./projectFilesQueryState"),
  () =>
    ({
      useProjectFileScope: () => state.scope!,
    }) as never,
);

import { WorkspaceContext } from "~/workspaceContext";
import { changeFileSaveSession, useFileSaveSession, type FileSaveTarget } from "./fileSaveSessions";

const target = {
  environmentId: EnvironmentId.make("file-save-sessions-browser"),
  cwd: "/workspace",
  relativePath: "README.md",
};
const threadId = ThreadId.make("save-session-thread");

function setup() {
  state.write.mockReset().mockResolvedValue(AsyncResult.success(undefined));
  state.prepare.mockReset();
  state.complete.mockReset().mockResolvedValue(AsyncResult.success(undefined));
  state.confirm.mockReset().mockReturnValue(true);
  state.unsaved = null;
  state.availability = {
    state: "active",
    cwd: target.cwd,
    context: { threadId, generation: "a".repeat(32) },
  } as WorkspaceAvailability;
  const scope = {
    threadId,
    workspace: { threadId, generation: "a".repeat(32) },
    unsaved: () => (state.unsaved === null ? null : { contents: state.unsaved }),
    change: (contents: string) => {
      state.unsaved = contents;
    },
    confirm: state.confirm,
  } as unknown as ReturnType<typeof import("./projectFilesQueryState").useProjectFileScope>;
  state.scope = scope;
  const pending = vi.fn();
  const editor = createRef<FileSaveTarget>();
  function EditorSurface() {
    const saveTarget = useFileSaveSession(target, pending);
    useImperativeHandle(editor, () => saveTarget, [saveTarget]);
    return <div>editor</div>;
  }
  function MarkdownSurface() {
    const saveTarget = useFileSaveSession(target, pending);
    useImperativeHandle(editor, () => saveTarget, [saveTarget]);
    return <div>markdown</div>;
  }
  const ui = (markdown = false) => (
    <StrictMode>
      <WorkspaceContext value={{ environmentId: target.environmentId, threadId, root: target.cwd }}>
        {markdown ? <MarkdownSurface /> : <EditorSurface />}
      </WorkspaceContext>
    </StrictMode>
  );
  return { ui, editor, pending, scope };
}

describe("file save sessions in React", () => {
  it("writes a pending draft once when the file switches surfaces under StrictMode", async () => {
    const h = setup();
    let finishWrite = () => {};
    state.write.mockImplementation(
      () =>
        new Promise((resolve) => {
          finishWrite = () => resolve(AsyncResult.success(undefined));
        }),
    );
    // The surfaces are bare save-session hosts and have no navigation.
    await using screen = await render(h.ui(), { router: false });
    using _timers = fakeTimers();
    changeFileSaveSession(h.editor.current!, "draft");
    await screen.rerender(h.ui(true));
    state.unsaved = null;
    finishWrite();
    await vi.runAllTimersAsync();
    expect(state.write.mock.calls.map(([, request]) => request)).toEqual([
      {
        environmentId: target.environmentId,
        input: { cwd: target.cwd, relativePath: target.relativePath, contents: "draft" },
        workspace: h.scope.workspace,
      },
    ]);
    expect(h.pending).toHaveBeenLastCalledWith("README.md", false);
  });

  it("keeps recovered drafts captured and activates only a new edit", async () => {
    const h = setup();
    state.unsaved = "recovered draft";
    state.availability = { state: "released", cwd: null };
    state.scope = { ...h.scope, workspace: undefined };
    state.prepare.mockResolvedValue(
      AsyncResult.success({
        workspace: { cwd: "/resumed", context: { threadId, generation: "b".repeat(32) } },
        reservationId: "reservation",
      }),
    );
    // The save hook needs workspace context but no route.
    using _timers = fakeTimers();
    await using _screen = await render(h.ui(), { router: false });
    await vi.runAllTimersAsync();
    expect(state.prepare).not.toHaveBeenCalled();
    expect(state.write).not.toHaveBeenCalled();
    expect(state.confirm).not.toHaveBeenCalled();

    changeFileSaveSession(h.editor.current!, "explicit edit");
    await vi.runAllTimersAsync();
    expect(state.prepare).toHaveBeenCalledTimes(1);
    expect(state.write.mock.calls.map(([, request]) => request)).toEqual([
      {
        environmentId: target.environmentId,
        input: { cwd: "/resumed", relativePath: target.relativePath, contents: "explicit edit" },
        workspace: { threadId, generation: "b".repeat(32) },
      },
    ]);
    expect(state.complete).toHaveBeenCalledTimes(1);
  });

  it("separates a retired generation's in-flight write from edits after resume", async () => {
    const h = setup();
    let finishOldWrite = () => {};
    state.write.mockImplementationOnce(
      () =>
        new Promise((resolve) => {
          finishOldWrite = () => resolve(AsyncResult.success(undefined));
        }),
    );
    // This drives hook lifetime and generation changes without navigation.
    await using screen = await render(h.ui(), { router: false });
    using _timers = fakeTimers();
    const retired = h.editor.current!;
    changeFileSaveSession(retired, "old generation");
    await vi.advanceTimersByTimeAsync(500);
    const oldConfirm = state.confirm;
    const nextConfirm = vi.fn().mockReturnValue(true);
    state.unsaved = null;
    state.scope = {
      ...h.scope,
      workspace: { threadId, generation: "b".repeat(32) },
      confirm: nextConfirm,
    };
    state.availability = { state: "active", cwd: target.cwd, context: state.scope.workspace! };
    await screen.rerender(h.ui());
    changeFileSaveSession(retired, "retired callback");
    changeFileSaveSession(h.editor.current!, "new generation");
    await vi.advanceTimersByTimeAsync(500);
    expect(
      state.write.mock.calls.map(([, request]) => [
        request.workspace?.generation,
        request.input.contents,
      ]),
    ).toEqual([
      ["a".repeat(32), "old generation"],
      ["b".repeat(32), "new generation"],
    ]);
    expect(nextConfirm).toHaveBeenCalledWith("new generation");
    finishOldWrite();
    await vi.runAllTimersAsync();
    expect(oldConfirm).toHaveBeenCalledWith("old generation");
    expect(nextConfirm).toHaveBeenCalledTimes(1);
  });
});

import { RegistryContext } from "@effect/atom-react";
import { registerWorkspaceQuery } from "@t3tools/client-runtime/state/runtime";
import { EnvironmentId, ThreadId, type WorkspaceAvailability } from "@t3tools/contracts";
import { AsyncResult, Atom, AtomRegistry } from "effect/reactivity";
import { describe, expect, it, vi } from "vite-plus/test";

import { render } from "../../test/render";

const state = vi.hoisted(() => ({ availability: null as WorkspaceAvailability | null }));
vi.mock(import("~/state/shell"), async () => {
  const { Atom: A, AsyncResult: R } = await import("effect/reactivity");
  return {
    shellEnvironment: { workspaceAvailability: () => A.make(() => R.success(state.availability)) },
  } as never;
});

import { WorkspaceContext } from "~/workspaceContext";
import { useEnvironmentQuery } from "./query";

const environmentId = EnvironmentId.make("revisioned-scoped-query");
const threadId = ThreadId.make("revisioned-scoped-thread");
const context = { threadId, generation: "a".repeat(32) };

function setup() {
  const reads = { raw: 0, scoped: 0 };
  const raw = registerWorkspaceQuery({
    atom: Atom.make(() => {
      reads.raw += 1;
      return AsyncResult.success("unscoped");
    }).pipe(Atom.keepAlive),
    target: { environmentId, input: { cwd: "/pool/1" } },
    bind: () => scoped,
  });
  const scoped = Atom.make(() => {
    reads.scoped += 1;
    return AsyncResult.success(`scoped ${reads.scoped}`);
  }).pipe(Atom.keepAlive);
  state.availability = { state: "active", cwd: "/pool/1", context };
  const registry = AtomRegistry.make();
  function Reader({ revision }: { revision: string | null }) {
    return <span>{useEnvironmentQuery(raw, revision).data ?? "none"}</span>;
  }
  const ui = (revision: string | null) => (
    <RegistryContext.Provider value={registry}>
      <WorkspaceContext value={{ environmentId, threadId, root: "/pool/1" }}>
        <Reader revision={revision} />
      </WorkspaceContext>
    </RegistryContext.Provider>
  );
  return { reads, ui, registry };
}

describe("useEnvironmentQuery revisions", () => {
  it("reads and refetches the workspace-scoped query under a mutation revision", async () => {
    const h = setup();
    // A bare query reader with no links or navigation.
    await using screen = await render(h.ui("mutation-1"), { router: false });
    await expect.element(screen.getByText("scoped 1")).toBeInTheDocument();

    await screen.rerender(h.ui("mutation-2"));

    await expect.element(screen.getByText("scoped 2")).toBeInTheDocument();
    expect(h.reads.raw).toBe(0);
    h.registry.dispose();
  });

  it("reads nothing for a returned workspace even with a revision", async () => {
    const h = setup();
    state.availability = { state: "released", cwd: null };
    await using screen = await render(h.ui("mutation-1"), { router: false });
    await expect.element(screen.getByText("none")).toBeInTheDocument();
    expect(h.reads).toEqual({ raw: 0, scoped: 0 });
    h.registry.dispose();
  });
});

import type {
  EnvironmentId,
  ThreadId,
  WorkspaceAvailability,
  WorkspaceOperationContext,
} from "@t3tools/contracts";
import type { AsyncResult, Atom } from "effect/reactivity";

type QueryAtom<A, E> = Atom.Atom<AsyncResult.AsyncResult<A, E>>;
interface QueryTarget<Input = unknown> {
  readonly environmentId: EnvironmentId;
  readonly input: Input;
  readonly workspace?: WorkspaceOperationContext;
}
interface QueryBinding {
  readonly target: QueryTarget;
  readonly bind: (input: {
    readonly workspace: WorkspaceOperationContext;
    readonly input: unknown;
  }) => object;
}
const queryBindings = new WeakMap<object, QueryBinding>();

export interface RegisterWorkspaceQueryOptions<A, E, Input> {
  readonly atom: QueryAtom<A, E>;
  readonly target: QueryTarget<Input>;
  readonly bind: (target: {
    readonly workspace: WorkspaceOperationContext;
    readonly input: Input;
  }) => QueryAtom<A, E>;
}
export function registerWorkspaceQuery<A, E, Input>(
  options: RegisterWorkspaceQueryOptions<A, E, Input>,
): QueryAtom<A, E> {
  queryBindings.set(options.atom, {
    target: options.target,
    bind: (target) => options.bind({ ...target, input: target.input as Input }),
  });
  return options.atom;
}

const WORKSPACE_PATH_FIELDS = ["cwd", "workspaceRoot", "worktreePath", "absolutePath", "path"];
// Nested inputs that address a workspace: asset resources and review patch requests.
const WORKSPACE_NESTED_FIELDS = ["resource", "request", "preview"];
// Asset resources that address the thread's own workspace; others are host-wide or immutable.
const WORKSPACE_ASSET_TAGS = new Set(["workspace-file", "media-file", "draft-workspace-file"]);

function isWorkspaceRecord(record: Record<string, unknown>): boolean {
  if (typeof record._tag === "string") return WORKSPACE_ASSET_TAGS.has(record._tag);
  return (
    WORKSPACE_PATH_FIELDS.some((key) => typeof record[key] === "string") ||
    Array.isArray(record.paths)
  );
}

export function hasWorkspaceTargets(input: unknown): boolean {
  if (input === null || typeof input !== "object") return false;
  const record = input as Record<string, unknown>;
  if (isWorkspaceRecord(record)) return true;
  return WORKSPACE_NESTED_FIELDS.some((key) => {
    const nested = record[key];
    return (
      nested !== null &&
      typeof nested === "object" &&
      isWorkspaceRecord(nested as Record<string, unknown>)
    );
  });
}

export interface ScopeWorkspaceQueryOptions<A, E> {
  readonly atom: QueryAtom<A, E> | null;
  readonly environmentId: EnvironmentId;
  readonly availability: WorkspaceAvailability | null;
  readonly threadId?: ThreadId;
  readonly root?: string;
}
export function scopeWorkspaceQuery<A, E>({
  atom,
  environmentId,
  availability,
  threadId,
  root,
}: ScopeWorkspaceQueryOptions<A, E>): QueryAtom<A, E> | null {
  if (atom === null) return null;
  const binding = queryBindings.get(atom);
  if (
    binding === undefined ||
    binding.target.environmentId !== environmentId ||
    !hasWorkspaceTargets(binding.target.input)
  )
    return atom;
  if (binding.target.workspace !== undefined) {
    if (threadId !== undefined && binding.target.workspace.threadId !== threadId) return atom;
    return availability?.cwd != null &&
      binding.target.workspace.generation === availability.context.generation
      ? atom
      : null;
  }
  if (availability?.cwd == null) return null;
  const input =
    root === undefined
      ? binding.target.input
      : rebaseWorkspaceInput({ input: binding.target.input, from: root, to: availability.cwd });
  return binding.bind({ workspace: availability.context, input }) as QueryAtom<A, E>;
}

export interface RebaseWorkspaceInputOptions<Input> {
  readonly input: Input;
  readonly from: string;
  readonly to: string;
}
export function rebaseWorkspaceInput<Input>({
  input,
  from,
  to,
}: RebaseWorkspaceInputOptions<Input>): Input {
  if (input === null || typeof input !== "object") return input;
  const source = from.replace(/[\\/]+$/, "");
  const destination = to.replace(/[\\/]+$/, "");
  const rebase = (value: unknown) => {
    if (typeof value !== "string") return value;
    if (value === source) return destination;
    return value.startsWith(`${source}/`) || value.startsWith(`${source}\\`)
      ? destination + value.slice(source.length)
      : value;
  };
  const rebaseRecord = (record: Record<string, unknown>): Record<string, unknown> => {
    // Absolute file targets inside the captured workspace follow it; relative ones already do.
    const relative =
      typeof record.relativePath === "string" && rebase(record.relativePath) !== record.relativePath
        ? { relativePath: rebase(record.relativePath) }
        : {};
    return {
      ...record,
      ...Object.fromEntries(
        WORKSPACE_PATH_FIELDS.filter((key) => typeof record[key] === "string").map((key) => [
          key,
          rebase(record[key]),
        ]),
      ),
      ...relative,
      ...(Array.isArray(record.paths) ? { paths: record.paths.map(rebase) } : {}),
    };
  };
  const record = input as Record<string, unknown>;
  return {
    ...rebaseRecord(record),
    ...Object.fromEntries(
      WORKSPACE_NESTED_FIELDS.filter((key) => {
        const nested = record[key];
        return nested !== null && typeof nested === "object" && !Array.isArray(nested);
      }).map((key) => [key, rebaseRecord(record[key] as Record<string, unknown>)]),
    ),
  } as Input;
}

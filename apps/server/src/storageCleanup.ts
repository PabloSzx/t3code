import {
  GitCommandError,
  CommandId,
  NodeId,
  ThreadId,
  ServerSettingsError,
  OrchestrationV2AppThreadJson,
  OrchestrationV2ProviderSessionJson,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import type {
  OrchestrationV2ThreadShell,
  ProjectId,
  ServerSettings,
  LeaseCleanupTask,
  TerminalSummary,
  WorktreeCleanupRules,
  StorageCleanupReport,
  StorageCleanupReportEntry,
} from "@t3tools/contracts";
import { resolveWorktreeCleanup } from "@t3tools/shared/projectSettings";
import { makeDrainableWorker } from "@t3tools/shared/DrainableWorker";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Layer from "effect/Layer";
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Equal from "effect/Equal";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Ref from "effect/Ref";
import type { PlatformError } from "effect/PlatformError";
import * as Schedule from "effect/Schedule";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import { parse as parseYaml } from "yaml";

import * as ServerConfig from "./config.ts";
import * as GitManager from "./git/GitManager.ts";
import * as ProjectStore from "./orchestration-v2/ProjectStore.ts";
import * as Orchestrator from "./orchestration-v2/Orchestrator.ts";
import * as ProjectionStore from "./orchestration-v2/ProjectionStore.ts";
import * as ProviderSessionManager from "./orchestration-v2/ProviderSessionManager.ts";
import { threadHasQueuedTurnStart } from "./orchestration-v2/ThreadSettlementService.ts";
import {
  worktreeActivityAt,
  worktreeOwner,
  worktreeThreadIdle,
} from "./workspace/worktreeThreads.ts";
import { forkParked } from "./serverActivation.ts";
import * as Settings from "./serverSettings.ts";
import * as TerminalManager from "./terminal/Manager.ts";
import { isFilesystemRoot, managedWorktreesDirectories } from "./worktreesDirectory.ts";
import * as GitVcsDriver from "./vcs/GitVcsDriver.ts";
import { removeLinkedWorktree } from "./vcs/worktreeRemoval.ts";
import { withWorkspaceLease } from "./workspace/workspaceLease.ts";
import * as TreehouseLeases from "./workspace/treehouseLeases.ts";
import * as LeaseCleanupQueue from "./workspace/LeaseCleanupQueue.ts";
import * as TreehouseWorktrees from "./vcs/TreehouseWorktrees.ts";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import { isNetworkPath } from "@t3tools/shared/path";

const decodeCleanupThread = Schema.decodeUnknownEffect(
  Schema.fromJsonString(OrchestrationV2AppThreadJson),
);
const decodeCleanupSession = Schema.decodeUnknownEffect(
  Schema.fromJsonString(OrchestrationV2ProviderSessionJson),
);
// pnpm 11 writes this file as JSON, older versions as YAML.
const decodePnpmModulesManifest = Schema.decodeUnknownEffect(
  Schema.Struct({
    storeDir: Schema.String,
    virtualStoreDir: Schema.optional(Schema.String),
  }),
);

const decodeTaskCompletion = Schema.decodeUnknownEffect(
  Schema.Struct({
    taskId: NodeId,
    status: Schema.String,
    completedAt: Schema.NullOr(Schema.DateTimeUtcFromString),
  }),
);

const DAY_MS = 86_400_000;
// Give settle scripts time to open terminals that protect the worktree.
const SETTLE_SCRIPT_GRACE_MS = 60_000;
// Worktree creation can precede its thread row, including review-guide checkouts.
const ORPHAN_WORKTREE_GRACE_MS = 60 * 60 * 1_000;

const sentence = (text: string) => text.charAt(0).toUpperCase() + text.slice(1);
const REPORT_ENTRY_LIMIT = 200;
const isGitCommandError = Schema.is(GitCommandError);

function cleanupFailureReason(error: { readonly message: string }) {
  return isGitCommandError(error)
    ? `${error.command}: ${error.detail.split(/\r?\n/)[0]}${error.reason ? ` (${error.reason})` : ""}`
    : (error.message.split(/\r?\n/)[0] ?? error.message);
}

const worktreeCleanupEnabled = (rules: WorktreeCleanupRules) =>
  rules.worktreeAfterDays !== null ||
  rules.worktreeSettledAfterDays !== null ||
  rules.worktreeOnMerge ||
  rules.worktreeOnDelete ||
  rules.worktreeUnchanged;

function anyWorktreePolicy(
  settings: ServerSettings,
  predicate: (rules: WorktreeCleanupRules) => boolean,
): boolean {
  return (
    predicate(resolveWorktreeCleanup(settings, null)) ||
    Object.keys(settings.projectSettingsOverrides).some((projectId) =>
      predicate(resolveWorktreeCleanup(settings, projectId as ProjectId)),
    )
  );
}

function sameProjectWorktreePolicies(left: ServerSettings, right: ServerSettings): boolean {
  return [
    ...new Set([
      ...Object.keys(left.projectSettingsOverrides),
      ...Object.keys(right.projectSettingsOverrides),
    ]),
  ].every((projectId) =>
    Equal.equals(
      left.projectSettingsOverrides[projectId as ProjectId]?.worktreeCleanup,
      right.projectSettingsOverrides[projectId as ProjectId]?.worktreeCleanup,
    ),
  );
}

type LimitRecoverySettings = Pick<
  ServerSettings,
  "autoResumeLimitedThreads" | "snoozeLimitedThreads"
>;

export function storageCleanupThreadIdle(
  thread: OrchestrationV2ThreadShell,
  now: number,
  settings: LimitRecoverySettings,
): boolean {
  return thread.worktreePath !== null && threadIdle(thread, now, settings);
}

function threadIdle(
  thread: OrchestrationV2ThreadShell,
  now: number,
  settings: LimitRecoverySettings,
): boolean {
  return (
    thread.activeRunId === null &&
    (thread.status === "idle" ||
      thread.status === "completed" ||
      thread.status === "interrupted" ||
      thread.status === "failed" ||
      thread.status === "cancelled" ||
      thread.status === "rolled_back") &&
    (thread.pendingBackgroundTasks?.length ?? 0) === 0 &&
    thread.pendingRuntimeRequest === null &&
    !threadHasQueuedTurnStart(thread, now) &&
    !heldForLimitRecovery(thread, now, settings)
  );
}

// A run held for its usage-limit reset has its work still ahead of it, and a
// fresh worktree would otherwise look unchanged and be removed before it runs.
function heldForLimitRecovery(
  thread: OrchestrationV2ThreadShell,
  now: number,
  settings: LimitRecoverySettings,
): boolean {
  if (
    thread.status !== "failed" ||
    thread.lastErrorClass !== "usage_limit" ||
    !thread.usageLimitResetAt ||
    thread.archivedAt !== null ||
    thread.settledOverride === "settled"
  )
    return false;
  const resetMs = Date.parse(thread.usageLimitResetAt);
  const recovery = thread.limitRecovery;
  if (recovery?.runId === thread.latestRunId && recovery.resetAt === thread.usageLimitResetAt)
    // An auto-resume holds until its continuation replaces the failed run.
    return recovery.autoResume || (recovery.snooze === true && resetMs > now);
  // The recovery worker arms this failure on a later tick; hold it until then.
  // Like the worker, skip a reset that had already passed when the run ended.
  if (
    !Number.isFinite(resetMs) ||
    resetMs <= DateTime.toEpochMillis(thread.latestRunCompletedAt ?? thread.updatedAt)
  )
    return false;
  return settings.autoResumeLimitedThreads || (settings.snoozeLimitedThreads && resetMs > now);
}

export { worktreeActivityAt as storageCleanupActivityAt } from "./workspace/worktreeThreads.ts";

function worktreeSettlementChain(
  thread: OrchestrationV2ThreadShell,
  byId: ReadonlyMap<ThreadId, OrchestrationV2ThreadShell>,
  now: number,
  settings: LimitRecoverySettings,
) {
  const chain = [thread];
  let current = thread;
  while (current.settledAt === null) {
    if (
      current.lineage.relationshipToParent !== "subagent" ||
      current.settledOverride === "active" ||
      current.pinnedAt != null
    )
      return [];
    const parent = byId.get(current.lineage.parentThreadId!);
    if (
      parent === undefined ||
      chain.some((member) => member.id === parent.id) ||
      parent.projectId !== thread.projectId ||
      parent.lineage.rootThreadId !== thread.lineage.rootThreadId ||
      !threadIdle(parent, now, settings)
    )
      return [];
    chain.push(parent);
    current = parent;
  }
  return chain;
}

/** Squash/rebase merges require the merged PR to name this exact head commit. */
export function storageCleanupPullRequestMerged(
  pullRequest: Pick<
    GitManager.GitBranchPullRequest,
    "state" | "headRef" | "baseRef" | "headSha"
  > | null,
  worktree: {
    readonly branch: string;
    readonly defaultBranch: string;
    readonly headSha: string;
    readonly integrated: boolean;
  },
): boolean {
  return (
    pullRequest?.state === "merged" &&
    (worktree.integrated ||
      (pullRequest.headRef === worktree.branch &&
        pullRequest.baseRef === worktree.defaultBranch &&
        pullRequest.headSha === worktree.headSha))
  );
}

export class StorageCleanup extends Context.Service<
  StorageCleanup,
  {
    readonly runNow: Effect.Effect<StorageCleanupReport, ServerSettingsError>;
    readonly latestReport: Effect.Effect<StorageCleanupReport | null>;
    readonly leaseTasks: Effect.Effect<readonly LeaseCleanupTask[], ServerSettingsError>;
    readonly reports: Stream.Stream<StorageCleanupReport | null>;
    readonly drain: Effect.Effect<void>;
  }
>()("t3/storageCleanup") {}

const make = Effect.gen(function* () {
  const config = yield* ServerConfig.ServerConfig;
  const settingsService = yield* Settings.ServerSettingsService;
  const projectStore = yield* ProjectStore.ProjectStoreV2;
  const engine = yield* Orchestrator.OrchestratorV2;
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const providerSessions = yield* ProviderSessionManager.ProviderSessionManagerV2;
  const worktreeRetryAt = yield* SubscriptionRef.make<number | null>(null);
  const deferWorktree = Effect.fn("StorageCleanup.deferWorktree")((at: number) =>
    SubscriptionRef.update(worktreeRetryAt, (current) => Math.min(current ?? Infinity, at)),
  );
  const sql = yield* SqlClient.SqlClient;
  const git = yield* GitVcsDriver.GitVcsDriver;
  const gitManager = yield* GitManager.GitManager;
  const terminals = yield* TerminalManager.TerminalManager;
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const treehouseLeases = yield* TreehouseLeases.TreehouseLeaseStore.pipe(
    Effect.provide(TreehouseLeases.layer),
  );
  const treehouse = yield* TreehouseWorktrees.TreehouseWorktrees.pipe(
    Effect.provide(TreehouseWorktrees.layer),
  );
  const leaseQueue = yield* LeaseCleanupQueue.LeaseCleanupQueue.pipe(
    Effect.provide(LeaseCleanupQueue.layer),
  );
  const liveTerminals = new Map<string, Map<string, TerminalSummary>>();
  const orphanSizes = new Map<string, number | null>();
  const noteTerminal = (terminal: TerminalSummary) => {
    const threadTerminals =
      liveTerminals.get(terminal.threadId) ?? new Map<string, TerminalSummary>();
    threadTerminals.set(terminal.terminalId, terminal);
    liveTerminals.set(terminal.threadId, threadTerminals);
  };

  const isWindows = yield* HostProcess.isWindows;
  const pathKey = (target: string) => {
    const resolved = path.resolve(target);
    return isWindows ? resolved.toLowerCase() : resolved;
  };
  const inside = (root: string, target: string) => {
    const relative = path.relative(pathKey(root), pathKey(target));
    return (
      relative !== "" &&
      relative !== ".." &&
      !relative.startsWith(`..${path.sep}`) &&
      !path.isAbsolute(relative)
    );
  };
  const hasTerminal = (worktreePath: string) =>
    [...liveTerminals.values()]
      .flatMap((entries) => [...entries.values()])
      .some((terminal) => {
        if (terminal.status !== "starting" && terminal.status !== "running") return false;
        const cwd = path.resolve(terminal.cwd);
        return (
          (terminal.worktreePath !== null &&
            pathKey(terminal.worktreePath) === pathKey(worktreePath)) ||
          pathKey(cwd) === pathKey(worktreePath) ||
          inside(worktreePath, cwd)
        );
      });

  // pnpm imports packages by clone or reflink when its store shares a device
  // with the worktree. Those files have one link but share blocks with the
  // store, so removing them frees almost nothing. A store on another device
  // forces real copies, which still count.
  const sharedPnpmVirtualStore = Effect.fnUntraced(
    function* (root: string) {
      const nodeModules = path.join(root, "node_modules");
      for (const candidate of [nodeModules, path.join(nodeModules, ".modules.yaml")]) {
        const linked = yield* fs.readLink(candidate).pipe(
          Effect.as(true),
          Effect.catchIf(
            (error) =>
              error.cause instanceof Error &&
              "code" in error.cause &&
              error.cause.code === "EINVAL",
            () => Effect.succeed(false),
          ),
        );
        if (linked) return null;
      }
      const text = yield* fs.readFileString(path.join(nodeModules, ".modules.yaml"));
      const manifest = yield* decodePnpmModulesManifest(yield* Effect.try(() => parseYaml(text)));
      // An agent can write this manifest, and statting a network path makes
      // Windows sign in to its host with the user's NTLM credentials.
      if (isNetworkPath(manifest.storeDir) || isNetworkPath(manifest.virtualStoreDir ?? "")) {
        return null;
      }
      const virtualStore = path.resolve(nodeModules, manifest.virtualStoreDir ?? ".pnpm");
      const [store, virtual] = yield* Effect.all([
        fs.stat(manifest.storeDir),
        fs.stat(virtualStore),
      ]);
      return store.dev === virtual.dev ? virtualStore : null;
    },
    Effect.orElseSucceed(() => null),
  );

  const measureWorktree = Effect.fn("storageCleanup.measureWorktree")(
    function* (worktreePath: string) {
      const root = yield* fs.realPath(worktreePath);
      const skipped = yield* sharedPnpmVirtualStore(root);
      const pending = [root];
      let entries = 1;
      let bytes = 0;
      while (pending.length > 0) {
        const target = pending.pop()!;
        if (target === skipped) continue;
        // Effect's stat follows links. Probe with readLink first to get lstat
        // semantics, including skipping dangling links and directory links.
        const isLink = yield* fs.readLink(target).pipe(
          Effect.as(true),
          Effect.catchIf(
            (error) =>
              error.cause instanceof Error &&
              "code" in error.cause &&
              error.cause.code === "EINVAL",
            () => Effect.succeed(false),
          ),
        );
        if (isLink) continue;
        if ((yield* fs.realPath(target)) !== target) return null;
        const stat = yield* fs.stat(target);
        if (stat.type === "File") {
          // Removing a shared hardlink leaves its blocks allocated.
          if (Option.getOrElse(stat.nlink, () => 1n) !== 1n) continue;
          bytes += Option.match(stat.blocks, {
            onNone: () => Number(stat.size),
            onSome: (blocks) => Number(blocks * 512n),
          });
        } else if (stat.type === "Directory") {
          const names = yield* fs.readDirectory(target);
          entries += names.length;
          if (entries > 2_000_000) return null;
          for (const name of names) {
            const child = path.join(target, name);
            if (!inside(root, child)) return null;
            pending.push(child);
          }
        }
      }
      return bytes;
    },
    Effect.timeout("30 seconds"),
    Effect.orElseSucceed(() => null),
  );

  // Large snapshots take seconds; use the reader worker instead of blocking the event loop.
  const readThreads = Effect.fn("StorageCleanup.readThreads")(function* () {
    const snapshot = yield* projections.getShellSnapshot().pipe(NodeSqliteClient.readOnly);
    const projects = yield* projectStore.listShells();
    return { projects, threads: [...snapshot.threads, ...snapshot.archivedThreads] };
  });

  const completedWorktreeTask = Effect.fn("StorageCleanup.completedWorktreeTask")(function* (
    thread: OrchestrationV2ThreadShell,
  ) {
    const parentId = thread.lineage.parentThreadId;
    if (
      thread.worktreePath === null ||
      parentId === null ||
      thread.lineage.relationshipToParent !== "subagent"
    )
      return null;
    const parent = yield* projections.getThread(parentId).pipe(
      Effect.catchTags({
        ProjectionStoreThreadNotFoundError: () => Effect.succeed(null),
      }),
    );
    if (parent === null) return null;
    if (
      parent.deletedAt === null &&
      parent.worktreePath !== null &&
      path.resolve(parent.worktreePath) === path.resolve(thread.worktreePath)
    ) {
      const current = yield* treehouseLeases.currentThreads({
        path: thread.worktreePath,
        threads: [parent],
      });
      if (!current.verified || current.threads.length !== 0) return null;
    }
    const rows = yield* sql<{
      readonly taskId: unknown;
      readonly status: unknown;
      readonly completedAt: unknown;
    }>`
      SELECT subagent_id AS "taskId", status, COALESCE(completed_at, updated_at) AS "completedAt"
      FROM orchestration_v2_projection_subagents
      WHERE child_thread_id = ${thread.id} AND thread_id = ${parentId} AND origin = 'app_owned'
      ORDER BY updated_at DESC, subagent_id DESC LIMIT 1
    `.pipe(NodeSqliteClient.readOnly);
    if (rows[0] === undefined) return null;
    const task = yield* decodeTaskCompletion(rows[0]);
    if (
      !["completed", "failed", "cancelled", "interrupted"].includes(task.status) ||
      task.completedAt === null
    )
      return null;
    const completedAt = DateTime.toEpochMillis(task.completedAt);
    if (
      completedAt < DateTime.toEpochMillis(thread.createdAt) ||
      (thread.latestRunCompletedAt != null &&
        completedAt < DateTime.toEpochMillis(thread.latestRunCompletedAt))
    )
      return null;
    return { taskId: task.taskId, completedAt };
  });

  const sessionsInWorktree = Effect.fn("StorageCleanup.sessionsInWorktree")(function* (
    worktreePath: string,
  ) {
    const rows = yield* sql<{ payload_json: string }>`
      SELECT payload_json FROM orchestration_v2_projection_provider_sessions WHERE status != 'stopped'
    `.pipe(NodeSqliteClient.readOnly);
    const sessions = yield* Effect.forEach(rows, (row) => decodeCleanupSession(row.payload_json));
    return sessions.filter((session) => {
      const cwd = path.resolve(session.cwd);
      return pathKey(cwd) === pathKey(worktreePath) || inside(worktreePath, cwd);
    });
  });

  const containsProjectRoot = Effect.fn("StorageCleanup.containsProjectRoot")(function* (
    worktreePath: string,
    projects: ReadonlyArray<{ readonly workspaceRoot: string }>,
  ) {
    for (const project of projects) {
      const projectPath = path.resolve(project.workspaceRoot);
      if (pathKey(projectPath) === pathKey(worktreePath) || inside(worktreePath, projectPath))
        return true;
      const realPath = yield* fs
        .realPath(projectPath)
        .pipe(Effect.orElseSucceed(() => projectPath));
      if (pathKey(realPath) === pathKey(worktreePath) || inside(worktreePath, realPath))
        return true;
    }
    return false;
  });

  const reportOrphanWorktrees = Effect.fn("StorageCleanup.reportOrphanWorktrees")(function* (
    roots: ReadonlyArray<string>,
    projects: ReadonlyArray<{ readonly workspaceRoot: string }>,
    entries: StorageCleanupReportEntry[],
  ) {
    const rows = yield* sql<{ worktreePath: string }>`
      SELECT DISTINCT json_extract(payload_json, '$.worktreePath') AS "worktreePath"
      FROM orchestration_v2_projection_threads
      WHERE json_extract(payload_json, '$.worktreePath') IS NOT NULL
    `.pipe(NodeSqliteClient.readOnly);
    const referenced = new Set<string>();
    for (const row of rows) {
      const resolved = path.resolve(row.worktreePath);
      referenced.add(pathKey(resolved));
      referenced.add(
        pathKey(yield* fs.realPath(resolved).pipe(Effect.orElseSucceed(() => resolved))),
      );
    }
    const referencedAncestors = new Set<string>();
    for (let ancestor of referenced) {
      while (roots.some((root) => inside(root, ancestor))) {
        ancestor = pathKey(path.dirname(ancestor));
        referencedAncestors.add(ancestor);
      }
    }
    const projectRoots = (yield* Effect.forEach(
      yield* Effect.filter(projects, (project) =>
        fs.exists(path.resolve(project.workspaceRoot)).pipe(Effect.orElseSucceed(() => true)),
      ),
      (project) => {
        const resolved = path.resolve(project.workspaceRoot);
        return fs.realPath(resolved).pipe(
          Effect.orElseSucceed(() => resolved),
          Effect.map((realPath) => [resolved, realPath]),
        );
      },
    )).flat();
    const inProject = (target: string, managedRoot: string) =>
      projectRoots.some(
        (root) =>
          !inside(root, managedRoot) && (pathKey(root) === pathKey(target) || inside(root, target)),
      );
    const directories = Effect.fnUntraced(function* (parent: string, managedRoot: string) {
      if (inProject(parent, managedRoot)) return [];
      const result: string[] = [];
      for (const name of yield* fs.readDirectory(parent).pipe(Effect.orElseSucceed(() => []))) {
        const target = path.join(parent, name);
        if (!inside(parent, target) || inProject(target, managedRoot)) continue;
        const directory = yield* Effect.gen(function* () {
          if (pathKey(yield* fs.realPath(target)) !== pathKey(target)) return false;
          return (yield* fs.stat(target)).type === "Directory";
        }).pipe(Effect.orElseSucceed(() => false));
        if (directory) result.push(target);
      }
      return result;
    });
    // Let the first walk time out and cache unmeasurable folders; later walks share the remaining budget.
    const now = yield* Clock.currentTimeMillis;
    const measurementDeadline = now + 30_000;
    let firstMeasurement = true;
    const reported = new Set<string>();
    const report = Effect.fnUntraced(
      function* (worktreePath: string) {
        const key = pathKey(worktreePath);
        if (
          reported.has(key) ||
          referenced.has(key) ||
          referencedAncestors.has(key) ||
          projectRoots.some((root) => pathKey(root) === key || inside(worktreePath, root))
        )
          return;
        const modified = Option.getOrNull((yield* fs.stat(worktreePath)).mtime);
        if (modified !== null && modified.getTime() > now - ORPHAN_WORKTREE_GRACE_MS) return;
        const gitPath = path.join(worktreePath, ".git");
        const linked = (yield* fs.exists(gitPath)) && (yield* fs.stat(gitPath)).type === "File";
        reported.add(key);
        const remaining = measurementDeadline - (yield* Clock.currentTimeMillis);
        if (!orphanSizes.has(key) && (firstMeasurement || remaining > 0)) {
          const measured = yield* measureWorktree(worktreePath).pipe(
            firstMeasurement ? Effect.asSome : Effect.timeoutOption(remaining),
          );
          firstMeasurement = false;
          if (Option.isSome(measured)) orphanSizes.set(key, measured.value);
        }
        const bytes = orphanSizes.get(key) ?? null;
        entries.push({
          kind: "worktree",
          outcome: "kept",
          reason: linked
            ? "No thread uses this worktree"
            : "No thread uses this folder, and it isn't a git worktree",
          path: worktreePath,
          projectPath: null,
          threadId: null,
          threadTitle: null,
          bytes,
          files: null,
        });
      },
      Effect.orElseSucceed(() => undefined),
    );
    for (const root of roots) {
      // Worktrees can be <root>/<repository>/<branch> or explicitly <root>/<name>.
      for (const projectDirectory of yield* directories(root, root)) {
        yield* Effect.gen(function* () {
          if (referenced.has(pathKey(projectDirectory))) return;
          const gitPath = path.join(projectDirectory, ".git");
          if (yield* fs.exists(gitPath)) {
            if ((yield* fs.stat(gitPath)).type === "File") yield* report(projectDirectory);
            return;
          }
          for (const worktreePath of yield* directories(projectDirectory, root)) {
            yield* report(worktreePath);
          }
        }).pipe(Effect.orElseSucceed(() => undefined));
      }
    }
    for (const key of orphanSizes.keys()) {
      if (!reported.has(key)) orphanSizes.delete(key);
    }
  });

  // Use two small Git reads: statusDetailsLocal also diffs and counts for every worktree.
  const branchChanged = Effect.fn("StorageCleanup.branchChanged")(function* (
    cwd: string,
    branch: string | null,
  ) {
    const failure = (detail: string) =>
      new GitCommandError({
        operation: "StorageCleanup.branchChanged",
        command: "git",
        cwd,
        detail,
      });
    // Return null for missing/non-repository cwd; force untranslated git diagnostics for matching.
    const read = Effect.fnUntraced(function* (args: string[], allowedExitCode?: number) {
      const result = yield* git
        .execute({
          operation: "StorageCleanup.branchChanged",
          cwd,
          args,
          env: { LC_ALL: "C" },
          allowNonZeroExit: true,
        })
        .pipe(
          Effect.catch((error) =>
            Effect.flatMap(fs.exists(cwd), (exists) =>
              exists ? Effect.fail(error) : Effect.succeed(null),
            ),
          ),
        );
      if (result === null) return null;
      if (result.exitCode === 0 || result.exitCode === allowedExitCode) return result;
      if (result.stderr.toLowerCase().includes("not a git repository")) return null;
      return yield* failure(
        result.stderr.trim().split(/\r?\n/)[0]?.slice(0, 1000) || "command failed",
      );
    });
    const index = yield* read(["rev-parse", "--git-path", "index"]);
    if (index === null) return true;
    if (yield* fs.exists(`${path.resolve(cwd, index.stdout.trim())}.lock`))
      return yield* failure(
        "Git index is locked. Status will resume when the index lock is removed.",
      );
    // Detached HEAD exits 1; full refs avoid shortening when a tag shares the branch name.
    const head = yield* read(["symbolic-ref", "--quiet", "HEAD"], 1);
    if (head === null) return true;
    const ref = head.exitCode === 0 ? head.stdout.trim() : "";
    const name = ref.startsWith("refs/heads/") ? ref.slice("refs/heads/".length).trim() : null;
    return (name?.startsWith("(") ? null : name) !== branch;
  });

  const localChangesReason = Effect.fn("StorageCleanup.localChangesReason")(function* (
    cwd: string,
    rules: WorktreeCleanupRules,
  ) {
    const status = yield* git.execute({
      operation: "StorageCleanup.localChanges",
      cwd,
      args: [
        "status",
        "--porcelain=v1",
        "-z",
        "--ignore-submodules=none",
        rules.worktreeKeepWhen === "tracked-changes"
          ? "--untracked-files=no"
          : "--untracked-files=all",
      ],
      maxOutputBytes: 64 * 1024,
    });
    if (status.stdoutTruncated) return "local changes exceed the inspection limit";
    const records = status.stdout.split("\0").filter(Boolean);
    let count = 0;
    for (let i = 0; i < records.length; i++) {
      count++;
      if (/^[RC]|^.[RC]/.test(records[i]!)) i++;
    }
    if (count > 0) return `has uncommitted changes (${count} ${count === 1 ? "file" : "files"})`;
    if (rules.worktreeKeepWhen !== "any-local-files") return null;
    const ignored = yield* git.execute({
      operation: "StorageCleanup.ignoredFiles",
      cwd,
      args: ["ls-files", "--others", "--ignored", "--exclude-standard", "--directory", "-z"],
      maxOutputBytes: 64 * 1024,
    });
    if (ignored.stdoutTruncated) return "ignored files exceed the inspection limit";
    const file = ignored.stdout
      .split("\0")
      .find((entry) => entry !== "" && !/(^|\/)node_modules\/$/.test(entry));
    return file === undefined ? null : `has ignored files (${file})`;
  });

  const submoduleRemoval = Effect.fn("StorageCleanup.submoduleRemoval")(
    function* (cwd: string, rules: WorktreeCleanupRules) {
      const inspect = (args: string[], maxOutputBytes = 64 * 1024) =>
        git.execute({
          operation: "StorageCleanup.submodules",
          cwd,
          args,
          maxOutputBytes,
        });
      const admin = yield* inspect(["rev-parse", "--absolute-git-dir"]);
      if (admin.stdoutTruncated || admin.stderrTruncated)
        return { force: false, reason: "submodule inspection exceeded the limit" };
      const modules = path.join(admin.stdout.replace(/\n$/, ""), "modules");
      const stored = yield* fs.exists(modules);
      if (!stored && !(yield* fs.exists(path.join(cwd, ".gitmodules"))))
        return { force: false, reason: null };
      const populated = yield* inspect([
        "submodule",
        "foreach",
        "--recursive",
        "--quiet",
        'printf "%s\\0" "$toplevel/$sm_path"',
      ]);
      if (populated.stdoutTruncated || populated.stderrTruncated)
        return { force: false, reason: "submodule inspection exceeded the limit" };
      const paths = populated.stdout.split("\0").filter(Boolean);
      const pending = [modules];
      for (const submodule of paths) {
        const directory = yield* inspect(["-C", submodule, "rev-parse", "--absolute-git-dir"]);
        if (directory.stdoutTruncated || directory.stderrTruncated)
          return { force: false, reason: "submodule inspection exceeded the limit" };
        pending.push(directory.stdout.replace(/\n$/, ""));
      }
      const visited = new Set<string>();
      while (pending.length > 0) {
        const directory = pending.pop()!;
        if (!(yield* fs.exists(directory))) continue;
        const canonical = yield* fs.realPath(directory);
        if (visited.has(canonical)) continue;
        visited.add(canonical);
        if (visited.size > 10_000)
          return { force: false, reason: "submodule inspection exceeded the limit" };
        if ((yield* fs.stat(directory)).type !== "Directory")
          return { force: false, reason: "submodule repository could not be inspected" };
        if (yield* fs.exists(path.join(directory, "HEAD"))) {
          const unpushed = yield* inspect([
            "--git-dir",
            directory,
            "--work-tree",
            cwd,
            "rev-list",
            "--max-count=1",
            "HEAD",
            "--all",
            "--reflog",
            "--not",
            "--remotes",
          ]);
          if (unpushed.stdoutTruncated || unpushed.stderrTruncated)
            return { force: false, reason: "submodule inspection exceeded the limit" };
          if (unpushed.stdout.trim() !== "")
            return { force: false, reason: "submodule has commits not on a remote-tracking ref" };
          pending.push(path.join(directory, "modules"));
        } else {
          for (const name of yield* fs.readDirectory(directory))
            pending.push(path.join(directory, name));
        }
      }
      // Check files after walking stored history to shorten the gap before forced removal.
      for (const submodule of paths) {
        const changes = yield* localChangesReason(submodule, rules);
        if (changes !== null) return { force: false, reason: `submodule ${changes}` };
      }
      const force = stored || paths.length > 0;
      if (force && rules.worktreeKeepWhen !== "tracked-changes") {
        const root = yield* fs.realPath(cwd);
        // Git status and foreach omit files inside deinitialized gitlink directories.
        const populatedPaths = new Set(paths.map(pathKey));
        const unchecked: string[] = [];
        for (const repository of [root, ...paths]) {
          const index = yield* inspect(
            ["-C", repository, "ls-files", "--stage", "-z"],
            16 * 1024 * 1024,
          );
          if (index.stdoutTruncated || index.stderrTruncated)
            return { force: false, reason: "submodule inspection exceeded the limit" };
          for (const entry of index.stdout.split("\0")) {
            if (!entry.startsWith("160000 ")) continue;
            const submodule = path.resolve(repository, entry.slice(entry.indexOf("\t") + 1));
            if (!inside(root, submodule))
              return { force: false, reason: "submodules could not be inspected" };
            if (!populatedPaths.has(pathKey(submodule)) && (yield* fs.exists(submodule)))
              unchecked.push(submodule);
          }
        }
        let inspected = unchecked.length;
        while (unchecked.length > 0) {
          if (inspected > 10_000)
            return { force: false, reason: "submodule inspection exceeded the limit" };
          const target = unchecked.pop()!;
          if (
            (yield* fs.realPath(target)) !== path.resolve(target) ||
            (yield* fs.stat(target)).type !== "Directory"
          )
            return { force: false, reason: "submodule has local files" };
          if (path.basename(target) === "node_modules") continue;
          const names = yield* fs.readDirectory(target);
          inspected += names.length;
          for (const name of names) unchecked.push(path.join(target, name));
        }
      }
      return { force, reason: null };
    },
    Effect.orElseSucceed(() => ({ force: false, reason: "submodules could not be inspected" })),
  );

  const cleanupGit = Effect.fn("StorageCleanup.cleanupGit")(function* (
    cwd: string,
    args: string[],
    command: string,
  ) {
    const result = yield* git.execute({
      operation: "StorageCleanup.cleanupGit",
      cwd,
      args,
      allowNonZeroExit: true,
      timeoutMs: 300_000,
    });
    if (result.exitCode !== 0)
      return yield* new GitCommandError({
        operation: "StorageCleanup.cleanupGit",
        command,
        cwd,
        detail: result.stderr.trim().split(/\r?\n/)[0]?.slice(0, 1000) || "command failed",
      });
  });

  const purgeExpired = Effect.fn("StorageCleanup.purgeExpired")(function* (input: {
    readonly owner: Pick<OrchestrationV2ThreadShell, "id" | "projectId">;
    readonly path: string;
    readonly leaseId: string;
    readonly settings: WorktreeCleanupRules;
    readonly now: number;
    readonly defaultBranches: Map<string, string | null>;
    readonly refreshedDefaultRefs: Map<string, Set<string>>;
  }) {
    const { settings, now } = input;
    const readFamily = Effect.fn("StorageCleanup.expiryFamily")(function* () {
      const snapshot = yield* projections
        .getShellSnapshot({ worktreePath: input.path, includeDeleted: true })
        .pipe(NodeSqliteClient.readOnly);
      const current = yield* treehouseLeases.currentThreads({
        path: input.path,
        threads: [...snapshot.threads, ...snapshot.archivedThreads],
      });
      return { ...current, sequence: snapshot.snapshotSequence };
    });
    const initial = yield* readFamily();
    const group = initial.threads;
    const owner = worktreeOwner(group);
    const idle = (member: OrchestrationV2ThreadShell) =>
      member.deletedAt === null
        ? worktreeThreadIdle(member, now)
        : member.activeRunId === null && (member.pendingBackgroundTasks?.length ?? 0) === 0;
    if (
      !initial.verified ||
      initial.leaseId !== input.leaseId ||
      owner?.id !== input.owner.id ||
      owner.projectId !== input.owner.projectId ||
      group.some((member) => member.projectId !== owner.projectId || !idle(member))
    )
      return {
        kind: "kept" as const,
        reason: "checkout family is active or its ownership changed",
      };
    const task = owner.deletedAt === null ? yield* completedWorktreeTask(owner) : null;
    const finishedAt =
      task?.completedAt ??
      (owner.settledAt === null
        ? owner.deletedAt === null
          ? null
          : DateTime.toEpochMillis(owner.deletedAt)
        : DateTime.toEpochMillis(owner.settledAt));
    const activityAt = Math.max(
      ...group.map(worktreeActivityAt),
      DateTime.toEpochMillis(owner.createdAt),
      task?.completedAt ?? -Infinity,
      owner.deletedAt !== null && owner.settledAt === null
        ? DateTime.toEpochMillis(owner.deletedAt)
        : -Infinity,
    );
    let due =
      (settings.worktreeAfterDays !== null &&
        activityAt <= now - settings.worktreeAfterDays * DAY_MS) ||
      (finishedAt !== null &&
        settings.worktreeSettledAfterDays !== null &&
        Math.max(activityAt, finishedAt) <= now - settings.worktreeSettledAfterDays * DAY_MS) ||
      (owner.deletedAt !== null && settings.worktreeOnDelete);
    const nextExpiry = Math.min(
      settings.worktreeAfterDays === null
        ? Infinity
        : activityAt + settings.worktreeAfterDays * DAY_MS,
      finishedAt === null || settings.worktreeSettledAfterDays === null
        ? Infinity
        : Math.max(activityAt, finishedAt) + settings.worktreeSettledAfterDays * DAY_MS,
    );
    const waitForExpiry =
      Number.isFinite(nextExpiry) && nextExpiry > now ? deferWorktree(nextExpiry) : Effect.void;
    if (!due) yield* waitForExpiry;
    if (!due && !settings.worktreeOnMerge && !settings.worktreeUnchanged) return null;
    const listed = yield* git.execute({
      operation: "StorageCleanup.expiryRepository",
      cwd: input.path,
      args: ["worktree", "list", "--porcelain", "-z"],
      maxOutputBytes: 1024 * 1024,
    });
    const first = listed.stdout.split("\0")[0];
    if (listed.stdoutTruncated || listed.stderrTruncated || !first?.startsWith("worktree "))
      return { kind: "kept" as const, reason: "checkout repository could not be verified" };
    const pool = yield* TreehouseLeases.treehousePoolForPath(input.path);
    if (pool === null)
      return { kind: "kept" as const, reason: "allocator pool could not be verified" };
    const parent = path.dirname(input.path);
    const removalRoot = parent !== pool && inside(pool, parent) ? parent : input.path;
    const repositoryPath = yield* fs.realPath(first.slice("worktree ".length));
    if (repositoryPath === input.path)
      return { kind: "kept" as const, reason: "contains a project checkout" };
    const symbolic = yield* git.execute({
      operation: "StorageCleanup.expiryBranch",
      cwd: input.path,
      args: ["symbolic-ref", "--quiet", "HEAD"],
      allowNonZeroExit: true,
    });
    const fullRef = symbolic.exitCode === 0 ? symbolic.stdout.trim() : "";
    const branch = fullRef.startsWith("refs/heads/") ? fullRef.slice("refs/heads/".length) : null;
    const head = yield* git.resolveCommit({ cwd: input.path, revision: "HEAD" });
    if (!due) {
      const remote = yield* git.resolvePrimaryRemoteName(repositoryPath);
      const cacheKey = `${repositoryPath}\0${remote}`;
      let defaultBranch = input.defaultBranches.get(cacheKey);
      if (defaultBranch === undefined) {
        defaultBranch = yield* git.resolveDefaultBranchName(repositoryPath, remote);
        if (defaultBranch === null)
          defaultBranch = yield* git
            .execute({
              operation: "StorageCleanup.expiryDefaultBranch",
              cwd: repositoryPath,
              args: ["ls-remote", "--symref", "--", remote, "HEAD"],
              timeoutMs: 20_000,
            })
            .pipe(
              Effect.map(
                (result) => /^ref: refs\/heads\/(.+)\tHEAD\r?$/m.exec(result.stdout)?.[1] ?? null,
              ),
              Effect.orElseSucceed(() => null),
            );
        input.defaultBranches.set(cacheKey, defaultBranch);
      }
      if (defaultBranch !== null) {
        const defaultRef = `refs/remotes/${remote}/${defaultBranch}`;
        const refreshed = input.refreshedDefaultRefs.get(repositoryPath) ?? new Set<string>();
        if (!refreshed.has(defaultRef)) {
          yield* git.fetchRemoteTrackingBranch({
            cwd: repositoryPath,
            remoteName: remote,
            remoteBranch: defaultBranch,
          });
          refreshed.add(defaultRef);
          input.refreshedDefaultRefs.set(repositoryPath, refreshed);
        }
        const base = yield* git
          .resolveCommit({ cwd: repositoryPath, revision: defaultRef })
          .pipe(Effect.orElseSucceed(() => null));
        if (base !== null) {
          const ancestor = yield* git.execute({
            operation: "StorageCleanup.expiryIntegrated",
            cwd: input.path,
            args: ["merge-base", "--is-ancestor", head.commitSha, base.commitSha],
            allowNonZeroExit: true,
          });
          const integrated = ancestor.exitCode === 0;
          due = settings.worktreeUnchanged && integrated;
          if (!due && settings.worktreeOnMerge && branch !== null) {
            const pullRequest = yield* gitManager.branchPullRequest(
              { cwd: input.path, branch },
              { refresh: true },
            );
            due = storageCleanupPullRequestMerged(pullRequest, {
              branch,
              defaultBranch,
              headSha: head.commitSha,
              integrated,
            });
          }
        }
      }
    }
    if (!due) return null;
    const quietUntil =
      Math.max(
        ...group.flatMap((member) =>
          member.settledAt === null ? [] : [DateTime.toEpochMillis(member.settledAt)],
        ),
      ) + SETTLE_SCRIPT_GRACE_MS;
    if (quietUntil > now) {
      yield* deferWorktree(quietUntil);
      return { kind: "kept" as const, reason: "finished moments ago" };
    }
    const memberIds = group.map((member) => member.id);
    const inspect = Effect.fn("StorageCleanup.recheckExpiry")(function* () {
      if (hasTerminal(removalRoot)) return { blocked: "open terminal", sequence: 0, group: [] };
      if (yield* containsProjectRoot(removalRoot, yield* projectStore.listShells()))
        return { blocked: "contains a project checkout", sequence: 0, group: [] };
      const current = yield* readFamily();
      if (
        !current.verified ||
        current.leaseId !== input.leaseId ||
        current.threads.length !== group.length ||
        current.threads.some((member) => {
          const previous = group.find((entry) => entry.id === member.id);
          return (
            previous === undefined ||
            !idle(member) ||
            !Equal.equals(member.deletedAt, previous.deletedAt) ||
            !Equal.equals(member.settledAt, previous.settledAt) ||
            worktreeActivityAt(member) !== worktreeActivityAt(previous) ||
            (member.branch !== previous.branch && member.branch !== branch)
          );
        })
      )
        return {
          blocked: "thread activity or ownership changed since check",
          sequence: 0,
          group: [],
        };
      if (worktreeOwner(current.threads)?.id !== owner.id)
        return { blocked: "checkout owner changed since check", sequence: 0, group: [] };
      if (task !== null) {
        const latestOwner = current.threads.find((member) => member.id === owner.id);
        const latestTask =
          latestOwner === undefined ? null : yield* completedWorktreeTask(latestOwner);
        if (latestTask?.taskId !== task.taskId || latestTask.completedAt !== task.completedAt)
          return { blocked: "delegated task changed since check", sequence: 0, group: [] };
      }
      const currentSettings = resolveWorktreeCleanup(
        yield* settingsService.getSettings,
        owner.projectId,
      );
      if (
        Object.keys(settings).some(
          (key) =>
            settings[key as keyof typeof settings] !==
            currentSettings[key as keyof typeof settings],
        )
      )
        return { blocked: "settings changed since check", sequence: 0, group: [] };
      for (const threadId of memberIds) {
        const pending =
          yield* sql`SELECT 1 FROM orchestration_v2_effect_outbox WHERE thread_id = ${threadId} AND status NOT IN ('succeeded', 'cancelled', 'failed') LIMIT 1`.pipe(
            NodeSqliteClient.readOnly,
          );
        if (pending.length > 0)
          return { blocked: "thread effects are still pending", sequence: 0, group: [] };
      }
      return { blocked: null, sequence: current.sequence, group: current.threads };
    });
    const stopped = yield* withWorkspaceLease(
      input.path,
      Effect.gen(function* () {
        const current = yield* inspect();
        if (current.blocked !== null) return current.blocked;
        for (const session of yield* sessionsInWorktree(removalRoot))
          if (
            (yield* providerSessions.isLive(session.id)) &&
            !(yield* providerSessions.releaseIdleForThreads({
              providerSessionId: session.id,
              threadIds: memberIds,
            }))
          )
            return "provider session is still open";
        return null;
      }),
    );
    if (stopped !== null) return { kind: "kept" as const, reason: stopped };
    const before = yield* inspect();
    if (before.blocked !== null) return { kind: "kept" as const, reason: before.blocked };
    if (branch !== null)
      for (const member of before.group) {
        if (member.deletedAt !== null || member.branch === branch) continue;
        yield* engine.dispatch({
          type: "thread.metadata.update",
          commandId: CommandId.make(
            `worktree-expiry:${input.leaseId}:${member.id}:${before.sequence}`,
          ),
          threadId: member.id,
          branch,
          expectedBranch: member.branch,
          expectedWorktreePath: input.path,
          expectedSequence: before.sequence,
        });
      }
    const bytes = yield* measureWorktree(removalRoot).pipe(
      Effect.timeout("1 second"),
      Effect.orElseSucceed(() => null),
    );
    const blocked = yield* treehouse.destroyIdle({
      cwd: repositoryPath,
      path: input.path,
      branch: branch ?? owner.branch ?? "",
      headSha: head.commitSha,
      leaseId: input.leaseId,
      threadIds: memberIds,
      recheck: inspect().pipe(
        Effect.map((result) => result.blocked),
        Effect.mapError(
          (cause) =>
            new GitCommandError({
              operation: "StorageCleanup.recheckExpiry",
              command: "treehouse destroy",
              cwd: repositoryPath,
              detail: "Could not recheck expiry ownership.",
              cause,
            }),
        ),
      ),
    });
    if (blocked !== null) return { kind: "kept" as const, reason: blocked };
    yield* gitManager.invalidateStatus(repositoryPath);
    return { kind: "removed" as const, bytes, repositoryPath };
  });

  const expireLeases = Effect.fn("StorageCleanup.expireLeases")(function* (input: {
    readonly settings: ServerSettings;
    readonly now: number;
    readonly entries: StorageCleanupReportEntry[];
  }) {
    const days = input.settings.storageCleanup.leaseHardExpiryDays;
    if (days === null) return;
    const inventory = yield* treehouse.listLeases;
    const snapshot = yield* readThreads().pipe(Effect.result);
    const owners = new Map<string, { threadIds: string[]; activityAt: number | null }>();
    const groups =
      snapshot._tag === "Success"
        ? Map.groupBy(
            snapshot.success.threads.filter((thread) => thread.worktreePath !== null),
            (thread) => path.resolve(thread.worktreePath!),
          )
        : new Map<string, OrchestrationV2ThreadShell[]>();
    for (const lease of inventory) {
      const group = groups.get(lease.path) ?? [];
      const current = yield* treehouseLeases
        .currentThreads({ path: lease.path, threads: group })
        .pipe(Effect.result);
      if (
        current._tag === "Success" &&
        current.success.verified &&
        current.success.leaseId === lease.lease_id
      ) {
        const activity = current.success.threads.map(worktreeActivityAt);
        owners.set(lease.path, {
          threadIds: current.success.threads.map((thread) => thread.id),
          activityAt: activity.length ? Math.max(...activity) : null,
        });
      }
    }
    const tasks = yield* leaseQueue.reconcile({ leases: inventory, owners, now: input.now, days });
    for (let task of tasks) {
      const lease = inventory.find(
        (entry) =>
          entry.lease_id === task.leaseId && entry.path === task.path && entry.pool === task.pool,
      );
      if (lease === undefined) continue;
      const due =
        task.dueAt === null
          ? null
          : Option.getOrNull(DateTime.make(task.dueAt).pipe(Option.map(DateTime.toEpochMillis)));
      const retry =
        task.nextAttemptAt === null
          ? null
          : Option.getOrNull(
              DateTime.make(task.nextAttemptAt).pipe(Option.map(DateTime.toEpochMillis)),
            );
      let reason = task.reason;
      let outcome: StorageCleanupReportEntry["outcome"] = "kept";
      if (snapshot._tag === "Failure") {
        task = yield* leaseQueue.update({
          task,
          state: "blocked",
          reason: "Task ownership lookup failed",
          now: input.now,
        });
        reason = task.reason;
        outcome = "failed";
        yield* deferWorktree(input.now + 300000);
      } else if (due !== null && due > input.now) yield* deferWorktree(due);
      else if (retry !== null && retry > input.now) yield* deferWorktree(retry);
      else if (
        due !== null &&
        task.state !== "removed" &&
        task.state !== "released" &&
        task.reason !== "A completed lease generation reappeared" &&
        lease.leased_at === task.acquiredAt &&
        lease.lease_holder === task.holder &&
        !lease.quarantined &&
        /^[a-f0-9]{32}$/.test(lease.lease_id)
      ) {
        task = yield* leaseQueue.update({
          task,
          state: "reclaiming",
          reason: "Hard lease age limit reached",
          now: input.now,
        });
        const result = yield* treehouse
          .expireLease({
            lease,
            days,
            candidateThreadIds: (groups.get(lease.path) ?? []).map((thread) => thread.id),
            recheck: Effect.gen(function* () {
              if ((yield* settingsService.getSettings).storageCleanup.leaseHardExpiryDays !== days)
                return "Lease expiry policy changed";
              if (
                !lease.removal_root ||
                (yield* containsProjectRoot(lease.removal_root, yield* projectStore.listShells()))
              )
                return "Contains a project checkout or an unverified removal boundary";
              return null;
            }).pipe(
              Effect.mapError(
                (cause) =>
                  new GitCommandError({
                    operation: "StorageCleanup.recheckForcedExpiry",
                    command: "treehouse destroy",
                    cwd: lease.path,
                    detail: "Cannot verify the forced expiry policy or removal boundary",
                    cause,
                  }),
              ),
            ),
            stopOwners: Effect.fn("StorageCleanup.stopExpiredLeaseOwners")(
              function* (target) {
                const current = yield* readThreads();
                for (const threadId of target.threadIds) {
                  if (
                    !current.threads.some(
                      (thread) =>
                        thread.id === threadId &&
                        thread.worktreePath !== null &&
                        path.resolve(thread.worktreePath) === target.path,
                    )
                  )
                    continue;
                  yield* engine.dispatch({
                    type: "thread.stop",
                    commandId: CommandId.make(`lease-expiry:${lease.lease_id}:${threadId}`),
                    threadId: ThreadId.make(threadId),
                    stopDelegatedTasks: false,
                    reason: "Lease reached its hard age limit",
                  });
                  for (const terminal of liveTerminals.get(threadId)?.values() ?? []) {
                    if (
                      path.resolve(terminal.cwd) === lease.removal_root ||
                      inside(lease.removal_root, path.resolve(terminal.cwd))
                    )
                      yield* terminals.close({ threadId, terminalId: terminal.terminalId });
                  }
                }
              },
              (effect) =>
                effect.pipe(
                  Effect.mapError(
                    (cause) =>
                      new GitCommandError({
                        operation: "StorageCleanup.stopExpiredLeaseOwners",
                        command: "thread.stop",
                        cwd: lease.path,
                        detail: "Cannot stop the expired lease owner",
                        cause,
                      }),
                  ),
                ),
            ),
          })
          .pipe(Effect.result);
        if (result._tag === "Success" && result.success === null) {
          yield* gitManager.invalidateStatus(lease.path);
          task = yield* leaseQueue.update({
            task,
            state: "removed",
            reason: "Expired lease checkout removed; branch refs retained",
            now: input.now,
          });
          outcome = "removed";
        } else {
          task = yield* leaseQueue.update({
            task,
            state: "blocked",
            reason:
              result._tag === "Success"
                ? result.success!
                : "Managed lease expiry failed; retry is scheduled",
            now: input.now,
          });
          outcome = "failed";
          yield* deferWorktree(input.now + 300000);
        }
        reason = task.reason;
      }
      input.entries.push({
        kind: "worktree",
        outcome,
        reason: `${reason}. Cleanup task ${task.id}; ${task.state}${task.dueAt === null ? "" : `; deadline ${task.dueAt}`}`,
        leaseId: lease.lease_id || undefined,
        path: lease.path,
        projectPath: null,
        threadId: null,
        threadTitle: null,
        bytes: null,
        files: null,
      });
    }
  });

  const cleanWorktrees = Effect.fn("StorageCleanup.cleanWorktrees")(function* (
    serverSettings: ServerSettings,
    now: number,
    entries: StorageCleanupReportEntry[],
  ) {
    if (!anyWorktreePolicy(serverSettings, worktreeCleanupEnabled))
      return { waiting: false, kept: [] };
    let waiting = false;
    const kept: string[] = [];
    const roots: Array<string> = [];
    for (const directory of managedWorktreesDirectories(
      serverSettings,
      config.worktreesDir,
      path,
      yield* HostProcess.HomeDirectory,
    )) {
      const root = yield* fs.exists(directory).pipe(
        Effect.flatMap((exists) => (exists ? fs.realPath(directory) : Effect.succeed(null))),
        Effect.orElseSucceed(() => null),
      );
      if (root !== null && !isFilesystemRoot(root, path)) roots.push(root);
    }
    const deletedRows = yield* sql<{ payload_json: string; workspaceRoot: string }>`
          SELECT t.payload_json, p.workspace_root AS "workspaceRoot"
          FROM orchestration_v2_projection_threads t
          JOIN projection_projects p ON p.project_id = t.project_id
          WHERE t.deleted_at IS NOT NULL
        `.pipe(NodeSqliteClient.readOnly);
    let deletedThreads = (yield* Effect.forEach(deletedRows, (row) =>
      decodeCleanupThread(row.payload_json).pipe(
        Effect.map((thread) => ({ ...thread, workspaceRoot: row.workspaceRoot })),
      ),
    )).filter((thread) => thread.worktreePath !== null);
    const heldInventory = yield* treehouseLeases.listHeld().pipe(Effect.result);
    const snapshot = yield* readThreads();
    const currentIds = new Set<string>();
    const generations = new Map<string, string | null>();
    const ownershipErrors = new Set<string>();
    const references = Map.groupBy(
      [...snapshot.threads, ...deletedThreads].filter((thread) => thread.worktreePath !== null),
      (thread) => path.resolve(thread.worktreePath!),
    );
    for (const [worktreePath, threads] of references) {
      const current = yield* treehouseLeases
        .currentThreads({ path: worktreePath, threads })
        .pipe(Effect.result);
      if (current._tag === "Failure") {
        const observed =
          heldInventory._tag === "Success"
            ? heldInventory.success.find((held) => held.path === worktreePath)?.leaseId
            : null;
        generations.set(worktreePath, observed ?? null);
        ownershipErrors.add(worktreePath);
        for (const thread of threads) currentIds.add(thread.id);
        continue;
      }
      generations.set(worktreePath, current.success.leaseId);
      if (!current.success.verified) ownershipErrors.add(worktreePath);
      for (const thread of current.success.threads) currentIds.add(thread.id);
      if (current.success.threads.length === 0 && current.success.leaseId !== null)
        entries.push({
          kind: "worktree",
          outcome: "kept",
          reason: "Current Treehouse lease has no verified thread owner",
          leaseId: current.success.leaseId ?? undefined,
          path: worktreePath,
          projectPath: null,
          threadId: null,
          threadTitle: null,
          bytes: null,
          files: null,
        });
    }
    deletedThreads = deletedThreads.filter((thread) => currentIds.has(thread.id));
    const threadsById = new Map(snapshot.threads.map((thread) => [thread.id, thread]));
    const refreshedDefaultRefs = new Map<string, Set<string>>();
    const defaultBranches = new Map<string, string | null>();
    const groups = Map.groupBy(
      snapshot.threads.filter(
        (thread) => thread.worktreePath !== null && currentIds.has(thread.id),
      ),
      (thread) => path.resolve(thread.worktreePath!),
    );
    const deletedByPath = Map.groupBy(deletedThreads, (thread) =>
      path.resolve(thread.worktreePath!),
    );
    const families = Map.groupBy(
      [...snapshot.threads, ...deletedThreads].filter(
        (thread) => thread.worktreePath !== null && currentIds.has(thread.id),
      ),
      (thread) => path.resolve(thread.worktreePath!),
    );
    const candidates = [...families].flatMap(([worktreePath, family]) => {
      const group = groups.get(worktreePath) ?? [];
      const deleted = deletedByPath.get(worktreePath) ?? [];
      const owner =
        generations.get(worktreePath) != null
          ? (worktreeOwner(family) ?? family[0])
          : (deleted.find(
              (member) =>
                resolveWorktreeCleanup(serverSettings, member.projectId).worktreeOnDelete &&
                worktreeOwner([member, ...group]) === member,
            ) ??
            worktreeOwner(group) ??
            group[0] ??
            deleted[0]);
      return owner === undefined ? [] : [owner];
    });
    for (const thread of candidates) {
      const settings = resolveWorktreeCleanup(serverSettings, thread.projectId);
      if (!worktreeCleanupEnabled(settings)) continue;
      const worktreePath = path.resolve(thread.worktreePath!);
      const group = groups.get(worktreePath) ?? [];
      const activityAt = Math.max(...group.map(worktreeActivityAt));
      const deleted = "workspaceRoot" in thread;
      const settlementChain = deleted
        ? []
        : worktreeSettlementChain(thread, threadsById, now, serverSettings);
      const settledThread = settlementChain.at(-1);
      const members = deleted ? [thread, ...group] : group;
      const project = deleted
        ? { workspaceRoot: thread.workspaceRoot }
        : snapshot.projects.find((entry) => entry.id === thread.projectId);
      const entry: StorageCleanupReportEntry = {
        kind: "worktree",
        outcome: "kept",
        reason: "",
        leaseId: generations.get(worktreePath) ?? undefined,
        path: worktreePath,
        projectPath: project?.workspaceRoot ?? null,
        threadId: ownershipErrors.has(worktreePath) ? null : thread.id,
        threadTitle: ownershipErrors.has(worktreePath) ? null : thread.title,
        bytes: null,
        files: null,
      };
      const keep = (reason: string) => {
        if (deleted && settings.worktreeOnDelete && !kept.includes(worktreePath))
          kept.push(worktreePath);
        entries.push({ ...entry, reason: sentence(reason) });
      };
      // Reasons a provider session stop or finished deletion work can clear.
      const waitForStop = (reason: string) => {
        if (deleted && settings.worktreeOnDelete) waiting = true;
        if (!kept.includes(worktreePath)) kept.push(worktreePath);
        return reason;
      };
      yield* Effect.gen(function* () {
        const pooled =
          (yield* treehouseLeases.knownPath(worktreePath)) ||
          (yield* TreehouseLeases.treehousePoolForPath(worktreePath)) !== null;
        if (pooled) {
          const released = yield* treehouseLeases.releasedThread({
            path: worktreePath,
            threadId: thread.id,
          });
          if (released !== null) {
            entries.push({
              ...entry,
              leaseId: released.record.lease.lease_id,
              reason: released.record.returned
                ? "Treehouse slot already returned; conversation and branch retained"
                : "Treehouse return needs operator verification",
            });
            return;
          }
          if (ownershipErrors.has(worktreePath))
            return keep("current Treehouse ownership could not be verified");
          const owned = yield* treehouseLeases
            .read(worktreePath)
            .pipe(Effect.orElseSucceed(() => null));
          if (owned === null) return keep("Treehouse lease could not be verified");
          if (owned.record.lease.lease_id !== generations.get(worktreePath))
            return keep("Treehouse lease generation changed since check");
        }
        if (!(yield* fs.exists(worktreePath))) return;
        if (deleted && !settings.worktreeOnDelete && !pooled) return keep("no rules apply");
        if (members.length > 1 && worktreeOwner(members) === undefined)
          return keep(`shared by ${members.length} threads`);
        if (project === undefined && !pooled) return keep("project is unavailable");
        if (
          (!pooled && thread.branch === null) ||
          !group.every((member) => storageCleanupThreadIdle(member, now, serverSettings))
        )
          return keep(waitForStop("thread is running or has pending work"));
        const familyBranchChanged = group.some((member) => member.branch !== thread.branch);
        if (!pooled && familyBranchChanged)
          return keep("a subagent thread's branch differs from this worktree's branch");
        if (hasTerminal(worktreePath)) return keep(waitForStop("open terminal"));
        // Compare canonical roots while allowing symlinked parents, never symlinked worktree directories.
        const realPath = yield* fs.realPath(worktreePath);
        const realParent = yield* fs.realPath(path.dirname(worktreePath));
        if (realPath !== path.join(realParent, path.basename(worktreePath)))
          return keep("worktree is a symbolic link");
        if (!pooled && !roots.some((root) => inside(root, realPath)))
          return keep("outside the managed worktree folder");
        if (
          yield* containsProjectRoot(
            worktreePath,
            project === undefined ? snapshot.projects : [project, ...snapshot.projects],
          )
        )
          return keep("contains a project checkout");
        if ((yield* fs.stat(path.join(worktreePath, ".git"))).type !== "File")
          return keep("not a linked worktree");
        const linkedMarker = yield* fs.readLink(path.join(worktreePath, ".git")).pipe(
          Effect.as(true),
          Effect.catchIf(
            (error) =>
              error.cause instanceof Error &&
              "code" in error.cause &&
              error.cause.code === "EINVAL",
            () => Effect.succeed(false),
          ),
        );
        if (linkedMarker) return keep("Git marker is a symbolic link");
        if (pooled) {
          const purged = yield* purgeExpired({
            owner: thread,
            path: worktreePath,
            leaseId: generations.get(worktreePath)!,
            settings,
            now,
            defaultBranches,
            refreshedDefaultRefs,
          });
          if (purged?.kind === "kept") return keep(waitForStop(purged.reason));
          if (purged?.kind === "removed") {
            entries.push({
              ...entry,
              projectPath: purged.repositoryPath,
              outcome: "removed",
              reason: "Expired checkout removed, including local work and ignored files",
              bytes: purged.bytes,
            });
            return;
          }
          return keep("no rules apply");
        }
        if (deleted && !settings.worktreeOnDelete) return keep("no rules apply");
        if (project === undefined) return keep("project is unavailable");
        if (familyBranchChanged || (yield* branchChanged(worktreePath, thread.branch)))
          return keep("repository or branch changed");
        if (thread.branch === null)
          return keep("checkout is detached or branch identity is unavailable");
        const changes = yield* localChangesReason(worktreePath, settings);
        if (changes !== null) return keep(changes);
        if (
          [...group, ...settlementChain.slice(1)].some(
            (member) =>
              member.settledAt !== null &&
              now - Math.max(DateTime.toEpochMillis(member.updatedAt), activityAt) <
                SETTLE_SCRIPT_GRACE_MS,
          )
        ) {
          yield* deferWorktree(
            Math.max(
              ...group.map(
                (member) =>
                  Math.max(DateTime.toEpochMillis(member.updatedAt), activityAt) +
                  SETTLE_SCRIPT_GRACE_MS,
              ),
            ),
          );
          return keep(waitForStop("settled moments ago"));
        }
        const head = yield* git.resolveCommit({ cwd: worktreePath, revision: "HEAD" });
        const old =
          !deleted &&
          settings.worktreeAfterDays !== null &&
          activityAt < now - settings.worktreeAfterDays * DAY_MS;
        const settledFor =
          settledThread?.settledAt == null
            ? null
            : now - Math.max(DateTime.toEpochMillis(settledThread.settledAt), activityAt);
        const settledDays = settledFor === null ? null : Math.floor(settledFor / DAY_MS);
        const settled =
          settledFor !== null &&
          settings.worktreeSettledAfterDays !== null &&
          settledFor >= settings.worktreeSettledAfterDays * DAY_MS &&
          (!pooled ||
            group.every(
              (member) =>
                member.settledAt !== null &&
                now - Math.max(DateTime.toEpochMillis(member.settledAt), activityAt) >=
                  settings.worktreeSettledAfterDays! * DAY_MS,
            ));
        let eligible = deleted || old || settled;
        let removalReason = deleted
          ? "thread was deleted"
          : old
            ? `inactive for ${Math.floor((now - activityAt) / DAY_MS)} days`
            : `settled for ${settledDays} days`;
        if (!eligible && (settings.worktreeUnchanged || settings.worktreeOnMerge)) {
          const repositoryCwd = path.resolve(project.workspaceRoot);
          const remotes = yield* git.execute({
            operation: "StorageCleanup.listRemotes",
            cwd: repositoryCwd,
            args: ["remote"],
          });
          const remote =
            remotes.stdout.trim() === ""
              ? null
              : yield* git.resolvePrimaryRemoteName(repositoryCwd);
          let branch: string | null | undefined;
          let defaultRef: string;
          if (remote === null) {
            if (!settings.worktreeUnchanged)
              return keep("no remote is configured for merged-worktree cleanup");
            const projectHead = yield* git.execute({
              operation: "StorageCleanup.resolveProjectBranch",
              cwd: repositoryCwd,
              args: ["symbolic-ref", "--quiet", "HEAD"],
              allowNonZeroExit: true,
            });
            if (projectHead.exitCode !== 0) return keep("project branch is unavailable");
            defaultRef = projectHead.stdout.trim();
            branch = defaultRef.slice("refs/heads/".length);
          } else {
            const defaultBranchKey = `${repositoryCwd}\0${remote}`;
            branch = defaultBranches.get(defaultBranchKey);
            if (branch === undefined) {
              branch = yield* git.resolveDefaultBranchName(repositoryCwd, remote);
              // Without a local HEAD ref, only the remote can identify its default;
              // a conventional name may be a feature branch.
              if (branch === null) {
                branch = yield* git
                  .execute({
                    operation: "StorageCleanup.resolveDefaultBranch",
                    cwd: repositoryCwd,
                    args: ["ls-remote", "--symref", "--", remote, "HEAD"],
                    timeoutMs: 20_000,
                  })
                  .pipe(
                    Effect.map(
                      (result) =>
                        /^ref: refs\/heads\/(.+)\tHEAD\r?$/m.exec(result.stdout)?.[1] ?? null,
                    ),
                    Effect.orElseSucceed(() => null),
                  );
              }
              defaultBranches.set(defaultBranchKey, branch);
            }
            if (branch === null) return keep("default branch is unavailable");
            defaultRef = `refs/remotes/${remote}/${branch}`;
            const refreshed = refreshedDefaultRefs.get(repositoryCwd) ?? new Set<string>();
            if (!refreshed.has(defaultRef)) {
              yield* git.fetchRemoteTrackingBranch({
                cwd: repositoryCwd,
                remoteName: remote,
                remoteBranch: branch,
              });
              refreshed.add(defaultRef);
              refreshedDefaultRefs.set(repositoryCwd, refreshed);
            }
          }
          const base = yield* git.resolveCommit({
            cwd: worktreePath,
            revision: defaultRef,
          });
          const ancestor = yield* git.execute({
            operation: "StorageCleanup.integratedBranch",
            cwd: worktreePath,
            args: ["merge-base", "--is-ancestor", head.commitSha, base.commitSha],
            allowNonZeroExit: true,
          });
          const integrated = ancestor.exitCode === 0;
          eligible = integrated && settings.worktreeUnchanged;
          if (eligible)
            removalReason =
              remote === null
                ? "no commits beyond the project branch"
                : "no commits beyond the default branch";
          if (!eligible && remote !== null && settings.worktreeOnMerge && thread.branch !== null) {
            const pullRequest = yield* gitManager.branchPullRequest(
              { cwd: worktreePath, branch: thread.branch },
              { refresh: true },
            );
            eligible = storageCleanupPullRequestMerged(pullRequest, {
              branch: thread.branch,
              defaultBranch: branch,
              headSha: head.commitSha,
              integrated,
            });
            if (eligible) removalReason = "pull request was merged";
          }
          if (!eligible && remote === null) return keep("has commits beyond the project branch");
        }
        if (!eligible)
          return keep(
            !deleted && settings.worktreeAfterDays !== null
              ? `inactive for ${Math.floor((now - activityAt) / DAY_MS)} of ${settings.worktreeAfterDays} days`
              : settings.worktreeSettledAfterDays !== null
                ? settledFor === null
                  ? "not settled"
                  : `settled for ${settledDays} of ${settings.worktreeSettledAfterDays} days`
                : settings.worktreeOnMerge
                  ? "not merged"
                  : "has commits beyond the default branch",
          );
        // Re-read this path's threads and sessions so new activity cancels removal; full per-worktree snapshots take seconds.
        const blockedSinceCheck = Effect.fnUntraced(function* () {
          const latestProjects = yield* projectStore.listShells();
          if (yield* containsProjectRoot(worktreePath, [project, ...latestProjects]))
            return "contains a project checkout";
          const latest = (yield* projections.getThreadWorktreePaths({
            includeArchived: true,
          })).filter((entry) => path.resolve(entry.worktreePath) === worktreePath);
          if (hasTerminal(worktreePath)) return waitForStop("open terminal");
          if (deleted) {
            if (
              !resolveWorktreeCleanup(yield* settingsService.getSettings, thread.projectId)
                .worktreeOnDelete
            )
              return "thread or cleanup settings changed since check";
            // Durable deletion cleanup must succeed or be explicitly cancelled before removing its checkout.
            const pendingCleanup = yield* sql<{ status: string }>`
              SELECT status FROM orchestration_v2_effect_outbox
              WHERE thread_id = ${thread.id} AND status NOT IN ('succeeded', 'cancelled')
            `.pipe(NodeSqliteClient.readOnly);
            // Failed effects never retry, so only running work can finish and free the path.
            if (pendingCleanup.some((row) => row.status !== "failed"))
              return waitForStop("thread deletion is still pending");
            if (pendingCleanup.length > 0) return "thread deletion is still pending";
          }
          const latestShells = yield* Effect.forEach(latest, (entry) =>
            projections.getThreadShell(entry.threadId),
          );
          const current = yield* treehouseLeases.currentThreads({
            path: worktreePath,
            threads: latestShells.filter((shell) => shell !== null),
          });
          if (pooled && current.leaseId !== generations.get(worktreePath))
            return "Treehouse lease generation changed since check";
          if (!current.verified) return "Treehouse thread ownership changed since check";
          const latestGroup = current.threads;
          const latestOwner = worktreeOwner(deleted ? [thread, ...latestGroup] : latestGroup);
          if (
            latestGroup.length !== group.length ||
            latestOwner?.id !== thread.id ||
            latestOwner?.branch !== thread.branch ||
            latestGroup.some((shell) => {
              const previous = group.find((member) => member.id === shell.id);
              return (
                previous === undefined ||
                shell.branch !== thread.branch ||
                !Equal.equals(shell.settledAt, previous.settledAt) ||
                (shell.settledAt !== null && !Equal.equals(shell.updatedAt, previous.updatedAt)) ||
                !storageCleanupThreadIdle(shell, now, serverSettings) ||
                path.resolve(shell.worktreePath!) !== worktreePath ||
                worktreeActivityAt(shell) !== worktreeActivityAt(previous)
              );
            })
          )
            return waitForStop("thread activity or shared worktree changed since check");
          if (pooled) {
            for (const shell of latestGroup) {
              const pending =
                yield* sql`SELECT 1 FROM orchestration_v2_effect_outbox WHERE thread_id = ${shell.id} AND status NOT IN ('succeeded', 'cancelled') LIMIT 1`.pipe(
                  NodeSqliteClient.readOnly,
                );
              if (pending.length > 0) return "thread effects are still pending";
            }
          }
          if (settlementChain.length > 1) {
            const ancestors = yield* Effect.forEach(settlementChain.slice(1), (ancestor) =>
              projections.getThreadShell(ancestor.id),
            );
            const latestSettlement = worktreeSettlementChain(
              latestGroup.find((shell) => shell.id === thread.id)!,
              new Map(
                [...latestGroup, ...ancestors.filter((ancestor) => ancestor !== null)].map(
                  (ancestor) => [ancestor.id, ancestor],
                ),
              ),
              now,
              serverSettings,
            ).at(-1);
            if (
              latestSettlement?.id !== settledThread?.id ||
              !Equal.equals(latestSettlement?.settledAt, settledThread?.settledAt) ||
              !Equal.equals(latestSettlement?.updatedAt, settledThread?.updatedAt)
            )
              return "parent settlement changed since check";
          }
          // Sessions can outlive their run and can be shared across app threads.
          // Ready persisted sessions may have released their runtime; only live sessions protect the path.
          for (const session of yield* sessionsInWorktree(worktreePath)) {
            if (yield* providerSessions.isLive(session.id))
              return waitForStop("provider session is still open");
          }
          return null;
        });
        const blocked = yield* blockedSinceCheck();
        if (blocked !== null) return keep(blocked);
        const bytes = yield* measureWorktree(worktreePath);
        const blockedWhileMeasuring = yield* blockedSinceCheck();
        if (blockedWhileMeasuring !== null) return keep(blockedWhileMeasuring);
        if (yield* branchChanged(worktreePath, thread.branch))
          return keep("repository or branch changed since check");
        if (
          (yield* git.resolveCommit({ cwd: worktreePath, revision: "HEAD" })).commitSha !==
          head.commitSha
        )
          return keep("commit changed since check");
        const finalChanges = yield* localChangesReason(worktreePath, settings);
        if (finalChanges !== null) return keep(finalChanges);
        const currentSettings = yield* settingsService.getSettings;
        const current = resolveWorktreeCleanup(currentSettings, thread.projectId);
        if (
          currentSettings.autoResumeLimitedThreads !== serverSettings.autoResumeLimitedThreads ||
          currentSettings.snoozeLimitedThreads !== serverSettings.snoozeLimitedThreads ||
          Object.keys(settings).some(
            (key) =>
              current[key as keyof typeof settings] !== settings[key as keyof typeof settings],
          )
        )
          return keep("settings changed since check");
        // Clean only untracked files. Ordinary worktrees retain Git's final tracked-edit check.
        if (settings.worktreeKeepWhen === "tracked-changes")
          yield* cleanupGit(worktreePath, ["clean", "-ffdx"], "git clean");
        const submodules = yield* submoduleRemoval(worktreePath, settings);
        if (submodules.reason !== null) return keep(submodules.reason);
        if (submodules.force) {
          const changes = yield* localChangesReason(worktreePath, settings);
          if (changes !== null) return keep(changes);
        }
        yield* removeLinkedWorktree({
          operation: "StorageCleanup.cleanupGit",
          cwd: project.workspaceRoot,
          worktreePath,
          // Git requires force for submodules; recursive checks above cannot make removal atomic.
          force: submodules.force,
          execute: git.execute,
          run: (args) => cleanupGit(project.workspaceRoot, [...args], "git worktree remove"),
        });
        entries.push({ ...entry, outcome: "removed", reason: sentence(removalReason), bytes });
        yield* gitManager.invalidateStatus(project.workspaceRoot);
        // Preserve branch/path so ProviderTurnStartService can recreate the checkout on resume.
        yield* Effect.logInfo("storage cleanup removed worktree", { threadId: thread.id });
      }).pipe(
        (effect) =>
          Effect.gen(function* () {
            const pooled =
              (yield* treehouseLeases.knownPath(worktreePath)) ||
              (yield* TreehouseLeases.treehousePoolForPath(worktreePath)) !== null;
            return yield* pooled ? effect : withWorkspaceLease(worktreePath, effect);
          }),
        Effect.catch((error) =>
          Effect.gen(function* () {
            entries.push({
              ...entry,
              outcome: "failed",
              reason: cleanupFailureReason(error),
            });
            if (deleted && settings.worktreeOnDelete) kept.push(worktreePath);
            yield* Effect.logWarning("storage cleanup failed for worktree", {
              threadId: thread.id,
              error,
            });
          }),
        ),
      );
    }
    if (heldInventory._tag === "Success") {
      for (const held of heldInventory.success) {
        if (entries.some((entry) => entry.path === held.path && entry.leaseId === held.leaseId))
          continue;
        const owner =
          generations.get(held.path) === held.leaseId
            ? (groups.get(held.path) ?? deletedByPath.get(held.path))?.[0]
            : undefined;
        entries.push({
          kind: "worktree",
          outcome: "kept",
          path: held.path,
          leaseId: held.leaseId ?? undefined,
          reason: !held.verified
            ? "Treehouse ownership record could not be verified"
            : owner === undefined
              ? "No verified thread owner in the cleanup snapshot"
              : !worktreeCleanupEnabled(resolveWorktreeCleanup(serverSettings, owner.projectId))
                ? "Worktree cleanup is disabled for this project"
                : "Allocation changed since the cleanup snapshot",
          projectPath: null,
          threadId: null,
          threadTitle: null,
          bytes: null,
          files: null,
        });
      }
    } else {
      entries.push({
        kind: "worktree",
        outcome: "failed",
        path: null,
        reason: "Held Treehouse allocation inventory could not be verified",
        projectPath: null,
        threadId: null,
        threadTitle: null,
        bytes: null,
        files: null,
      });
    }
    yield* reportOrphanWorktrees(
      roots,
      yield* projectStore.list({ includeDeleted: true }),
      entries,
    );
    return { waiting, kept };
  });

  const cleanFiles = Effect.fn("StorageCleanup.cleanFiles")(function* (
    root: string,
    days: number | null,
    now: number,
    rotatedLogs: boolean,
    removed: { files: number; bytes: number },
  ) {
    if (days === null || !(yield* fs.exists(root))) return removed;
    // A symlinked parent (a linked T3 home) is fine; a symlinked folder is not.
    const realRoot = yield* fs.realPath(root);
    const realParent = yield* fs.realPath(path.dirname(root));
    if (realRoot !== path.join(realParent, path.basename(root))) return removed;
    const visit = Effect.fn("StorageCleanup.visitFiles")(function* (
      directory: string,
    ): Effect.fn.Return<void, PlatformError | ServerSettingsError> {
      for (const name of yield* fs.readDirectory(directory)) {
        const target = path.join(directory, name);
        if ((yield* fs.realPath(target)) !== target || !inside(realRoot, target)) continue;
        const stat = yield* fs.stat(target);
        if (stat.type === "Directory" && rotatedLogs) {
          yield* visit(target);
        } else if (stat.type === "File" && (!rotatedLogs || /\.(?:log|ndjson)\.\d+$/.test(name))) {
          const modified = Option.getOrNull(stat.mtime);
          if (modified !== null && modified.getTime() < now - days * DAY_MS) {
            const current = (yield* settingsService.getSettings).storageCleanup;
            if ((rotatedLogs ? current.logsAfterDays : current.browserArtifactsAfterDays) !== days)
              return;
            yield* fs.remove(target);
            removed.files++;
            removed.bytes += Number(stat.size);
          }
        }
      }
    });
    yield* visit(realRoot);
    return removed;
  });

  const reportRef = yield* SubscriptionRef.make<StorageCleanupReport | null>(null);
  // Only deleted threads' worktrees wait on session stops: any stop while one waits, or a stop inside one kept for another reason.
  // A deletion stays pending until a sweep that started after it finishes.
  let deletedWorktreeWaits = true;
  let keptWorktrees: ReadonlyArray<string> = [];
  let deletions = 0;
  let sweptDeletions = 0;
  const sweep = Effect.fn("StorageCleanup.sweep")(function* (
    trigger: StorageCleanupReport["trigger"],
  ) {
    const deletionsAtStart = deletions;
    yield* SubscriptionRef.set(worktreeRetryAt, null);
    const now = yield* Clock.currentTimeMillis;
    const serverSettings = yield* settingsService.getSettings;
    const settings = serverSettings.storageCleanup;
    const entries: StorageCleanupReportEntry[] = [];
    yield* expireLeases({ settings: serverSettings, now, entries }).pipe(
      Effect.catch((cause) =>
        Effect.gen(function* () {
          yield* deferWorktree(now + 300000);
          yield* Effect.gen(function* () {
            for (const task of yield* leaseQueue.list) {
              if (task.state === "removed" || task.state === "released") continue;
              yield* leaseQueue.update({
                task,
                state: "blocked",
                reason:
                  "Lease reconciliation failed; discovery or responsibility could not be verified",
                now,
              });
            }
          }).pipe(
            Effect.catch((queueError) =>
              Effect.logWarning("cannot persist lease reconciliation failure", { queueError }),
            ),
          );
          entries.push({
            kind: "worktree",
            outcome: "failed",
            reason: "Lease cleanup reconciliation failed; persisted requests remain pending",
            path: null,
            projectPath: null,
            threadId: null,
            threadTitle: null,
            bytes: null,
            files: null,
          });
          yield* Effect.logWarning("lease cleanup reconciliation failed", { cause });
        }),
      ),
    );
    const deletedWorktrees = yield* cleanWorktrees(serverSettings, now, entries).pipe(
      Effect.catch((error) =>
        Effect.gen(function* () {
          entries.push({
            kind: "worktree",
            outcome: "failed",
            reason: cleanupFailureReason(error),
            path: null,
            projectPath: null,
            threadId: null,
            threadTitle: null,
            bytes: null,
            files: null,
          });
          yield* Effect.logWarning("worktree cleanup failed", { error });
          return { waiting: true, kept: [] };
        }),
      ),
    );
    deletedWorktreeWaits = deletedWorktrees.waiting;
    keptWorktrees = deletedWorktrees.kept;
    sweptDeletions = deletionsAtStart;
    for (const category of [
      {
        kind: "browser-artifacts" as const,
        root: config.browserArtifactsDir,
        days: settings.browserArtifactsAfterDays,
        label: "browser artifacts",
      },
      {
        kind: "logs" as const,
        root: config.logsDir,
        days: settings.logsAfterDays,
        label: "rotated logs",
      },
    ]) {
      if (category.days === null) continue;
      const removed = { files: 0, bytes: 0 };
      yield* cleanFiles(category.root, category.days, now, category.kind === "logs", removed).pipe(
        Effect.map(({ files, bytes }) =>
          entries.push({
            kind: category.kind,
            outcome: files > 0 ? "removed" : "kept",
            reason: files === 0 ? "No expired files" : `Removed ${files} ${category.label}`,
            path: null,
            projectPath: null,
            threadId: null,
            threadTitle: null,
            bytes: files > 0 ? bytes : null,
            files: files > 0 ? files : null,
          }),
        ),
        Effect.catch((error) =>
          Effect.gen(function* () {
            entries.push({
              kind: category.kind,
              outcome: "failed",
              reason: cleanupFailureReason(error),
              path: null,
              projectPath: null,
              threadId: null,
              threadTitle: null,
              bytes: removed.files > 0 ? removed.bytes : null,
              files: removed.files > 0 ? removed.files : null,
            });
            yield* Effect.logWarning("storage file cleanup failed", { kind: category.kind, error });
          }),
        ),
      );
    }
    const counts = { removed: 0, kept: 0, failed: 0 };
    let bytesFreed = 0;
    for (const entry of entries) {
      if (!entry.returned) counts[entry.outcome]++;
      if (entry.outcome !== "kept") bytesFreed += entry.bytes ?? 0;
    }
    const priority = (entry: StorageCleanupReportEntry) =>
      entry.outcome === "failed"
        ? 0
        : entry.outcome === "removed"
          ? 1
          : entry.kind === "worktree" && entry.threadId === null
            ? 2
            : 3;
    const latestReport: StorageCleanupReport = {
      trigger,
      startedAt: DateTime.formatIso(DateTime.makeUnsafe(now)),
      finishedAt: DateTime.formatIso(yield* DateTime.now),
      entries: entries.sort((a, b) => priority(a) - priority(b)).slice(0, REPORT_ENTRY_LIMIT),
      counts,
      bytesFreed,
      omittedCount: Math.max(0, entries.length - REPORT_ENTRY_LIMIT),
      returnedCount: entries.filter((entry) => entry.returned).length,
    };
    yield* SubscriptionRef.set(reportRef, latestReport);
    return latestReport;
  });
  const automaticSweepQueued = yield* Ref.make(false);
  const worker = yield* makeDrainableWorker(
    (completion: Deferred.Deferred<StorageCleanupReport, ServerSettingsError> | undefined) =>
      (completion === undefined ? Ref.set(automaticSweepQueued, false) : Effect.void).pipe(
        Effect.andThen(sweep(completion === undefined ? "automatic" : "manual")),
        Effect.exit,
        Effect.flatMap((exit) =>
          completion === undefined
            ? exit.pipe(
                Effect.asVoid,
                Effect.catchCause((cause) =>
                  Effect.logWarning("storage cleanup failed", { cause }),
                ),
              )
            : Deferred.done(completion, exit).pipe(Effect.asVoid),
        ),
      ),
  );
  const requestSweep = Ref.getAndSet(automaticSweepQueued, true).pipe(
    Effect.flatMap((queued) => (queued ? Effect.void : worker.enqueue(undefined))),
  );
  const runNow = Effect.gen(function* () {
    const completion = yield* Deferred.make<StorageCleanupReport, ServerSettingsError>();
    yield* worker.enqueue(completion);
    return yield* Deferred.await(completion);
  });

  const start = Effect.fn("StorageCleanup.start")(function* () {
    const unsubscribe = yield* terminals.subscribeMetadata((event) =>
      Effect.sync(() => {
        const blockedBefore = keptWorktrees.filter(hasTerminal);
        if (event.type === "snapshot") {
          liveTerminals.clear();
          for (const terminal of event.terminals) noteTerminal(terminal);
        } else if (event.type === "upsert") {
          noteTerminal(event.terminal);
        } else {
          const threadTerminals = liveTerminals.get(event.threadId);
          threadTerminals?.delete(event.terminalId);
          if (threadTerminals?.size === 0) liveTerminals.delete(event.threadId);
        }
        return blockedBefore.some((worktreePath) => !hasTerminal(worktreePath));
      }).pipe(Effect.andThen((changed) => (changed ? requestSweep : Effect.void))),
    );
    yield* Effect.addFinalizer(() => Effect.sync(unsubscribe));
    const changes = yield* settingsService.subscribeChanges;
    const events = engine.streamDomainEvents;
    let lastSettings = yield* settingsService.getSettings.pipe(Effect.orDie);
    yield* forkParked(
      requestSweep.pipe(
        Effect.andThen(worker.drain),
        Effect.repeat(Schedule.spaced("1 hour")),
        Effect.asVoid,
      ),
    );
    yield* forkParked(
      Stream.runForEach(changes, (settings) => {
        if (
          Equal.equals(settings.storageCleanup, lastSettings.storageCleanup) &&
          Equal.equals(settings.worktreeCleanup, lastSettings.worktreeCleanup) &&
          sameProjectWorktreePolicies(settings, lastSettings)
        )
          return Effect.void;
        lastSettings = settings;
        return requestSweep;
      }),
    );
    yield* forkParked(
      SubscriptionRef.changes(worktreeRetryAt).pipe(
        Stream.changes,
        Stream.switchMap((at) =>
          at === null
            ? Stream.empty
            : Stream.fromEffect(
                Effect.gen(function* () {
                  yield* Effect.sleep(Math.max(0, at - (yield* Clock.currentTimeMillis)));
                  yield* requestSweep.pipe(Effect.uninterruptible);
                }),
              ),
        ),
        Stream.runDrain,
      ),
    );
    yield* forkParked(
      Stream.runForEach(events, (event) => {
        if (
          event.type === "thread.settled" ||
          (event.type === "subagent.updated" &&
            ["completed", "failed", "cancelled", "interrupted"].includes(event.payload.status)) ||
          (event.type === "run.updated" &&
            ["completed", "failed", "interrupted", "cancelled", "rolled_back"].includes(
              event.payload.status,
            )) ||
          (event.type === "thread.pull-request-link-updated" &&
            event.payload.snapshot?.state === "merged")
        )
          return anyWorktreePolicy(lastSettings, worktreeCleanupEnabled)
            ? requestSweep
            : Effect.void;
        if (event.type === "thread.deleted") deletions++;
        else if (
          event.type !== "provider-session.updated" ||
          (event.payload.status !== "stopped" && event.payload.status !== "error")
        )
          return Effect.void;
        else if (!deletedWorktreeWaits && deletions === sweptDeletions) {
          const cwd = pathKey(event.payload.cwd);
          if (
            !keptWorktrees.some(
              (worktreePath) => pathKey(worktreePath) === cwd || inside(worktreePath, cwd),
            )
          )
            return Effect.void;
        }
        return anyWorktreePolicy(lastSettings, worktreeCleanupEnabled) ? requestSweep : Effect.void;
      }).pipe(
        Effect.catchCause((cause) =>
          Effect.logWarning("Storage cleanup event stream failed", { cause }),
        ),
      ),
    );
  });
  yield* start();
  return StorageCleanup.of({
    runNow,
    latestReport: SubscriptionRef.get(reportRef),
    leaseTasks: leaseQueue.list.pipe(
      Effect.mapError(
        (cause) =>
          new ServerSettingsError({
            settingsPath: config.settingsPath,
            operation: "read-file",
            cause,
          }),
      ),
    ),
    reports: SubscriptionRef.changes(reportRef),
    drain: worker.drain,
  });
});

export const layer = Layer.effect(StorageCleanup, make);

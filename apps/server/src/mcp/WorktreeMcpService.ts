import {
  CommandId,
  type GitCommandError,
  MessageId,
  WorkspaceOperation,
  type ProjectId,
  WorktreeMcpFailure,
  type WorktreeMcpContinuationStatus,
  type WorktreeMcpHandoffInput,
  type WorktreeMcpHandoffResult,
  type WorktreeMcpSetupScriptStatus,
  type WorktreeMcpStatusResult,
  type WorktreeMcpReconcileBranchInput,
  type WorktreeMcpReconcileBranchResult,
  type VcsListRefsInput,
  type VcsListRefsResult,
} from "@t3tools/contracts";
import { resolveProjectSettings } from "@t3tools/shared/projectSettings";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as SecureRandom from "effect/crypto/SecureRandom";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import type * as PlatformError from "effect/PlatformError";

import * as GitWorkflowService from "../git/GitWorkflowService.ts";
import * as ThreadManagementService from "../orchestration-v2/ThreadManagementService.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as ProjectSetupScriptRunner from "../project/ProjectSetupScriptRunner.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as VcsStatusBroadcaster from "../vcs/VcsStatusBroadcaster.ts";
import * as WorkspaceAccess from "../workspace/WorkspaceAccess.ts";
import { treehousePoolForPath } from "../workspace/treehouseLeases.ts";
import * as WorktreeBranchReconciliation from "../workspace/WorktreeBranchReconciliation.ts";
import type { McpInvocationScope, McpThreadInvocationScope } from "./McpInvocationContext.ts";

interface ReconcileWorktreeBranchOptions {
  readonly scope: McpInvocationScope;
  readonly input: WorktreeMcpReconcileBranchInput;
}

export class WorktreeMcpService extends Context.Service<
  WorktreeMcpService,
  {
    readonly handoff: (
      scope: McpInvocationScope,
      input: WorktreeMcpHandoffInput,
    ) => Effect.Effect<WorktreeMcpHandoffResult, WorktreeMcpFailure>;
    readonly reconcileBranch: (
      options: ReconcileWorktreeBranchOptions,
    ) => Effect.Effect<WorktreeMcpReconcileBranchResult, WorktreeMcpFailure>;
    readonly status: (
      scope: McpInvocationScope,
    ) => Effect.Effect<WorktreeMcpStatusResult, WorktreeMcpFailure>;
    readonly listRefs: (
      input: Omit<VcsListRefsInput, "cwd"> & {
        readonly worktreePath: string | null;
        readonly workspaceRoot: string;
      },
    ) => Effect.Effect<VcsListRefsResult, GitCommandError | PlatformError.PlatformError>;
  }
>()("t3/mcp/WorktreeMcpService") {}

function failure(code: WorktreeMcpFailure["code"], message: string): WorktreeMcpFailure {
  return new WorktreeMcpFailure({ code, message });
}

function errorMessage(error: unknown): string {
  if (error instanceof Error) return error.message;
  if (typeof error === "object" && error !== null && "message" in error) {
    return String((error as { message: unknown }).message);
  }
  return String(error);
}

const isWorktreeMcpFailure = Schema.is(WorktreeMcpFailure);
const asOperationFailed = (prefix: string) =>
  Effect.mapError((error: unknown) =>
    isWorktreeMcpFailure(error)
      ? error
      : failure("operation_failed", `${prefix}: ${errorMessage(error)}`),
  );

const make = Effect.gen(function* () {
  const random = yield* SecureRandom.SecureRandom;
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const threadManagement = yield* ThreadManagementService.ThreadManagementService;
  const projects = yield* ProjectService.ProjectService;
  const serverSettings = yield* ServerSettings.ServerSettingsService;
  const gitWorkflow = yield* GitWorkflowService.GitWorkflowService;
  const setupScriptRunner = yield* ProjectSetupScriptRunner.ProjectSetupScriptRunner;
  const vcsStatusBroadcaster = yield* VcsStatusBroadcaster.VcsStatusBroadcaster;
  const access = yield* WorkspaceAccess.WorkspaceAccess.pipe(Effect.provide(WorkspaceAccess.layer));
  const branchReconciliation = yield* WorktreeBranchReconciliation.WorktreeBranchReconciliation;

  // Serialize handoffs per thread to avoid creating two worktrees and orphaning one.
  const handoffThreadsInFlight = new Set<string>();

  const requireThreadScope = (scope: McpInvocationScope) =>
    scope.thread === undefined
      ? Effect.fail(
          failure(
            "thread_credential_required",
            "Worktree handoff and status act as the calling T3 thread, so they need an agent running inside T3 Code.",
          ),
        )
      : Effect.succeed(scope as McpThreadInvocationScope);

  const requireCapability = (scope: McpInvocationScope) =>
    scope.capabilities.has("worktree")
      ? Effect.void
      : Effect.fail(
          failure("capability_denied", "This MCP credential does not grant worktree capabilities."),
        );

  const loadThread = (scope: McpThreadInvocationScope) =>
    threadManagement.getThreadRecords(scope.thread.threadId, []).pipe(
      Effect.mapError((error) =>
        error._tag === "OrchestratorProjectionError"
          ? failure("thread_not_found", `Thread '${scope.thread.threadId}' was not found.`)
          : failure(
              "operation_failed",
              `Unable to read thread ${scope.thread.threadId}: ${errorMessage(error)}`,
            ),
      ),
      Effect.filterOrFail(
        (projection) => projection.thread.deletedAt === null,
        () => failure("thread_not_found", `Thread '${scope.thread.threadId}' was not found.`),
      ),
    );

  const loadProject = (scope: McpThreadInvocationScope, projectId: ProjectId) =>
    projects.getById(projectId).pipe(
      asOperationFailed(`Unable to read project ${projectId}`),
      Effect.flatMap(
        Option.match({
          onNone: () =>
            Effect.fail(
              failure(
                "project_not_found",
                `Project '${projectId}' was not found for thread '${scope.thread.threadId}'.`,
              ),
            ),
          onSome: Effect.succeed,
        }),
      ),
    );

  const readDefaultStartFromOrigin = (projectId: ProjectId) =>
    serverSettings.getSettings.pipe(
      Effect.map(
        (settings) =>
          resolveProjectSettings(settings, projectId).settings.newWorktreesStartFromOrigin,
      ),
      asOperationFailed("Unable to read server settings"),
    );

  const handoffIds = (scope: McpThreadInvocationScope) =>
    random.uuidV4().pipe(
      Effect.map((uuid) => {
        const part = (kind: string, operation: string) =>
          [kind, "mcp", encodeURIComponent(scope.thread.providerSessionId), operation, uuid].join(
            ":",
          );
        return {
          commandId: CommandId.make(part("command", "worktree-handoff")),
          continuationCommandId: CommandId.make(part("command", "worktree-continuation")),
          continuationMessageId: MessageId.make(part("message", "worktree-continuation")),
        };
      }),
      Effect.orDie,
    );

  const performHandoff = Effect.fn("WorktreeMcpService.performHandoff")(function* (
    scope: McpThreadInvocationScope,
    input: WorktreeMcpHandoffInput,
  ) {
    const alreadyInWorktree = (worktreePath: string) =>
      failure(
        "already_in_worktree",
        `Thread '${scope.thread.threadId}' is already attached to worktree '${worktreePath}'. Read t3_worktree_status and pass expectedWorktreePath to replace it in this conversation.`,
      );

    const projection = yield* loadThread(scope);
    const previousPath = projection.thread.worktreePath;
    if (input.expectedWorktreePath === undefined && previousPath !== null) {
      return yield* alreadyInWorktree(previousPath);
    }
    if (input.expectedWorktreePath !== undefined && input.expectedWorktreePath !== previousPath) {
      return yield* failure(
        "invalid_request",
        "The checkout binding changed. Read t3_worktree_status before retrying.",
      );
    }
    if (
      input.existingWorktreePath !== undefined &&
      (input.baseRef !== undefined || input.startFromOrigin !== undefined)
    ) {
      return yield* failure(
        "invalid_request",
        "baseRef and startFromOrigin apply only to a new worktree.",
      );
    }
    if (projection.thread.archivedAt !== null) {
      return yield* failure(
        "invalid_request",
        `Thread '${scope.thread.threadId}' is archived and cannot be handed off to a worktree.`,
      );
    }

    const project = yield* loadProject(scope, projection.thread.projectId);
    const projectCwd = project.workspaceRoot;

    const localStatus = yield* gitWorkflow
      .localStatus({ cwd: projectCwd })
      .pipe(asOperationFailed("Unable to read git status"));
    if (!localStatus.isRepo) {
      return yield* failure(
        "invalid_request",
        `Project workspace '${projectCwd}' is not a git repository.`,
      );
    }

    const localBranchNames =
      input.existingWorktreePath === undefined
        ? yield* gitWorkflow
            .listLocalBranchNames(projectCwd)
            .pipe(asOperationFailed("Unable to list branches"))
        : [];
    if (localBranchNames.includes(input.branch)) {
      const existingRef = yield* gitWorkflow
        .listRefs({ cwd: projectCwd, query: input.branch, refKind: "local" })
        .pipe(
          Effect.map((result) =>
            result.refs.find((ref) => ref.name === input.branch && ref.isRemote !== true),
          ),
          Effect.orElseSucceed(() => undefined),
        );
      const checkoutPath = existingRef?.worktreePath ?? null;
      return yield* failure(
        "invalid_request",
        `Branch '${input.branch}' already exists${
          checkoutPath === null ? "" : ` and is checked out at '${checkoutPath}'`
        }. ${checkoutPath === null ? "Choose a different branch name." : "Pass existingWorktreePath to attach that checkout, or choose a different branch name."}`,
      );
    }

    const baseRef =
      input.existingWorktreePath === undefined
        ? (input.baseRef ?? projection.thread.branch ?? localStatus.refName)
        : input.branch;
    if (baseRef === null) {
      return yield* failure(
        "invalid_request",
        "Could not determine the current branch of the project workspace (detached HEAD?). Pass baseRef explicitly.",
      );
    }

    const startFromOrigin =
      input.existingWorktreePath === undefined &&
      (input.startFromOrigin ?? (yield* readDefaultStartFromOrigin(project.id)));

    let worktreeBaseRef = baseRef;
    let baseRefName = baseRef;
    let startedFromOrigin = false;
    if (startFromOrigin) {
      const resolvedRemoteBase = yield* gitWorkflow
        .fetchWorktreeBaseCommit({ cwd: projectCwd, refName: baseRef })
        .pipe(asOperationFailed(`Unable to fetch the remote base of '${baseRef}'`));
      if (resolvedRemoteBase !== null) {
        worktreeBaseRef = resolvedRemoteBase.commitSha;
        baseRefName = `refs/remotes/${resolvedRemoteBase.remoteRefName}`;
        startedFromOrigin = true;
      }
    }

    const ids = yield* handoffIds(scope);
    const inspectExisting = Effect.fn("WorktreeMcpService.inspectExisting")(function* (
      target: string,
    ) {
      const canonical = yield* fileSystem
        .realPath(target)
        .pipe(asOperationFailed("Unable to resolve the destination checkout"));
      if (canonical === previousPath)
        return yield* failure("invalid_request", "This checkout is already selected.");
      if (
        (yield* treehousePoolForPath(canonical).pipe(
          Effect.provideService(FileSystem.FileSystem, fileSystem),
          Effect.provideService(Path.Path, path),
          asOperationFailed("Unable to verify destination lease ownership"),
        )) !== null
      )
        return yield* failure(
          "invalid_request",
          "Existing Treehouse slots cannot be adopted. Create a fresh managed checkout instead.",
        );
      const refs = yield* gitWorkflow
        .listRefs({ cwd: projectCwd, query: input.branch, refKind: "local" })
        .pipe(asOperationFailed("Unable to verify the destination repository"));
      const ref = refs.refs.find((ref) => ref.name === input.branch && ref.isRemote !== true);
      if (
        ref?.worktreePath == null ||
        (yield* fileSystem
          .realPath(ref.worktreePath)
          .pipe(asOperationFailed("Unable to verify the destination checkout"))) !== canonical
      )
        return yield* failure(
          "invalid_request",
          "The destination must be a checkout of the requested branch in this project.",
        );
      const snapshot = yield* threadManagement
        .getShellSnapshot()
        .pipe(asOperationFailed("Unable to inspect checkout ownership"));
      const projectRoots = new Map<ProjectId, string>([[project.id, projectCwd]]);
      const canonicalPaths = new Map<string, string | null>();
      for (const thread of [...snapshot.threads, ...snapshot.archivedThreads]) {
        if (thread.id === scope.thread.threadId) continue;
        let otherPath = thread.worktreePath;
        if (otherPath === null) {
          otherPath = projectRoots.get(thread.projectId) ?? null;
          if (otherPath === null) {
            const otherProject = yield* loadProject(scope, thread.projectId);
            otherPath = otherProject.workspaceRoot;
            projectRoots.set(thread.projectId, otherPath);
          }
        }
        if (!canonicalPaths.has(otherPath)) {
          canonicalPaths.set(
            otherPath,
            yield* fileSystem.realPath(otherPath).pipe(
              Effect.catchIf(
                (error) => error.reason._tag === "NotFound",
                () => Effect.succeed(null),
              ),
              asOperationFailed("Unable to inspect checkout ownership"),
            ),
          );
        }
        const otherCanonical = canonicalPaths.get(otherPath);
        if (otherCanonical === canonical)
          return yield* failure(
            "invalid_request",
            "Another thread owns the destination checkout. Use a fresh worktree instead.",
          );
      }
      const branch = yield* gitWorkflow
        .currentBranch(canonical)
        .pipe(asOperationFailed("Unable to inspect the destination branch"));
      if (branch !== input.branch)
        return yield* failure(
          "invalid_request",
          "The destination branch changed. Inspect the checkout before retrying.",
        );
      return { worktree: { path: canonical, refName: input.branch } };
    });

    // Mask interruption after worktree creation through setup; binding can detach and interrupt the caller.
    const handoffEffect = Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        const worktree = yield* restore(
          input.existingWorktreePath === undefined
            ? gitWorkflow
                .createWorktree(
                  {
                    cwd: projectCwd,
                    refName: worktreeBaseRef,
                    newRefName: input.branch,
                    baseRefName,
                    path: null,
                  },
                  { preparationScope: yield* Effect.scope },
                )
                .pipe(asOperationFailed("Unable to create the worktree"))
            : inspectExisting(input.existingWorktreePath),
        );
        const worktreePath = worktree.worktree.path;

        const reportFailed = (logMessage: string) =>
          Effect.catchCause((cause: Cause.Cause<unknown>) => {
            const detail = errorMessage(Cause.squash(cause));
            return Effect.logWarning(logMessage, {
              threadId: scope.thread.threadId,
              worktreePath,
              detail,
            }).pipe(Effect.as({ status: "failed", detail } as const));
          });

        const removeCreatedWorktree = Effect.suspend(() =>
          input.existingWorktreePath !== undefined
            ? Effect.void
            : gitWorkflow.removeWorktree({ cwd: projectCwd, path: worktreePath, force: true }).pipe(
                Effect.andThen(
                  Effect.suspend(() =>
                    gitWorkflow.deleteLocalBranch({
                      cwd: projectCwd,
                      refName: worktree.worktree.refName,
                      force: true,
                    }),
                  ),
                ),
              ),
        ).pipe(Effect.ignoreCause({ log: true }));

        const recheckAndBind = Effect.gen(function* () {
          // A concurrent UI binding may have landed during git work; do not orphan a second worktree.
          const recheck = yield* loadThread(scope);
          if (
            previousPath === null &&
            recheck.thread.worktreePath !== null &&
            input.expectedWorktreePath === undefined
          )
            return yield* alreadyInWorktree(recheck.thread.worktreePath);
          if (
            recheck.thread.worktreePath !== previousPath ||
            recheck.thread.branch !== projection.thread.branch
          ) {
            return yield* failure(
              "invalid_request",
              "The checkout binding changed while preparing the destination. Read t3_worktree_status before retrying.",
            );
          }
          if (input.existingWorktreePath !== undefined) {
            const destination = yield* inspectExisting(input.existingWorktreePath);
            if (destination.worktree.path !== worktreePath)
              return yield* failure(
                "invalid_request",
                "The destination checkout moved while preparing the handoff. Inspect it before retrying.",
              );
          }
          if (recheck.thread.archivedAt !== null) {
            return yield* failure(
              "invalid_request",
              `Thread '${scope.thread.threadId}' was archived while the worktree was being created; the handoff was rolled back.`,
            );
          }
          yield* threadManagement
            .dispatch({
              type: "thread.metadata.update",
              commandId: ids.commandId,
              threadId: scope.thread.threadId,
              branch: worktree.worktree.refName,
              worktreePath,
              expectedWorktreePath: previousPath,
              expectedBranch: projection.thread.branch,
            })
            .pipe(
              Effect.catchCause((cause) =>
                Cause.hasInterruptsOnly(cause)
                  ? Effect.failCause(cause as Cause.Cause<never>)
                  : Effect.fail(
                      failure(
                        "operation_failed",
                        `Unable to re-point the thread at the worktree: ${errorMessage(Cause.squash(cause))}`,
                      ),
                    ),
              ),
            );
        }).pipe(
          // Do not remove on interruption alone; the thread binding may already have committed.
          Effect.onError((cause) =>
            Cause.hasInterruptsOnly(cause) ? Effect.void : removeCreatedWorktree,
          ),
        );

        // Queue continuation immediately after binding; detachment terminates the caller, so relaunch needs a durable message.
        const queueContinuation: Effect.Effect<WorktreeMcpContinuationStatus, WorktreeMcpFailure> =
          Effect.suspend(() =>
            input.continuationPrompt === undefined
              ? Effect.succeed<WorktreeMcpContinuationStatus>({ status: "skipped" })
              : threadManagement
                  .sendToThread({
                    projectId: projection.thread.projectId,
                    commandId: ids.continuationCommandId,
                    threadId: scope.thread.threadId,
                    messageId: ids.continuationMessageId,
                    text: input.continuationPrompt,
                    attachments: [],
                    mode: "queue",
                    createdBy: "agent",
                    creationSource: "mcp",
                  })
                  .pipe(
                    Effect.map((sendResult): WorktreeMcpContinuationStatus => ({
                      status: "scheduled",
                      delivery: sendResult.delivery,
                    })),
                    reportFailed("worktree handoff continuation failed to queue"),
                  ),
          );

        const continuation = yield* recheckAndBind.pipe(Effect.andThen(queueContinuation));

        yield* access
          .useThread({
            threadId: scope.thread.threadId,
            cwd: worktreePath,
            targets: [],
            effect: vcsStatusBroadcaster.refreshStatus(worktreePath),
          })
          .pipe(Effect.ignoreCause({ log: true }), Effect.forkDetach);

        let setupScript: WorktreeMcpSetupScriptStatus = { status: "skipped" };
        if (input.runSetupScript ?? input.existingWorktreePath === undefined) {
          setupScript = yield* setupScriptRunner
            .runForThread({
              threadId: scope.thread.threadId,
              projectId: projection.thread.projectId,
              projectCwd,
              worktreePath,
              project: {
                id: project.id,
                workspaceRoot: project.workspaceRoot,
                scripts: project.scripts,
              },
            })
            .pipe(
              Effect.map((result): WorktreeMcpSetupScriptStatus =>
                result.status === "started"
                  ? {
                      status: "started",
                      scriptName: result.scriptName,
                      terminalId: result.terminalId,
                    }
                  : result.status === "skipped-for-link"
                    ? { status: "skipped" }
                    : { status: "no-script" },
              ),
              reportFailed("worktree handoff setup script failed"),
            );
        }

        const result: WorktreeMcpHandoffResult = {
          worktreePath,
          branch: worktree.worktree.refName,
          baseRef,
          startedFromOrigin,
          setupScript,
          continuation,
          note:
            continuation.status === "scheduled"
              ? "Handoff recorded. Changing the workspace detaches this provider session, so the current turn ends shortly after this call; the queued continuation prompt then starts the next turn inside the worktree with the conversation preserved. The worktree is not removed automatically when the thread is deleted."
              : "Handoff recorded. Changing the workspace detaches this provider session, so the current turn ends shortly after this call; the conversation continues inside the worktree when the thread receives its next message. Pass continuationPrompt to resume automatically. The worktree is not removed automatically when the thread is deleted.",
        };
        return result;
      }),
    );
    return yield* input.existingWorktreePath === undefined
      ? handoffEffect
      : access
          .useThread({
            threadId: scope.thread.threadId,
            cwd: input.existingWorktreePath,
            targets: [],
            effect: handoffEffect,
          })
          .pipe(
            Effect.provideService(WorkspaceOperation, undefined),
            asOperationFailed("Destination workspace unavailable"),
          );
  }, Effect.scoped);

  const handoff: WorktreeMcpService["Service"]["handoff"] = Effect.fn("WorktreeMcpService.handoff")(
    function* (callerScope, input) {
      yield* requireCapability(callerScope);
      const scope = yield* requireThreadScope(callerScope);
      // Acquire and register cleanup atomically so cancellation cannot strand the guard until restart.
      return yield* Effect.uninterruptibleMask((restore) =>
        Effect.suspend(() => {
          if (handoffThreadsInFlight.has(scope.thread.threadId)) {
            return Effect.fail(
              failure(
                "handoff_in_progress",
                `A worktree handoff is already in progress for thread '${scope.thread.threadId}'.`,
              ),
            );
          }
          handoffThreadsInFlight.add(scope.thread.threadId);
          return restore(performHandoff(scope, input)).pipe(
            Effect.ensuring(
              Effect.sync(() => handoffThreadsInFlight.delete(scope.thread.threadId)),
            ),
          );
        }),
      );
    },
  );

  const reconcileBranch: WorktreeMcpService["Service"]["reconcileBranch"] = Effect.fn(
    "WorktreeMcpService.reconcileBranch",
  )(function* ({ scope, input }) {
    yield* requireCapability(scope);
    yield* requireThreadScope(scope);
    if (!scope.capabilities.has("orchestration"))
      return yield* failure(
        "capability_denied",
        "Branch reconciliation requires orchestration capabilities.",
      );
    return yield* branchReconciliation
      .reconcile({
        threadId: input.threadId,
        expected: {
          ...input.expected,
          members: input.expected.threadIds.map((threadId) => ({
            threadId,
            branch: input.expected.recordedBranch,
          })),
        },
      })
      .pipe(
        Effect.mapError((cause) =>
          failure(
            cause.reason === "changed" ? "invalid_request" : "operation_failed",
            cause.message,
          ),
        ),
      );
  });

  const status: WorktreeMcpService["Service"]["status"] = Effect.fn("WorktreeMcpService.status")(
    function* (callerScope) {
      yield* requireCapability(callerScope);
      const scope = yield* requireThreadScope(callerScope);
      const projection = yield* loadThread(scope);
      const project = yield* loadProject(scope, projection.thread.projectId);

      const defaultStartFromOrigin = yield* readDefaultStartFromOrigin(project.id);

      const result: WorktreeMcpStatusResult = {
        attached: projection.thread.worktreePath !== null,
        worktreePath: projection.thread.worktreePath,
        branch: projection.thread.branch,
        projectWorkspaceRoot: project.workspaceRoot,
        defaultStartFromOrigin,
      };
      return result;
    },
  );

  const listRefs: WorktreeMcpService["Service"]["listRefs"] = Effect.fn(
    "WorktreeMcpService.listRefs",
  )(function* ({ worktreePath, workspaceRoot, ...refs }) {
    // Storage cleanup removes a worktree but keeps its path on the thread.
    const cwd =
      worktreePath !== null && (yield* fileSystem.exists(worktreePath))
        ? worktreePath
        : workspaceRoot;
    return yield* gitWorkflow.listRefs({ ...refs, cwd });
  });

  return WorktreeMcpService.of({ handoff, status, listRefs, reconcileBranch });
});

export const layer = Layer.effect(WorktreeMcpService, make);

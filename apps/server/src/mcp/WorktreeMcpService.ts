import {
  CommandId,
  MessageId,
  type ProjectId,
  WorktreeMcpFailure,
  type WorktreeMcpContinuationStatus,
  type WorktreeMcpHandoffInput,
  type WorktreeMcpHandoffResult,
  type WorktreeMcpSetupScriptStatus,
  type WorktreeMcpStatusResult,
  type WorktreeMcpReconcileBranchInput,
  type WorktreeMcpReconcileBranchResult,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as DateTime from "effect/DateTime";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";

import * as GitWorkflowService from "../git/GitWorkflowService.ts";
import * as ThreadManagementService from "../orchestration-v2/ThreadManagementService.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as ProjectSetupScriptRunner from "../project/ProjectSetupScriptRunner.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as VcsStatusBroadcaster from "../vcs/VcsStatusBroadcaster.ts";
import * as WorkspaceAccess from "../workspace/WorkspaceAccess.ts";
import * as TreehouseLeases from "../workspace/treehouseLeases.ts";
import * as ProcessRunner from "../processRunner.ts";
import { withWorkspaceLease } from "../workspace/workspaceLease.ts";
import { storageCleanupThreadIdle } from "../storageCleanup.ts";
import type { McpInvocationScope, McpThreadInvocationScope } from "./McpInvocationContext.ts";

const isWorktreeMcpFailure = Schema.is(WorktreeMcpFailure);

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
    ) => Effect.Effect<
      WorktreeMcpReconcileBranchResult,
      WorktreeMcpFailure,
      ProcessRunner.ProcessRunner
    >;
    readonly status: (
      scope: McpInvocationScope,
    ) => Effect.Effect<WorktreeMcpStatusResult, WorktreeMcpFailure>;
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

const asOperationFailed = (prefix: string) =>
  Effect.mapError((error: unknown) =>
    failure("operation_failed", `${prefix}: ${errorMessage(error)}`),
  );

const make = Effect.gen(function* () {
  const crypto = yield* Crypto.Crypto;
  const threadManagement = yield* ThreadManagementService.ThreadManagementService;
  const projects = yield* ProjectService.ProjectService;
  const serverSettings = yield* ServerSettings.ServerSettingsService;
  const gitWorkflow = yield* GitWorkflowService.GitWorkflowService;
  const setupScriptRunner = yield* ProjectSetupScriptRunner.ProjectSetupScriptRunner;
  const vcsStatusBroadcaster = yield* VcsStatusBroadcaster.VcsStatusBroadcaster;
  const access = yield* WorkspaceAccess.WorkspaceAccess.pipe(Effect.provide(WorkspaceAccess.layer));
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const leases = yield* TreehouseLeases.TreehouseLeaseStore.pipe(
    Effect.provide(TreehouseLeases.layer),
  );

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

  const readDefaultStartFromOrigin = serverSettings.getSettings.pipe(
    Effect.map((settings) => settings.newWorktreesStartFromOrigin),
    asOperationFailed("Unable to read server settings"),
  );

  const handoffIds = (scope: McpThreadInvocationScope) =>
    crypto.randomUUIDv4().pipe(
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
        `Thread '${scope.thread.threadId}' is already attached to worktree '${worktreePath}'.`,
      );

    const projection = yield* loadThread(scope);
    if (projection.thread.worktreePath !== null) {
      return yield* alreadyInWorktree(projection.thread.worktreePath);
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

    const localBranchNames = yield* gitWorkflow
      .listLocalBranchNames(projectCwd)
      .pipe(asOperationFailed("Unable to list branches"));
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
        }. Choose a different branch name, or delete the existing branch${
          checkoutPath === null ? "" : " and its worktree"
        } first.`,
      );
    }

    let baseRef = input.baseRef;
    if (baseRef === undefined) {
      if (localStatus.refName === null) {
        return yield* failure(
          "invalid_request",
          "Could not determine the current branch of the project workspace (detached HEAD?). Pass baseRef explicitly.",
        );
      }
      baseRef = localStatus.refName;
    }

    const startFromOrigin = input.startFromOrigin ?? (yield* readDefaultStartFromOrigin);

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

    // Mask interruption after worktree creation through setup; binding can detach and interrupt the caller.
    return yield* Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        const worktree = yield* restore(
          gitWorkflow
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
            .pipe(asOperationFailed("Unable to create the worktree")),
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
          gitWorkflow.removeWorktree({ cwd: projectCwd, path: worktreePath, force: true }).pipe(
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
          if (recheck.thread.worktreePath !== null) {
            return yield* alreadyInWorktree(recheck.thread.worktreePath);
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
              expectedWorktreePath: null,
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
        if (input.runSetupScript ?? true) {
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
  )(
    function* ({ scope: callerScope, input }) {
      yield* requireCapability(callerScope);
      const scope = yield* requireThreadScope(callerScope);
      if (!callerScope.capabilities.has("orchestration"))
        return yield* failure(
          "capability_denied",
          "Branch reconciliation requires orchestration capabilities.",
        );
      const expected = input.expected;
      if (
        !path.isAbsolute(expected.worktreePath) ||
        new Set(expected.threadIds).size !== expected.threadIds.length
      )
        return yield* failure(
          "invalid_request",
          "Inspect the canonical checkout and its complete thread family first.",
        );
      const runner = yield* ProcessRunner.ProcessRunner;
      const readGit = Effect.fn("WorktreeMcpService.reconcileBranch.readGit")(function* (options: {
        readonly cwd: string;
        readonly args: readonly string[];
      }) {
        const output = yield* runner.run({
          command: "git",
          cwd: options.cwd,
          args: ["--no-optional-locks", ...options.args],
          timeout: "30 seconds",
          maxOutputBytes: 1024 * 1024,
        });
        if (output.code !== 0 || output.stdoutTruncated || output.stderrTruncated)
          return yield* failure(
            "operation_failed",
            "Git inspection failed; no checkout or reset is performed.",
          );
        return output.stdout.trim();
      });
      const inspect = Effect.fn("WorktreeMcpService.reconcileBranch.inspect")(function* () {
        const snapshot = yield* threadManagement.getShellSnapshot({
          worktreePath: expected.worktreePath,
        });
        const members = [...snapshot.threads, ...snapshot.archivedThreads];
        const ids = new Set(members.map((thread) => thread.id));
        const owner = members.find((thread) => thread.id === input.threadId);
        if (
          !owner ||
          owner.lineage.parentThreadId !== null ||
          owner.lineage.rootThreadId !== owner.id ||
          members.length !== expected.threadIds.length ||
          expected.threadIds.some((id) => !ids.has(id))
        )
          return yield* failure(
            "invalid_request",
            "The inspected root thread family changed or has another owner.",
          );
        const now = DateTime.toEpochMillis(yield* DateTime.now);
        const byId = new Map(members.map((thread) => [thread.id, thread]));
        for (const member of members) {
          if (
            member.archivedAt !== null ||
            member.settledAt === null ||
            !storageCleanupThreadIdle(member, now) ||
            (member.branch !== expected.recordedBranch && member.branch !== expected.branch)
          )
            return yield* failure(
              "invalid_request",
              "Every shared thread must remain settled, idle and on an inspected branch.",
            );
          let current = member;
          const visited = new Set<string>();
          while (current.id !== owner.id) {
            if (
              visited.has(current.id) ||
              current.lineage.relationshipToParent !== "subagent" ||
              current.lineage.rootThreadId !== owner.lineage.rootThreadId
            )
              return yield* failure(
                "invalid_request",
                "The checkout is not one owned subagent family.",
              );
            visited.add(current.id);
            const parent =
              current.lineage.parentThreadId === null
                ? undefined
                : byId.get(current.lineage.parentThreadId);
            if (!parent)
              return yield* failure(
                "invalid_request",
                "A shared thread has an uninspected parent.",
              );
            current = parent;
          }
          const generation = yield* leases.operationGeneration({
            path: expected.worktreePath,
            threadId: member.id,
          });
          if (generation !== expected.leaseId)
            return yield* failure("invalid_request", "The Treehouse lease generation changed.");
        }
        const owned = yield* leases.read(expected.worktreePath);
        if (
          !owned ||
          owned.record.lease.lease_id !== expected.leaseId ||
          owned.record.lease.path !== expected.worktreePath
        )
          return yield* failure(
            "invalid_request",
            "The current T3 lease does not match the inspection.",
          );
        yield* leases.assertUnused({ owned, allowedThreadIds: expected.threadIds });
        const project = yield* loadProject(scope, owner.projectId);
        if ((yield* fs.realPath(expected.worktreePath)) !== expected.worktreePath)
          return yield* failure("invalid_request", "The worktree path is not canonical.");
        const commonArgs = ["rev-parse", "--path-format=absolute", "--git-common-dir"];
        const common = yield* readGit({ cwd: expected.worktreePath, args: commonArgs });
        const projectCommon = yield* readGit({ cwd: project.workspaceRoot, args: commonArgs });
        const branch = yield* readGit({
          cwd: expected.worktreePath,
          args: ["symbolic-ref", "--short", "HEAD"],
        });
        const head = yield* readGit({ cwd: expected.worktreePath, args: ["rev-parse", "HEAD"] });
        const retained = yield* readGit({
          cwd: project.workspaceRoot,
          args: [
            "rev-parse",
            "--verify",
            "--end-of-options",
            `refs/heads/${expected.branch}^{commit}`,
          ],
        });
        const changes = yield* readGit({
          cwd: expected.worktreePath,
          args: [
            "status",
            "--porcelain=v1",
            "-z",
            "--untracked-files=normal",
            "--ignore-submodules=none",
          ],
        });
        if (
          (yield* fs.realPath(common)) !== (yield* fs.realPath(projectCommon)) ||
          branch !== expected.branch ||
          head !== expected.headSha ||
          retained !== head ||
          changes !== ""
        )
          return yield* failure(
            "invalid_request",
            "Repository, branch, commit or local changes differ from the inspection.",
          );
        return { snapshotSequence: snapshot.snapshotSequence, members, owner };
      });
      let dispatchAttempted = false;
      return yield* withWorkspaceLease(
        expected.worktreePath,
        Effect.gen(function* () {
          const initial = yield* inspect();
          const updatedThreadIds: (typeof input.expected.threadIds)[number][] = [];
          const ordered = [
            initial.owner,
            ...initial.members.filter((thread) => thread.id !== input.threadId),
          ];
          for (const member of ordered) {
            const checked = yield* inspect();
            const current = checked.members.find((thread) => thread.id === member.id)!;
            if (current.branch === expected.branch) continue;
            dispatchAttempted = true;
            yield* threadManagement.dispatch({
              type: "thread.metadata.update",
              commandId: CommandId.make(
                `worktree-reconcile:${yield* crypto.randomUUIDv4().pipe(Effect.orDie)}`,
              ),
              threadId: member.id,
              branch: expected.branch,
              expectedBranch: expected.recordedBranch,
              expectedWorktreePath: expected.worktreePath,
              expectedSequence: initial.snapshotSequence,
            });
            updatedThreadIds.push(member.id);
          }
          const final = yield* inspect();
          if (final.members.some((member) => member.branch !== expected.branch))
            return yield* failure(
              "operation_failed",
              "Thread branch synchronization is incomplete; no checkout or return was performed.",
            );
          return {
            worktreePath: expected.worktreePath,
            branch: expected.branch,
            headSha: expected.headSha,
            threadIds: expected.threadIds,
            updatedThreadIds,
          };
        }),
      ).pipe(
        Effect.mapError((cause) =>
          dispatchAttempted
            ? failure(
                "operation_failed",
                "Reconciliation stopped after metadata dispatch. Family records may be partially synchronized; inspect before retrying. No checkout, reset or return was performed.",
              )
            : cause,
        ),
      );
    },
    Effect.catchTags({
      TreehouseLeaseError: (cause) => Effect.fail(failure("invalid_request", cause.message)),
    }),
    Effect.mapError((cause) =>
      isWorktreeMcpFailure(cause)
        ? cause
        : failure(
            "operation_failed",
            "Reconciliation stopped. Git files and branches are unchanged; inspect thread metadata before retrying a partially completed family update.",
          ),
    ),
  );

  const status: WorktreeMcpService["Service"]["status"] = Effect.fn("WorktreeMcpService.status")(
    function* (callerScope) {
      yield* requireCapability(callerScope);
      const scope = yield* requireThreadScope(callerScope);
      const projection = yield* loadThread(scope);
      const project = yield* loadProject(scope, projection.thread.projectId);

      const defaultStartFromOrigin = yield* readDefaultStartFromOrigin;

      const result: WorktreeMcpStatusResult = {
        attached: projection.thread.worktreePath !== null,
        worktreePath: projection.thread.worktreePath,
        branch: projection.thread.branch,
        projectWorkspaceRoot: project.workspaceRoot,
        defaultStartFromOrigin,
      };
      return yield* access
        .useThread({
          threadId: scope.thread.threadId,
          cwd: projection.thread.worktreePath ?? project.workspaceRoot,
          targets: [],
          effect: Effect.succeed(result),
        })
        .pipe(asOperationFailed("Workspace unavailable"));
    },
  );

  return WorktreeMcpService.of({ handoff, status, reconcileBranch });
});

export const layer = Layer.effect(WorktreeMcpService, make);

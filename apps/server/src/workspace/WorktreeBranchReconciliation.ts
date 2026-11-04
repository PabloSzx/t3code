import { CommandId, type ThreadId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as SecureRandom from "effect/crypto/SecureRandom";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import * as ProjectStore from "../orchestration-v2/ProjectStore.ts";
import * as ProcessRunner from "../processRunner.ts";
import * as TreehouseLeases from "./treehouseLeases.ts";
import { withWorkspaceLease } from "./workspaceLease.ts";
import { worktreeOwner, worktreeThreadIdle } from "./worktreeThreads.ts";

interface ReconcileWorktreeBranchOptions {
  readonly threadId: ThreadId;
  readonly expected: {
    readonly worktreePath: string;
    readonly branch: string;
    readonly headSha: string;
    readonly leaseId: string;
    readonly members: readonly { readonly threadId: ThreadId; readonly branch: string | null }[];
  };
}

export class WorktreeBranchReconciliationError extends Schema.TaggedError<WorktreeBranchReconciliationError>()(
  "WorktreeBranchReconciliationError",
  {
    path: Schema.String,
    reason: Schema.Literals(["changed", "inspection", "partial"]),
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return this.reason === "partial"
      ? "Branch reconciliation stopped after metadata dispatch. Family records may be partially synchronized; inspect before retrying. No checkout, reset or return was performed."
      : this.reason === "inspection"
        ? "Branch reconciliation could not verify the checkout. Git files and branch refs are unchanged."
        : "Checkout ownership, activity, repository, branch or commit differs from the inspection.";
  }
}
const isReconciliationError = Schema.is(WorktreeBranchReconciliationError);

export class WorktreeBranchReconciliation extends Context.Service<
  WorktreeBranchReconciliation,
  {
    readonly reconcile: (options: ReconcileWorktreeBranchOptions) => Effect.Effect<
      {
        readonly worktreePath: string;
        readonly branch: string;
        readonly headSha: string;
        readonly threadIds: readonly ThreadId[];
        readonly updatedThreadIds: readonly ThreadId[];
      },
      WorktreeBranchReconciliationError
    >;
  }
>()("t3/workspace/WorktreeBranchReconciliation") {}

const make = Effect.gen(function* () {
  const engine = yield* Orchestrator.OrchestratorV2;
  const projects = yield* ProjectStore.ProjectStoreV2;
  const random = yield* SecureRandom.SecureRandom;
  const runner = yield* ProcessRunner.ProcessRunner;
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const leases = yield* TreehouseLeases.TreehouseLeaseStore.pipe(
    Effect.provide(TreehouseLeases.layer),
  );
  const reconcile: WorktreeBranchReconciliation["Service"]["reconcile"] = Effect.fn(
    "WorktreeBranchReconciliation.reconcile",
  )(function* (options) {
    const expected = options.expected;
    if (
      !path.isAbsolute(expected.worktreePath) ||
      expected.members.length === 0 ||
      new Set(expected.members.map((member) => member.threadId)).size !== expected.members.length
    )
      return yield* new WorktreeBranchReconciliationError({
        path: expected.worktreePath,
        reason: "changed",
      });
    const readGit = Effect.fn("WorktreeBranchReconciliation.readGit")(function* (input: {
      readonly cwd: string;
      readonly args: readonly string[];
    }) {
      const result = yield* runner.run({
        command: "git",
        cwd: input.cwd,
        args: ["--no-optional-locks", ...input.args],
        timeout: "30 seconds",
        maxOutputBytes: 1024 * 1024,
      });
      if (result.code !== 0 || result.stdoutTruncated || result.stderrTruncated)
        return yield* new WorktreeBranchReconciliationError({
          path: expected.worktreePath,
          reason: "inspection",
        });
      return result.stdout.trim();
    });
    const inspect = Effect.fn("WorktreeBranchReconciliation.inspect")(function* () {
      const snapshot = yield* engine.getShellSnapshot({ worktreePath: expected.worktreePath });
      const current = yield* leases.currentThreads({
        path: expected.worktreePath,
        threads: [...snapshot.threads, ...snapshot.archivedThreads],
      });
      const members = current.threads;
      const owner = worktreeOwner(members);
      if (
        !current.verified ||
        current.leaseId !== expected.leaseId ||
        owner?.id !== options.threadId ||
        owner.lineage.parentThreadId !== null ||
        owner.lineage.rootThreadId !== owner.id ||
        members.length !== expected.members.length ||
        expected.members.some((member) => !members.some((thread) => thread.id === member.threadId))
      )
        return yield* new WorktreeBranchReconciliationError({
          path: expected.worktreePath,
          reason: "changed",
        });
      const now = DateTime.toEpochMillis(yield* DateTime.now);
      for (const member of members) {
        const recorded = expected.members.find((entry) => entry.threadId === member.id)!;
        if (
          !worktreeThreadIdle(member, now) ||
          member.projectId !== owner.projectId ||
          member.archivedAt !== null ||
          member.settledAt === null ||
          (member.branch !== recorded.branch && member.branch !== expected.branch)
        )
          return yield* new WorktreeBranchReconciliationError({
            path: expected.worktreePath,
            reason: "changed",
          });
      }
      const owned = yield* leases.read(expected.worktreePath);
      if (owned?.record.lease.lease_id !== expected.leaseId)
        return yield* new WorktreeBranchReconciliationError({
          path: expected.worktreePath,
          reason: "changed",
        });
      yield* leases.assertUnused({ owned, allowedThreadIds: members.map((member) => member.id) });
      const project = yield* projects.get(owner.projectId);
      if (
        Option.isNone(project) ||
        (yield* fs.realPath(expected.worktreePath)) !== expected.worktreePath
      )
        return yield* new WorktreeBranchReconciliationError({
          path: expected.worktreePath,
          reason: "changed",
        });
      const commonArgs = ["rev-parse", "--path-format=absolute", "--git-common-dir"];
      const common = yield* readGit({ cwd: expected.worktreePath, args: commonArgs });
      const projectCommon = yield* readGit({ cwd: project.value.workspaceRoot, args: commonArgs });
      const branch = yield* readGit({
        cwd: expected.worktreePath,
        args: ["symbolic-ref", "--short", "HEAD"],
      });
      const head = yield* readGit({ cwd: expected.worktreePath, args: ["rev-parse", "HEAD"] });
      const retained = yield* readGit({
        cwd: project.value.workspaceRoot,
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
      const indexLock = yield* readGit({
        cwd: expected.worktreePath,
        args: ["rev-parse", "--git-path", "index.lock"],
      });
      if (
        (yield* fs.realPath(common)) !== (yield* fs.realPath(projectCommon)) ||
        branch !== expected.branch ||
        head !== expected.headSha ||
        retained !== head ||
        changes !== "" ||
        (yield* fs.exists(path.resolve(expected.worktreePath, indexLock)))
      )
        return yield* new WorktreeBranchReconciliationError({
          path: expected.worktreePath,
          reason: "changed",
        });
      return { members, owner, snapshotSequence: snapshot.snapshotSequence };
    });
    let dispatchAttempted = false;
    return yield* withWorkspaceLease(
      expected.worktreePath,
      Effect.gen(function* () {
        const initial = yield* inspect();
        const updatedThreadIds: ThreadId[] = [];
        const ordered = [
          initial.owner,
          ...initial.members.filter((member) => member.id !== options.threadId),
        ];
        for (const member of ordered) {
          const checked = yield* inspect();
          const current = checked.members.find((thread) => thread.id === member.id)!;
          if (current.branch === expected.branch) continue;
          dispatchAttempted = true;
          yield* engine.dispatch({
            type: "thread.metadata.update",
            commandId: CommandId.make(`worktree-reconcile:${yield* random.uuidV4()}`),
            threadId: member.id,
            branch: expected.branch,
            expectedBranch: current.branch,
            expectedWorktreePath: expected.worktreePath,
            expectedSequence: initial.snapshotSequence,
          });
          updatedThreadIds.push(member.id);
        }
        const final = yield* inspect();
        if (final.members.some((member) => member.branch !== expected.branch))
          return yield* new WorktreeBranchReconciliationError({
            path: expected.worktreePath,
            reason: "partial",
          });
        return {
          worktreePath: expected.worktreePath,
          branch: expected.branch,
          headSha: expected.headSha,
          threadIds: expected.members.map((member) => member.threadId),
          updatedThreadIds,
        };
      }),
    ).pipe(
      Effect.mapError((cause) =>
        dispatchAttempted
          ? new WorktreeBranchReconciliationError({
              path: expected.worktreePath,
              reason: "partial",
              cause,
            })
          : isReconciliationError(cause)
            ? cause
            : new WorktreeBranchReconciliationError({
                path: expected.worktreePath,
                reason: TreehouseLeases.isTreehouseLeaseError(cause) ? "changed" : "inspection",
                cause,
              }),
      ),
    );
  });
  return WorktreeBranchReconciliation.of({ reconcile });
});

export const layer = Layer.effect(WorktreeBranchReconciliation, make);

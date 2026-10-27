import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import {
  ContextTransferId,
  EventId,
  MessageId,
  ProjectId,
  ProviderInstanceId,
  ProviderThreadId,
  RunId,
  ThreadId,
  type OrchestrationV2AppThread,
} from "@t3tools/contracts";
import * as Sqlite from "../persistence/Sqlite.ts";
import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as EventStore from "../orchestration-v2/EventStore.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as ThreadForkService from "../orchestration-v2/ThreadForkService.ts";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as PlatformError from "effect/PlatformError";
import * as Result from "effect/Result";
import * as Exit from "effect/Exit";
import * as Scope from "effect/Scope";
import * as ServerConfig from "../config.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as ProcessRunner from "../processRunner.ts";
import * as TreehouseLeases from "../workspace/treehouseLeases.ts";
import { makeGitVcsDriverCore } from "./GitVcsDriverCore.ts";
import { removeLinkedWorktree } from "./worktreeRemoval.ts";

// Only the external allocator is a fixture. Revision selection and removal use real T3 + Git.
const allocator = `import { readFileSync, writeFileSync, appendFileSync, existsSync, mkdirSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";
import { randomBytes } from "node:crypto";
const root = dirname(fileURLToPath(import.meta.url));
const repo = join(root, "repo");
const pool = join(root, "pool");
const slot = join(pool, "1", "repo");
const stateFile = join(pool, "treehouse-state.json");
const args = process.argv.slice(2);
readFileSync(0);
appendFileSync(join(root, "calls"), args.join(" ") + "\\n");
function git(cwd, args) {
  const result = spawnSync("git", args, { cwd, encoding: "utf8" });
  if (result.status !== 0) throw new Error(result.stderr);
}
if (args[0] === "get") {
  if (!args.includes("--no-fetch") || !args.includes("--lease") || !args.includes("--json") || args.includes("--base")) process.exit(2);
  if (existsSync(join(root, "fail-get"))) process.exit(3);
  mkdirSync(join(pool, "1"), { recursive: true });
  if (!existsSync(slot)) git(repo, ["worktree", "add", "--detach", slot, "main"]);
  else git(slot, ["switch", "--detach", "main"]);
  const holder = args[args.indexOf("--lease-holder") + 1];
  const lease = { path: slot, lease_id: randomBytes(16).toString("hex"), lease_holder: holder, leased_at: "2026-01-01T00:00:00Z", base_branch: "" };
  writeFileSync(stateFile, JSON.stringify({ version: 4, worktrees: [{ ...lease, leased: true }] }));
  console.log(JSON.stringify(lease));
} else if (args[0] === "return") {
  const state = JSON.parse(readFileSync(stateFile, "utf8"));
  if (args.includes("--force") || args[2] !== "--if-lease-id" || args[3] !== state.worktrees[0].lease_id) process.exit(4);
  if (existsSync(join(root, "fail-return"))) process.exit(5);
  state.worktrees[0].leased = false;
  writeFileSync(stateFile, JSON.stringify(state));
} else process.exit(6);
`;
const fixture = Effect.fn("TreehouseTest.fixture")(function* (
  options: { treehouseEnabled?: boolean } = {},
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const runner = yield* ProcessRunner.ProcessRunner;
  const root = yield* fs
    .makeTempDirectoryScoped({ prefix: "t3-treehouse-" })
    .pipe(Effect.flatMap(fs.realPath));
  const cwd = path.join(root, "repo");
  yield* fs.makeDirectory(cwd);
  const binary = path.join(root, "allocator.mjs");
  yield* fs.writeFileString(binary, `#!${process.execPath}\n${allocator}`, { mode: 0o755 });
  const git = Effect.fn("TreehouseTest.git")(function* (input: { cwd?: string; args: string[] }) {
    const result = yield* runner.run({
      command: "git",
      args: input.args,
      cwd: input.cwd ?? cwd,
      stdin: "",
    });
    assert.strictEqual(result.code, 0, result.stderr);
    return result.stdout.trim();
  });
  yield* git({ args: ["init", "-b", "main"] });
  yield* git({ args: ["config", "user.name", "Test"] });
  yield* git({ args: ["config", "user.email", "test@example.invalid"] });
  yield* fs.writeFileString(path.join(cwd, "file"), "main\n");
  yield* git({ args: ["add", "file"] });
  yield* git({ args: ["commit", "-m", "main"] });
  const baseSha = yield* git({ args: ["rev-parse", "HEAD"] });
  yield* git({ args: ["checkout", "-b", "parent"] });
  yield* fs.writeFileString(path.join(cwd, "file"), "parent\n");
  yield* git({ args: ["commit", "-am", "parent"] });
  const parentSha = yield* git({ args: ["rev-parse", "HEAD"] });
  yield* git({ args: ["update-ref", "refs/remotes/origin/main", parentSha] });
  const settings = yield* ServerSettings.ServerSettingsService;
  if (options.treehouseEnabled !== false) {
    yield* settings.updateSettings({
      treehouseWorktreesEnabled: true,
      treehouseBinaryPath: binary,
    });
  }
  const driver = yield* makeGitVcsDriverCore();
  const leases = yield* TreehouseLeases.TreehouseLeaseStore.pipe(
    Effect.provide(TreehouseLeases.layer),
  );
  const config = yield* ServerConfig.ServerConfig;
  return {
    root,
    cwd,
    fs,
    path,
    git,
    driver,
    leases,
    settings,
    binary,
    baseSha,
    parentSha,
    registry: path.join(config.stateDir, "treehouse-leases"),
    slot: path.join(root, "pool", "1", "repo"),
  };
});
const testLayer = ProcessRunner.layer.pipe(
  Layer.provideMerge(ServerSettings.layerTest()),
  Layer.provideMerge(
    ServerConfig.layerTest(process.cwd(), { prefix: "treehouse-config-" }).pipe(
      Layer.provideMerge(NodeServices.layer),
    ),
  ),
);
const provide = <A, E, R>(effect: Effect.Effect<A, E, R>) => effect.pipe(Effect.provide(testLayer));

describe("Treehouse worktrees", () => {
  it.live(
    "keeps native allocation and removal by default, ignoring retired repository selectors",
    () =>
      Effect.gen(function* () {
        const f = yield* fixture({ treehouseEnabled: false });
        yield* f.git({ args: ["config", "t3.worktree-manager", "treehouse"] });
        yield* f.git({ args: ["config", "t3.treehouse-binary", f.binary] });
        const nativePath = f.path.join(f.root, "native");
        const created = yield* f.driver.createWorktree({
          cwd: f.cwd,
          refName: "main",
          newRefName: "native-branch",
          path: nativePath,
        });
        assert.strictEqual(created.worktree.path, nativePath);
        assert.isFalse(yield* f.fs.exists(f.path.join(f.root, "calls")));
        assert.strictEqual(yield* f.git({ args: ["rev-parse", "HEAD"] }), f.parentSha);
        yield* f.driver.removeWorktree({ cwd: f.cwd, path: nativePath });
        assert.isFalse(yield* f.fs.exists(nativePath));
        assert.strictEqual(
          yield* f.git({ args: ["rev-parse", "refs/heads/native-branch"] }),
          f.baseSha,
        );
      }).pipe(provide),
  );
  it.live(
    "applies host enablement to unrelated repositories without repository configuration",
    () =>
      Effect.gen(function* () {
        for (let index = 0; index < 2; index++) {
          const f = yield* fixture();
          const created = yield* f.driver.createWorktree({
            cwd: f.cwd,
            refName: f.parentSha,
            path: null,
          });
          assert.strictEqual(created.worktree.path, f.slot);
          assert.strictEqual(
            yield* f.git({ cwd: f.slot, args: ["rev-parse", "HEAD"] }),
            f.parentSha,
          );
          assert.strictEqual(yield* f.git({ args: ["branch", "--show-current"] }), "parent");
        }
      }).pipe(provide),
  );
  it.live.each(["relative/treehouse", "/missing-t3-test/treehouse"])(
    "refuses unusable host binary %s without creating a native worktree",
    (binary) =>
      Effect.gen(function* () {
        const f = yield* fixture();
        yield* f.settings.updateSettings({ treehouseBinaryPath: binary });
        const result = yield* f.driver
          .createWorktree({ cwd: f.cwd, refName: "main", path: null })
          .pipe(Effect.result);
        assert.isTrue(Result.isFailure(result));
        if (Result.isFailure(result)) assert.include(result.failure.message, "Settings");
        assert.strictEqual(
          (yield* f.git({ args: ["worktree", "list", "--porcelain"] }))
            .split("\n")
            .filter((line) => line.startsWith("worktree ")).length,
          1,
        );
      }).pipe(provide),
  );
  it.live.each(["parent", "commit", "origin/main", "HEAD", "HEAD~1"])(
    "resolves %s in the caller checkout before creating a branch",
    (ref) =>
      Effect.gen(function* () {
        const f = yield* fixture();
        const created = yield* f.driver.createWorktree({
          cwd: f.cwd,
          refName: ref === "commit" ? f.parentSha : ref,
          newRefName: "pablosaez/child",
          path: null,
        });
        assert.strictEqual(created.worktree.path, f.slot);
        assert.strictEqual(
          yield* f.git({ cwd: f.slot, args: ["rev-parse", "HEAD"] }),
          ref === "HEAD~1" ? f.baseSha : f.parentSha,
        );
        assert.strictEqual(
          yield* f.git({ cwd: f.slot, args: ["branch", "--show-current"] }),
          "pablosaez/child",
        );
        assert.strictEqual(
          yield* f.fs.readFileString(f.path.join(f.slot, "file")),
          ref === "HEAD~1" ? "main\n" : "parent\n",
        );
        assert.deepEqual(yield* f.fs.readDirectory(f.path.join(f.root, "pool")), [
          "1",
          "treehouse-state.json",
        ]);
      }).pipe(provide),
  );
  it.live(
    "preserves an existing branch checkout and returns the exact unbound lease after restart",
    () =>
      Effect.gen(function* () {
        const f = yield* fixture();
        yield* f.driver.createWorktree({ cwd: f.cwd, refName: "main", path: null });
        assert.strictEqual(
          yield* f.git({ cwd: f.slot, args: ["branch", "--show-current"] }),
          "main",
        );
        const initialLease = yield* f.leases.read(f.slot);
        assert.isNotNull(initialLease);
        const restarted = yield* makeGitVcsDriverCore();
        const lock = f.path.join(f.registry, "lifecycle.lock");
        yield* f.fs.makeDirectory(lock);
        assert.isTrue(
          Result.isFailure(
            yield* restarted.removeWorktree({ cwd: f.cwd, path: f.slot }).pipe(Effect.result),
          ),
        );
        yield* f.fs.remove(lock, { recursive: true });
        yield* restarted.removeWorktree({ cwd: f.cwd, path: f.slot, force: true });
        assert.isTrue(yield* f.fs.exists(f.slot));
        const calls = yield* f.fs.readFileString(f.path.join(f.root, "calls"));
        assert.include(
          calls,
          `return ${f.slot} --if-lease-id ${initialLease!.record.lease.lease_id}`,
        );
        assert.notInclude(calls, "--force");
        assert.isTrue(Result.isFailure(yield* f.leases.read(f.slot).pipe(Effect.result)));
      }).pipe(provide),
  );
  it.live.each(["standalone", "thread handoff"] as const)(
    "protects preparation until completion of %s creation",
    (handoff) =>
      Effect.gen(function* () {
        const f = yield* fixture();
        const scope = yield* Scope.make();
        yield* Effect.addFinalizer((exit) => Scope.close(scope, exit));
        const claimed = yield* Deferred.make<void>();
        const finish = yield* Deferred.make<void>();
        const creation = yield* f.driver
          .createWorktree(
            { cwd: f.cwd, refName: f.parentSha, path: null },
            {
              ...(handoff === "thread handoff" ? { preparationScope: scope } : {}),
              progress: {
                onWorktreeClaimed: () =>
                  Deferred.succeed(claimed, undefined).pipe(Effect.andThen(Deferred.await(finish))),
              },
            },
          )
          .pipe(Effect.forkChild());
        yield* Deferred.await(claimed);
        const duringPreparation = yield* f.driver
          .removeWorktree({ cwd: f.cwd, path: f.slot })
          .pipe(Effect.result);
        yield* Deferred.succeed(finish, undefined);
        yield* Fiber.join(creation);
        assert.isTrue(Result.isFailure(duringPreparation), "preparation must block return");
        if (handoff === "thread handoff") {
          assert.isTrue(
            Result.isFailure(
              yield* f.driver.removeWorktree({ cwd: f.cwd, path: f.slot }).pipe(Effect.result),
            ),
            "binding handoff must block return",
          );
          yield* f.leases.bindThread({ path: f.slot, threadId: "launch-thread", sequence: 1 });
          yield* Scope.close(scope, Exit.void);
          yield* f.leases.assertThread({ path: f.slot, threadId: "launch-thread" });
          yield* f.leases.commitThread({
            path: null,
            threadId: "launch-thread",
            deleted: false,
            sequence: 2,
          });
        }
        yield* f.driver.removeWorktree({ cwd: f.cwd, path: f.slot });
        assert.isTrue(Result.isFailure(yield* f.leases.read(f.slot).pipe(Effect.result)));
      }).pipe(provide),
  );
  it.live("retains failed allocations without native fallback", () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      const failGet = f.path.join(f.root, "fail-get");
      yield* f.fs.writeFileString(failGet, "");
      assert.isTrue(
        Result.isFailure(
          yield* f.driver
            .createWorktree({ cwd: f.cwd, refName: "main", path: null })
            .pipe(Effect.result),
        ),
      );
      assert.strictEqual(
        (yield* f.git({ args: ["worktree", "list", "--porcelain"] }))
          .split("\n")
          .filter((line) => line.startsWith("worktree ")).length,
        1,
      );
      yield* f.fs.remove(failGet);
      assert.isTrue(
        Result.isFailure(
          yield* f.driver
            .createWorktree({ cwd: f.cwd, refName: f.parentSha, newRefName: "parent", path: null })
            .pipe(Effect.result),
        ),
      );
      assert.isNotNull(yield* f.leases.read(f.slot));
      yield* f.fs.writeFileString(f.path.join(f.root, "fail-return"), "");
      assert.isTrue(
        Result.isFailure(
          yield* f.driver
            .removeWorktree({ cwd: f.cwd, path: f.slot, force: true })
            .pipe(Effect.result),
        ),
      );
      assert.isNotNull(yield* f.leases.read(f.slot));
      assert.include(yield* f.git({ args: ["worktree", "list", "--porcelain"] }), f.slot);
    }).pipe(provide),
  );
  it.live(
    "keeps shared references and live uses protected after disablement, then allows explicit return",
    () =>
      Effect.gen(function* () {
        const f = yield* fixture();
        yield* f.driver.createWorktree({ cwd: f.cwd, refName: f.parentSha, path: null });
        yield* f.leases.bindThread({ path: f.slot, threadId: "parent-thread", sequence: 1 });
        yield* f.leases.bindThread({ path: f.slot, threadId: "child-thread", sequence: 2 });
        const releaseProvider = yield* f.leases.pinUse({
          path: f.slot,
          threadId: "parent-thread",
          kind: "provider",
        });
        const releaseTerminal = yield* f.leases.pinUse({
          path: f.slot,
          threadId: "child-thread",
          kind: "terminal",
        });
        yield* f.settings.updateSettings({
          treehouseWorktreesEnabled: false,
          treehouseBinaryPath: "/missing-t3-test/treehouse",
        });
        const nativePath = f.path.join(f.root, "native-after-disable");
        yield* f.driver.createWorktree({ cwd: f.cwd, refName: f.parentSha, path: nativePath });
        assert.isTrue(yield* f.fs.exists(nativePath));
        yield* f.driver.removeWorktree({ cwd: f.cwd, path: nativePath });
        yield* f.leases.commitThread({
          path: f.slot,
          threadId: "parent-thread",
          deleted: true,
          sequence: 3,
        });
        const restarted = yield* makeGitVcsDriverCore();
        assert.isTrue(
          Result.isFailure(
            yield* restarted.removeWorktree({ cwd: f.cwd, path: f.slot }).pipe(Effect.result),
          ),
        );
        yield* f.leases.commitThread({
          path: null,
          threadId: "child-thread",
          deleted: false,
          sequence: 4,
        });
        assert.isTrue(
          Result.isFailure(
            yield* restarted.removeWorktree({ cwd: f.cwd, path: f.slot }).pipe(Effect.result),
          ),
        );
        yield* releaseProvider;
        assert.isTrue(
          Result.isFailure(
            yield* restarted.removeWorktree({ cwd: f.cwd, path: f.slot }).pipe(Effect.result),
          ),
        );
        yield* releaseTerminal;
        yield* restarted.removeWorktree({ cwd: f.cwd, path: f.slot, force: true });
        // Reacquire the same physical path, then prove old identities cannot be rebound or resumed.
        yield* f.settings.updateSettings({
          treehouseWorktreesEnabled: true,
          treehouseBinaryPath: f.binary,
        });
        yield* f.driver.createWorktree({ cwd: f.cwd, refName: f.parentSha, path: null });
        assert.isTrue(
          Result.isFailure(
            yield* f.leases
              .assertThread({ path: f.slot, threadId: "parent-thread", requireBinding: false })
              .pipe(Effect.result),
          ),
        );
        assert.isTrue(
          Result.isFailure(
            yield* f.leases
              .bindThread({ path: f.slot, threadId: "child-thread", sequence: 5 })
              .pipe(Effect.result),
          ),
        );
        yield* f.leases.bindThread({ path: f.slot, threadId: "new-thread", sequence: 6 });
        yield* f.leases.assertThread({ path: f.slot, threadId: "new-thread" });
      }).pipe(provide),
  );
  it.live("refuses unknown leases, missing pooled paths, and raw linked-worktree cleanup", () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      yield* f.driver.createWorktree({ cwd: f.cwd, refName: f.parentSha, path: null });
      const statePath = f.path.join(f.root, "pool", "treehouse-state.json");
      const snapshot = yield* f.fs.readFileString(statePath);
      yield* f.fs.writeFileString(statePath, snapshot.replace('"version":4', '"version":5'));
      assert.isTrue(Result.isFailure(yield* f.leases.read(f.slot).pipe(Effect.result)));
      yield* f.fs.writeFileString(statePath, snapshot);
      const raw = yield* removeLinkedWorktree({
        operation: "test",
        cwd: f.cwd,
        worktreePath: f.slot,
        force: true,
        execute: f.driver.execute,
        run: (args) => f.git({ args: [...args] }),
      }).pipe(Effect.result);
      assert.isTrue(Result.isFailure(raw));
      assert.isTrue(yield* f.fs.exists(f.slot));
      yield* f.fs.remove(f.path.join(f.registry, "leases"), { recursive: true });
      assert.isTrue(
        Result.isFailure(
          yield* f.driver.removeWorktree({ cwd: f.cwd, path: f.slot }).pipe(Effect.result),
        ),
      );
      yield* f.fs.remove(f.slot, { recursive: true });
      yield* f.fs.remove(statePath);
      assert.isTrue(
        Result.isFailure(
          yield* f.driver
            .createWorktree({ cwd: f.cwd, refName: "main", path: f.slot })
            .pipe(Effect.result),
        ),
      );
      assert.isFalse(yield* f.fs.exists(f.slot));
    }).pipe(provide),
  );
  it.live("allows manually leased roots but refuses T3-owned slots from another home", () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      yield* f.driver.createWorktree({ cwd: f.cwd, refName: f.parentSha, path: null });
      const foreignHome = yield* TreehouseLeases.TreehouseLeaseStore.pipe(
        Effect.provide(TreehouseLeases.layer),
        Effect.provideService(ServerConfig.ServerConfig, {
          stateDir: f.path.join(f.root, "other-home"),
        } as ServerConfig.ServerConfig["Service"]),
      );
      assert.isTrue(
        Result.isFailure(
          yield* foreignHome
            .assertThread({ path: f.slot, threadId: "manual-thread", requireBinding: false })
            .pipe(Effect.result),
        ),
      );
      const owned = yield* f.leases.read(f.slot);
      if (owned === null) return yield* Effect.die("missing fixture lease");
      const statePath = f.path.join(f.root, "pool", "treehouse-state.json");
      const snapshot = yield* f.fs.readFileString(statePath);
      yield* f.fs.writeFileString(
        statePath,
        snapshot.replace(owned.record.lease.lease_holder, "operator:manual"),
      );
      yield* foreignHome.assertThread({
        path: f.slot,
        threadId: "manual-thread",
        requireBinding: false,
      });
    }).pipe(provide),
  );

  it.live("binds committed threads and releases deleted references through the event sink", () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      yield* f.driver.createWorktree({ cwd: f.cwd, refName: f.parentSha, path: null });
      const sink = yield* EventSink.EventSinkV2;
      const now = yield* DateTime.now;
      const threadId = ThreadId.make("treehouse-thread");
      const thread: OrchestrationV2AppThread = {
        id: threadId,
        projectId: ProjectId.make("treehouse-project"),
        title: "Treehouse",
        createdBy: "user",
        creationSource: "web",
        providerInstanceId: ProviderInstanceId.make("codex"),
        modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "test" },
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: f.slot,
        branchPullRequest: null,
        activeOrderKey: null,
        activeProviderThreadId: null,
        lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
        forkedFrom: null,
        createdAt: now,
        updatedAt: now,
        archivedAt: null,
        settledOverride: null,
        settledAt: null,
        lastVisitedAt: null,
        deletedAt: null,
      };
      yield* sink.write({
        events: [
          {
            id: EventId.make("treehouse-created"),
            type: "thread.created",
            threadId,
            occurredAt: now,
            payload: thread,
          },
        ],
      });
      yield* f.leases.assertThread({ path: f.slot, threadId });
      assert.isTrue(
        Result.isFailure(
          yield* f.driver.removeWorktree({ cwd: f.cwd, path: f.slot }).pipe(Effect.result),
        ),
      );
      yield* sink.write({
        events: [
          {
            id: EventId.make("treehouse-deleted"),
            type: "thread.deleted",
            threadId,
            occurredAt: now,
            payload: { ...thread, deletedAt: now },
          },
        ],
      });
      yield* f.driver.removeWorktree({ cwd: f.cwd, path: f.slot });
    }).pipe(
      Effect.provide(
        EventSink.layer.pipe(
          Layer.provide(
            Layer.merge(EventStore.layer, ProjectionStore.layer).pipe(
              Layer.provideMerge(Sqlite.layerMemory),
            ),
          ),
          Layer.provideMerge(testLayer),
        ),
      ),
    ),
  );

  it.live("rejects an inherited fork of a recycled lease without blocking a fresh thread", () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      yield* f.driver.createWorktree({ cwd: f.cwd, refName: f.parentSha, path: null });
      const sink = yield* EventSink.EventSinkV2;
      const fork = yield* ThreadForkService.ThreadForkServiceV2;
      const now = yield* DateTime.now;
      const parentId = ThreadId.make("fork-parent");
      const parent: OrchestrationV2AppThread = {
        id: parentId,
        projectId: ProjectId.make("fork-project"),
        title: "parent",
        createdBy: "user",
        creationSource: "web",
        providerInstanceId: ProviderInstanceId.make("codex"),
        modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "test" },
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: "old-branch",
        worktreePath: f.slot,
        activeProviderThreadId: null,
        lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: parentId },
        forkedFrom: null,
        createdAt: now,
        updatedAt: now,
        archivedAt: null,
        settledOverride: null,
        settledAt: null,
        lastVisitedAt: null,
        deletedAt: null,
      };
      const writeThread = (thread: OrchestrationV2AppThread) =>
        sink.write({
          events: [
            {
              id: EventId.make(`create-${thread.id}`),
              type: "thread.created",
              threadId: thread.id,
              occurredAt: now,
              payload: thread,
            },
          ],
        });
      yield* writeThread(parent);
      const runId = RunId.make("fork-run");
      const plan = (targetThreadId: ThreadId) =>
        fork.plan({
          sourceProjection: { thread: parent },
          sourceRun: {
            id: runId,
            threadId: parentId,
            ordinal: 1,
            providerInstanceId: parent.providerInstanceId,
            modelSelection: parent.modelSelection,
            providerThreadId: ProviderThreadId.make("fork-provider"),
            userMessageId: MessageId.make("fork-message"),
            rootNodeId: null,
            activeAttemptId: null,
            status: "completed",
            queuePosition: null,
            requestedAt: now,
            startedAt: now,
            completedAt: now,
            checkpointId: null,
            contextHandoffId: null,
          },
          sourceProviderThread: undefined,
          canonicalSourcePoint: { threadId: parentId, runId },
          transferId: ContextTransferId.make(`transfer-${targetThreadId}`),
          targetThreadId,
          createdBy: "user",
          creationSource: "web",
          createdAt: now,
        });
      const valid = yield* plan(ThreadId.make("valid-fork"));
      yield* writeThread(valid.targetThread);
      yield* f.leases.assertThread({ path: f.slot, threadId: valid.targetThread.id });
      const owned = yield* f.leases.read(f.slot);
      assert.isNotNull(owned);
      const recycled = { ...owned!.record.lease, lease_id: "b".repeat(32) };
      const pool = f.path.join(f.root, "pool");
      yield* f.fs.writeFileString(
        f.path.join(pool, "treehouse-state.json"),
        JSON.stringify({ version: 4, worktrees: [{ ...recycled, leased: true }] }),
      );
      yield* f.leases.register({ lease: recycled, binary: owned!.record.binary, pool });
      const stale = yield* plan(ThreadId.make("stale-fork"));
      const result = yield* writeThread(stale.targetThread).pipe(Effect.result);
      assert.isTrue(Result.isFailure(result), "fork must not adopt the recycled lease");
      assert.isTrue(
        Result.isFailure(
          yield* f.leases
            .assertThread({ path: f.slot, threadId: stale.targetThread.id })
            .pipe(Effect.result),
        ),
      );
      const fresh = {
        ...parent,
        id: ThreadId.make("fresh-thread"),
        createdBy: "agent" as const,
        branch: "new-branch",
      };
      yield* writeThread(fresh);
      yield* f.leases.assertThread({ path: f.slot, threadId: fresh.id });
    }).pipe(
      Effect.provide(
        EventSink.layer.pipe(
          Layer.provide(
            Layer.merge(EventStore.layer, ProjectionStore.layer).pipe(
              Layer.provideMerge(Sqlite.layerMemory),
            ),
          ),
          Layer.provideMerge(ThreadForkService.layer),
          Layer.provideMerge(testLayer),
        ),
      ),
    ),
  );

  it.effect(
    "queues concurrent lifecycle requests instead of treating the local lock as foreign",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const config = yield* ServerConfig.ServerConfig;
        const registry = path.join(config.stateDir, "treehouse-leases");
        yield* fs.makeDirectory(registry, { recursive: true });
        const canonical = yield* fs.realPath(registry);
        const lock = path.join(registry, "lifecycle.lock");
        let held = false;
        const leases = yield* TreehouseLeases.TreehouseLeaseStore.pipe(
          Effect.provide(TreehouseLeases.layer),
          Effect.provideService(FileSystem.FileSystem, {
            ...fs,
            realPath: (target) =>
              target === registry ? Effect.succeed(canonical) : fs.realPath(target),
            makeDirectory: (target, options) =>
              target === registry
                ? Effect.void
                : target !== lock
                  ? fs.makeDirectory(target, options)
                  : held
                    ? Effect.fail(
                        PlatformError.systemError({
                          module: "FileSystem",
                          method: "makeDirectory",
                          _tag: "AlreadyExists",
                          pathOrDescriptor: target,
                        }),
                      )
                    : Effect.sync(() => {
                        held = true;
                      }),
            remove: (target, options) =>
              target === lock
                ? Effect.sync(() => {
                    held = false;
                  })
                : fs.remove(target, options),
          }),
        );
        const entered = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const first = yield* leases
          .withLock({
            path: registry,
            effect: Deferred.succeed(entered, undefined).pipe(
              Effect.andThen(Deferred.await(release)),
            ),
          })
          .pipe(Effect.forkChild({ startImmediately: true }));
        yield* Deferred.await(entered);
        const second = yield* leases
          .withLock({ path: registry, effect: Effect.succeed("second") })
          .pipe(Effect.forkChild({ startImmediately: true }));
        yield* Deferred.succeed(release, undefined);
        yield* Fiber.join(first);
        assert.strictEqual(yield* Fiber.join(second), "second");
      }).pipe(provide),
  );
});

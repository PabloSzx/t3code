import * as NodeServices from "@effect/platform-node/NodeServices";
import { ThreadId } from "@t3tools/contracts";
import * as TreehouseWorktrees from "./TreehouseWorktrees.ts";
import { assert } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as ServerConfig from "../config.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as ProcessRunner from "../processRunner.ts";
import * as TreehouseLeases from "../workspace/treehouseLeases.ts";
import { makeGitVcsDriverCore } from "./GitVcsDriverCore.ts";

// Only the external allocator is a fixture. Revision selection and removal use real T3 + Git.
const allocator = `import { readFileSync, writeFileSync, appendFileSync, existsSync, mkdirSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";
import { randomBytes } from "node:crypto";
const root = dirname(fileURLToPath(import.meta.url));
const repo = join(root, "repo");
const pool = join(root, "pool");
const slot = join(pool, existsSync(join(root, "next-slot")) ? "2" : "1", "repo");
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
  const state = existsSync(stateFile) ? JSON.parse(readFileSync(stateFile, "utf8")) : { version: 4, worktrees: [] };
  if (state.worktrees.some(entry => entry.path === slot && entry.leased)) process.exit(7);
  mkdirSync(dirname(slot), { recursive: true });
  if (!existsSync(slot)) git(repo, ["worktree", "add", "--detach", slot, "main"]);
  else git(slot, ["switch", "--detach", "main"]);
  const holder = args[args.indexOf("--lease-holder") + 1];
  const lease = { path: slot, lease_id: randomBytes(16).toString("hex"), lease_holder: holder, leased_at: "2026-01-01T00:00:00Z", base_branch: "" };
  state.worktrees = [...state.worktrees.filter(entry => entry.path !== slot), { ...lease, leased: true }];
  writeFileSync(stateFile, JSON.stringify(state));
  console.log(JSON.stringify(lease));
} else if (args[0] === "return") {
  const state = JSON.parse(readFileSync(stateFile, "utf8"));
  const entry = state.worktrees.find(entry => entry.path === args[1]);
  if (args.includes("--force") || args[2] !== "--if-lease-id" || args[3] !== entry?.lease_id) process.exit(4);
  if (existsSync(join(root, "fail-return"))) process.exit(5);
  git(entry.path, ["switch", "--detach", "main"]);
  entry.leased = false;
  writeFileSync(stateFile, JSON.stringify(state));
} else process.exit(6);
`;
export const fixture = Effect.fn("TreehouseTest.fixture")(function* (
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
  if (options.treehouseEnabled !== false)
    yield* settings.updateSettings({
      treehouseWorktreesEnabled: true,
      treehouseBinaryPath: binary,
    });
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
export const accessFixture = Effect.fn("TreehouseTest.accessFixture")(function* () {
  const f = yield* fixture();
  yield* f.driver.createWorktree({
    cwd: f.cwd,
    refName: f.parentSha,
    newRefName: "access-owner",
    path: null,
  });
  const threadId = ThreadId.make("access-owner");
  yield* f.leases.bindThread({ path: f.slot, threadId, sequence: 1 });
  const generation = yield* f.leases.operationGeneration({ path: f.slot, threadId });
  const treehouse = yield* TreehouseWorktrees.TreehouseWorktrees.pipe(
    Effect.provide(TreehouseWorktrees.layer),
  );
  const release = treehouse.returnIdle({
    cwd: f.cwd,
    path: f.slot,
    branch: "access-owner",
    headSha: f.parentSha,
    threadIds: [threadId],
    recheck: Effect.succeed(null),
  });
  const recycle = Effect.gen(function* () {
    yield* release;
    yield* f.driver.createWorktree({
      cwd: f.cwd,
      refName: f.baseSha,
      newRefName: "foreign-owner",
      path: null,
    });
    const threadId = ThreadId.make("foreign-owner");
    yield* f.leases.bindThread({ path: f.slot, threadId, sequence: 2 });
    return {
      threadId,
      generation: yield* f.leases.operationGeneration({ path: f.slot, threadId }),
    };
  });
  return { ...f, treehouse, context: { threadId, generation }, release, recycle };
});

export const testLayer = ProcessRunner.layer.pipe(
  Layer.provideMerge(ServerSettings.layerTest()),
  Layer.provideMerge(
    ServerConfig.layerTest(process.cwd(), { prefix: "treehouse-config-" }).pipe(
      Layer.provideMerge(NodeServices.layer),
    ),
  ),
);

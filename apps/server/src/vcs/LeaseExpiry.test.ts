import { assert, it } from "@effect/vitest";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import { ThreadId } from "@t3tools/contracts";
import * as ProcessRunner from "../processRunner.ts";
import { fixture, testLayer } from "./TreehouseWorktrees.testkit.ts";
import * as TreehouseWorktrees from "./TreehouseWorktrees.ts";

const provide = <A, E, R>(effect: Effect.Effect<A, E, R>) => effect.pipe(Effect.provide(testLayer));

it.live.each(["owned", "foreign", "replacement"] as const)(
  "reclaims an expired %s acquisition without losing branch refs or crossing generations",
  (kind) =>
    Effect.gen(function* () {
      const f = yield* fixture();
      yield* f.settings.updateSettings({ storageCleanup: { leaseHardExpiryDays: 7 } });
      if (kind === "foreign") {
        const runner = yield* ProcessRunner.ProcessRunner;
        assert.equal(
          (yield* runner.run({
            command: f.binary,
            args: ["get", "--lease", "--json", "--lease-holder", "legacy", "--no-fetch"],
            cwd: f.cwd,
            stdin: "",
          })).code,
          0,
        );
      } else {
        yield* f.driver.createWorktree({
          cwd: f.cwd,
          refName: f.parentSha,
          newRefName: "expired-task",
          path: null,
        });
        yield* f.leases.bindThread({ path: f.slot, threadId: "owner", sequence: 1 });
      }
      if (kind === "replacement")
        yield* f.fs.writeFileString(f.path.join(f.root, "reacquire-after-destroy"), "");
      const treehouse = yield* TreehouseWorktrees.TreehouseWorktrees.pipe(
        Effect.provide(TreehouseWorktrees.layer),
      );
      const [lease] = yield* treehouse.listLeases;
      if (!lease) return assert.fail("The fixture must have a held lease");
      yield* f.fs.writeFileString(f.path.join(f.slot, "local-work"), "discardable synthetic work");
      let stopped = false;
      assert.isNull(
        yield* treehouse.expireLease({
          lease,
          days: 7,
          candidateThreadIds: ["owner", "historical-unrelated"],
          recheck: Effect.succeed(null),
          stopOwners: Effect.fn("test.stopOwners")(function* (input) {
            stopped = true;
            assert.deepEqual(input.threadIds, kind === "foreign" ? [] : ["owner"]);
            if (kind !== "foreign")
              assert.equal(
                (yield* Effect.result(f.leases.assertThread({ path: f.slot, threadId: "owner" })))
                  ._tag,
                "Failure",
              );
          }),
        }),
      );
      assert.isTrue(stopped);
      assert.equal(
        yield* f.git({ args: ["rev-parse", `refs/t3/expired-leases/${lease.lease_id}`] }),
        kind === "foreign" ? f.baseSha : f.parentSha,
      );
      assert.equal(yield* f.git({ args: ["rev-parse", "refs/heads/parent"] }), f.parentSha);
      if (kind === "foreign")
        assert.isFalse(
          yield* f.fs.exists(f.path.join(f.registry, "leases", `${lease.lease_id}.json`)),
        );
      else
        assert.isTrue(
          (yield* f.leases.releasedThread({ path: f.slot, threadId: "owner" }))?.record.returned,
        );
      if (kind === "replacement")
        assert.equal(
          yield* f.fs.readFileString(f.path.join(f.slot, "new-owner-work")),
          "keep replacement acquisition",
        );
      else assert.isFalse(yield* f.fs.exists(f.slot));
    }).pipe(provide),
);

it.live(
  "does not stop owners when the allocator generation changes during expiry preparation",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      yield* f.settings.updateSettings({ storageCleanup: { leaseHardExpiryDays: 7 } });
      yield* f.driver.createWorktree({
        cwd: f.cwd,
        refName: f.parentSha,
        newRefName: "old-owner",
        path: null,
      });
      yield* f.leases.bindThread({ path: f.slot, threadId: "owner", sequence: 1 });
      const treehouse = yield* TreehouseWorktrees.TreehouseWorktrees.pipe(
        Effect.provide(TreehouseWorktrees.layer),
      );
      const [lease] = yield* treehouse.listLeases;
      if (!lease) return assert.fail("The fixture must have a held lease");
      let checks = 0;
      let stopped = false;
      const result = yield* treehouse
        .expireLease({
          lease,
          days: 7,
          candidateThreadIds: ["owner"],
          recheck: Effect.gen(function* () {
            checks++;
            if (checks === 2)
              yield* f.fs.writeFileString(
                f.path.join(lease.pool, "treehouse-state.json"),
                JSON.stringify({
                  version: 4,
                  worktrees: [
                    {
                      ...lease,
                      leased: true,
                      lease_id: "b".repeat(32),
                      leased_at: DateTime.formatIso(yield* DateTime.now),
                    },
                  ],
                }),
              );
            return null;
          }).pipe(Effect.orDie),
          stopOwners: () =>
            Effect.sync(() => {
              stopped = true;
            }),
        })
        .pipe(Effect.result);
      assert.equal(result._tag, "Failure");
      assert.isFalse(stopped);
      assert.isTrue(yield* f.fs.exists(f.slot));
    }).pipe(provide),
);

it.live("does not stop a fresh lease owner, and retains an operation-pinned expiry for retry", () =>
  Effect.gen(function* () {
    const f = yield* fixture();
    yield* f.settings.updateSettings({ storageCleanup: { leaseHardExpiryDays: 7 } });
    yield* f.driver.createWorktree({
      cwd: f.cwd,
      refName: f.parentSha,
      newRefName: "expired-task",
      path: null,
    });
    yield* f.leases.bindThread({ path: f.slot, threadId: "owner", sequence: 1 });
    const treehouse = yield* TreehouseWorktrees.TreehouseWorktrees.pipe(
      Effect.provide(TreehouseWorktrees.layer),
    );
    const [lease] = yield* treehouse.listLeases;
    if (!lease) return assert.fail("The fixture must have a held lease");
    let stopped = false;
    const request = {
      lease,
      days: 7,
      candidateThreadIds: ["owner"],
      recheck: Effect.succeed(null),
      stopOwners: () =>
        Effect.sync(() => {
          stopped = true;
        }),
    };
    assert.include(
      (yield* treehouse.expireLease({
        ...request,
        lease: { ...lease, leased_at: DateTime.formatIso(yield* DateTime.now) },
      })) ?? "",
      "age limit",
    );
    assert.isFalse(stopped);
    assert.isNotNull(yield* f.leases.read(f.slot));
    const release = yield* f.leases.pinOperation({
      targets: [f.slot],
      context: { threadId: ThreadId.make("owner"), generation: lease.lease_id },
    });
    assert.equal((yield* Effect.result(treehouse.expireLease(request)))._tag, "Failure");
    assert.isFalse(stopped);
    assert.isTrue(yield* f.fs.exists(f.slot));
    assert.isNotNull(yield* f.leases.read(f.slot));
    yield* release;
    const unsupported = f.path.join(f.root, "old-destroy");
    yield* f.fs.writeFileString(unsupported, "");
    assert.include((yield* treehouse.expireLease(request)) ?? "", "lacks age-checked");
    assert.isFalse(stopped);
    assert.isNotNull(yield* f.leases.read(f.slot));
    yield* f.fs.remove(unsupported);
    assert.isNull(yield* treehouse.expireLease(request));
    assert.isTrue(stopped);
    assert.isFalse(yield* f.fs.exists(f.slot));
  }).pipe(provide),
);

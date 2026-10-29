import { AuthFilesystemWriteScope, type EnvironmentId } from "@t3tools/contracts";
import { type AtomCommandResult } from "@t3tools/client-runtime/state/runtime";
import { useEffect, useState } from "react";

import { appAtomRegistry } from "~/rpc/atomRegistry";
import { useAtomCommand } from "~/state/use-atom-command";
import { projectEnvironment } from "~/state/projects";
import { environmentSession, readEnvironmentScope, sessionHasScope } from "~/state/session";

import { FileSaveCoordinator } from "./fileSaveCoordinator";
import { useProjectFileScope } from "./projectFilesQueryState";

export interface FileSaveTarget {
  readonly environmentId: EnvironmentId;
  readonly cwd: string;
  readonly relativePath: string;
  readonly scope?: ReturnType<typeof useProjectFileScope>;
}

export type FilePendingListener = (relativePath: string, pending: boolean) => void;

interface FileSaveSessionDeps {
  readonly debounceMs: number;
  readonly canPersist: (target: FileSaveTarget) => boolean;
  /** Calls `onWritable` each time the target environment regains write access. */
  readonly subscribeWritable: (target: FileSaveTarget, onWritable: () => void) => () => void;
  readonly confirm: (target: FileSaveTarget, contents: string) => boolean;
  readonly readUnsaved: (target: FileSaveTarget) => string | null;
}

export type FileSavePersist = (options: {
  readonly target: FileSaveTarget;
  readonly contents: string;
  readonly explicitEdit: boolean;
}) => Promise<AtomCommandResult<unknown, unknown>>;

interface FileSaveSession {
  explicitEdit: boolean;
  readonly coordinator: FileSaveCoordinator;
  /** Lease count per listener; surfaces may share one listener. */
  readonly listeners: Map<FilePendingListener, number>;
  readonly unsubscribeWritable: () => void;
  leases: number;
  /** The last release flushed, and the session stays open until that settles. */
  flushingOnRelease: boolean;
}

/**
 * One save session, so one ordered write queue, per file. A released session
 * stays open until its flush settles, so a surface rejoining meanwhile reuses
 * it instead of racing the flush. A session that cannot write closes at once,
 * and the next one, or regained write access, resumes the unsaved draft.
 */
export function createFileSaveSessions(deps: FileSaveSessionDeps) {
  const sessions = new Map<string, FileSaveSession>();

  const recover = (target: FileSaveTarget, session: FileSaveSession) => {
    if (!deps.canPersist(target)) return;
    const unsaved = deps.readUnsaved(target);
    if (unsaved !== null) {
      session.explicitEdit = false;
      session.coordinator.change(unsaved);
    }
  };

  const create = (
    key: string,
    target: FileSaveTarget,
    persist: FileSavePersist,
  ): FileSaveSession => {
    const listeners = new Map<FilePendingListener, number>();
    const coordinator: FileSaveCoordinator = new FileSaveCoordinator({
      debounceMs: deps.debounceMs,
      canPersist: () => deps.canPersist(target),
      // The last surface's listeners outlive it: a flush that settles after
      // the session is released still has to clear the pending marker.
      onPendingChange: (pending) => {
        for (const listener of listeners.keys()) listener(target.relativePath, pending);
      },
      persist: (contents) => persist({ target, contents, explicitEdit: session.explicitEdit }),
      onConfirmed: (contents) => deps.confirm(target, contents),
      onSettled: () => {
        if (!session.flushingOnRelease) return;
        session.flushingOnRelease = false;
        if (session.leases === 0) {
          if (sessions.get(key) === session) sessions.delete(key);
          session.unsubscribeWritable();
        } else if (coordinator.unconfirmedContents !== null) {
          // A surface rejoined while the release flush failed; retry it once.
          recover(target, session);
        }
      },
    });
    const unsubscribeWritable = deps.subscribeWritable(target, () => recover(target, session));
    const session: FileSaveSession = {
      coordinator,
      explicitEdit: false,
      listeners,
      unsubscribeWritable,
      leases: 0,
      flushingOnRelease: false,
    };
    return session;
  };

  const acquire = ({
    target,
    onPendingChange,
    persist,
  }: {
    readonly target: FileSaveTarget;
    readonly onPendingChange: FilePendingListener;
    readonly persist: FileSavePersist;
  }): (() => void) => {
    const key = fileSaveKey(target);
    const existing = sessions.get(key);
    const session = existing ?? create(key, target, persist);
    sessions.set(key, session);
    session.leases += 1;
    session.listeners.set(onPendingChange, (session.listeners.get(onPendingChange) ?? 0) + 1);
    if (!existing) recover(target, session);
    else if (session.coordinator.unconfirmedContents !== null) {
      onPendingChange(target.relativePath, true);
    }

    let released = false;
    return () => {
      if (released) return;
      released = true;
      session.leases -= 1;
      if (session.leases > 0) {
        const count = (session.listeners.get(onPendingChange) ?? 1) - 1;
        if (count > 0) session.listeners.set(onPendingChange, count);
        else session.listeners.delete(onPendingChange);
        return;
      }
      session.flushingOnRelease = true;
      session.coordinator.flush();
    };
  };

  return {
    /** Holds the file's session open until the returned release runs. */
    acquire,
    /** Edits for a file nobody holds open are dropped, so retired editors stay inert. */
    change(target: FileSaveTarget, contents: string): void {
      const session = sessions.get(fileSaveKey(target));
      if (session && session.leases > 0) {
        target.scope?.change(contents);
        session.explicitEdit = true;
        session.coordinator.change(contents);
      }
    },
  };
}

function fileSaveKey({ environmentId, cwd, relativePath, scope }: FileSaveTarget): string {
  return JSON.stringify([
    environmentId,
    cwd,
    relativePath,
    scope?.threadId,
    scope?.workspace ?? null,
  ]);
}

const fileSaveSessions = createFileSaveSessions({
  debounceMs: 500,
  canPersist: ({ environmentId }) => readEnvironmentScope(environmentId, AuthFilesystemWriteScope),
  subscribeWritable: ({ environmentId }, onWritable) => {
    let writable = readEnvironmentScope(environmentId, AuthFilesystemWriteScope);
    return appAtomRegistry.subscribe(
      environmentSession.sessionStateAtom(environmentId),
      (state) => {
        const next = sessionHasScope(state, AuthFilesystemWriteScope);
        if (next && !writable) onWritable();
        writable = next;
      },
    );
  },
  confirm: (target, contents) => target.scope!.confirm(contents),
  readUnsaved: (target) => target.scope!.unsaved()?.contents ?? null,
});

/** Holds the file's save session while the surface is mounted; the last surface to leave flushes. */
export function useFileSaveSession(
  { environmentId, cwd, relativePath }: FileSaveTarget,
  onPendingChange: FilePendingListener,
): FileSaveTarget {
  const scope = useProjectFileScope({ environmentId, cwd, relativePath });
  const writeFile = useAtomCommand(projectEnvironment.writeFileFromEdit);
  const recoverFile = useAtomCommand(projectEnvironment.writeFile);
  const [writer, setWriter] = useState(() => ({ scope, writeFile, recoverFile }));
  const current = writer.scope === scope ? writer : { scope, writeFile, recoverFile };
  if (current !== writer) setWriter(current);
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- owns the external per-file save queue through unmount; its captured scope and writer must survive the final flush.
  useEffect(
    () =>
      fileSaveSessions.acquire({
        target: { environmentId, cwd, relativePath, scope },
        onPendingChange,
        persist: ({ contents, explicitEdit }) =>
          (explicitEdit ? current.writeFile : current.recoverFile)({
            environmentId,
            input: { cwd, relativePath, contents },
            ...(scope.workspace === undefined ? {} : { workspace: scope.workspace }),
          }),
      }),
    [environmentId, cwd, relativePath, scope, onPendingChange, current],
  );
  return { environmentId, cwd, relativePath, scope };
}

export function changeFileSaveSession(target: FileSaveTarget, contents: string): void {
  fileSaveSessions.change(target, contents);
}

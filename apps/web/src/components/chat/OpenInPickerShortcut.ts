import type { EditorId, EnvironmentId, ResolvedKeybindingsConfig } from "@t3tools/contracts";
import { useEffect } from "react";

import { usePreferredEditor } from "../../editorPreferences";
import { editorLabelForPlatform } from "../../editorLabels";
import { isOpenFavoriteEditorShortcut } from "../../keybindings";
import { announce } from "../../lib/announce";
import { useRemoteOpenState, useRemoteCapableEditors } from "../../remoteOpen";
import { useWorkspaceEditorAction } from "../../workspaceEditor";

export function useOpenFavoriteEditorShortcut({
  enabled,
  environmentId,
  keybindings,
  availableEditors,
  openInCwd,
}: {
  enabled: boolean;
  environmentId: EnvironmentId;
  keybindings: ResolvedKeybindingsConfig;
  availableEditors: ReadonlyArray<EditorId>;
  openInCwd: string | null;
}) {
  const openInEditorMutation = useWorkspaceEditorAction(environmentId, { isFile: false });
  const remote = useRemoteOpenState(environmentId);
  const remoteEditors = useRemoteCapableEditors();
  const [preferredEditor] = usePreferredEditor(remote.mode === "local-exec" ? availableEditors : remoteEditors);

  useEffect(() => {
    if (!enabled || remote.mode === "remote-unavailable") return;
    const handler = (event: globalThis.KeyboardEvent) => {
      if (event.defaultPrevented || !isOpenFavoriteEditorShortcut(event, keybindings)) return;
      if (!openInCwd || !preferredEditor) return;

      event.preventDefault();
      const label = editorLabelForPlatform(preferredEditor, navigator.platform);
      void openInEditorMutation({
        environmentId,
        input: {
          cwd: openInCwd,
          editor: preferredEditor,
        },
      }).then((result) => {
        if (result._tag === "Success") announce(`Opened in ${label}`);
      });
    };
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, [
    enabled,
    environmentId,
    keybindings,
    openInCwd,
    openInEditorMutation,
    preferredEditor,
    remote.mode,
  ]);
}

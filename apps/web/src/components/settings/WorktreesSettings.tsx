import { DraftInput } from "../ui/draft-input";
import { useScopedSettingsMixed } from "./useScopedSettings";
import { DEFAULT_UNIFIED_SETTINGS } from "@t3tools/contracts/settings";

import { AdvancedSettings } from "./AdvancedSettings";
import { BranchNamingSettings } from "./BranchNamingSettings";
import { WorktreeSubmodulesRow } from "./ProjectDefaultsSettings";
import { ScopedSwitch } from "./ScopedSwitch";
import { useSettingsScope } from "./SettingsScopeContext";
import {
  SettingResetButton,
  SettingsPageContainer,
  SettingsRow,
  SettingsSection,
} from "./settingsLayout";
import { searchableSetting } from "./settingsSearch";
import { WorktreeCleanupSection, WorktreesDirectoryRow } from "./StorageSettings";
import { useScopedSettings, useUpdateScopedSettings } from "./useScopedSettings";

function TreehouseRows() {
  const settings = useScopedSettings();
  const updateSettings = useUpdateScopedSettings();
  const { connectedEnvironments } = useSettingsScope();
  const supportsTreehouse =
    connectedEnvironments.length > 0 &&
    connectedEnvironments.every(
      (target) => target.serverConfig?.environment.capabilities.treehouseWorktrees === true,
    );
  const mixedTreehouseBinaryPath = useScopedSettingsMixed(["treehouseBinaryPath"]);
  return (
    <>
      {" "}
      <SettingsRow
        serverScoped
        settingKeys={["treehouseWorktreesEnabled"]}
        {...searchableSetting("treehouse-worktrees")}
        description="Use an installed Treehouse CLI on each selected environment for new worktrees in any Git repository. Off uses native Git for new allocations. Existing checkouts and leases stay unchanged. T3 does not install Treehouse or fall back to Git if allocation fails."
        status={
          !supportsTreehouse ? "Update selected environments to enable Treehouse." : undefined
        }
        resetAction={
          supportsTreehouse && settings.treehouseWorktreesEnabled ? (
            <SettingResetButton
              label="Treehouse worktrees"
              onClick={() => updateSettings({ treehouseWorktreesEnabled: false })}
            />
          ) : null
        }
        control={
          <ScopedSwitch
            settingKeys={["treehouseWorktreesEnabled"]}
            checked={settings.treehouseWorktreesEnabled}
            disabled={!supportsTreehouse}
            onCheckedChange={(checked) =>
              updateSettings({ treehouseWorktreesEnabled: Boolean(checked) })
            }
            aria-label="Use Treehouse for new worktrees"
          />
        }
      />
      <SettingsRow
        serverScoped
        settingKeys={["treehouseBinaryPath"]}
        {...searchableSetting("treehouse-binary-path")}
        description="Absolute executable path on the environment's host, not this client. Leave empty to find treehouse on the host PATH. Requires identified leases, conditional return, and version 4 pool snapshots."
        resetAction={
          supportsTreehouse && (mixedTreehouseBinaryPath || settings.treehouseBinaryPath !== "") ? (
            <SettingResetButton
              label="Treehouse binary path"
              onClick={() => updateSettings({ treehouseBinaryPath: "" })}
            />
          ) : null
        }
        control={
          <DraftInput
            size="sm"
            className="w-full sm:w-72"
            value={mixedTreehouseBinaryPath ? "" : settings.treehouseBinaryPath}
            onCommit={(next) => updateSettings({ treehouseBinaryPath: next })}
            placeholder={mixedTreehouseBinaryPath ? "Mixed" : "Host PATH"}
            disabled={!supportsTreehouse}
            spellCheck={false}
            aria-label="Treehouse binary path"
          />
        }
      />
    </>
  );
}

function StartFromRemoteRow() {
  const settings = useScopedSettings();
  const updateSettings = useUpdateScopedSettings();
  return (
    <SettingsRow
      serverScoped
      settingKeys={["newWorktreesStartFromOrigin"]}
      {...searchableSetting("start-from-origin")}
      description="Uses the latest commit of the base branch’s tracked remote branch, even if its name differs. Otherwise uses the same-named branch on origin. Uses your local branch if that remote branch is missing."
      resetAction={
        settings.newWorktreesStartFromOrigin !==
        DEFAULT_UNIFIED_SETTINGS.newWorktreesStartFromOrigin ? (
          <SettingResetButton
            label="new worktrees start from remote"
            onClick={() =>
              updateSettings({
                newWorktreesStartFromOrigin: DEFAULT_UNIFIED_SETTINGS.newWorktreesStartFromOrigin,
              })
            }
          />
        ) : null
      }
      control={
        <ScopedSwitch
          settingKeys={["newWorktreesStartFromOrigin"]}
          checked={settings.newWorktreesStartFromOrigin}
          onCheckedChange={(checked) =>
            updateSettings({ newWorktreesStartFromOrigin: Boolean(checked) })
          }
          aria-label="Start new worktrees from remote by default"
        />
      }
    />
  );
}

export function WorktreesSettingsPanel() {
  const { scope } = useSettingsScope();
  const isProjectScope = scope.kind === "project" || scope.kind === "checkout";

  return (
    <SettingsPageContainer>
      <SettingsSection id="worktree-creation" title="Creation">
        {!isProjectScope && <WorktreesDirectoryRow />}
        {!isProjectScope && <TreehouseRows />}
        <StartFromRemoteRow />
        <AdvancedSettings targetIds={[searchableSetting("worktree-submodules").id]}>
          <WorktreeSubmodulesRow />
        </AdvancedSettings>
      </SettingsSection>
      <SettingsSection id="worktree-branches" title="Branches">
        <BranchNamingSettings />
      </SettingsSection>
      <WorktreeCleanupSection />
    </SettingsPageContainer>
  );
}

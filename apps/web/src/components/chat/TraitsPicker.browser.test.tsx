import {
  ProviderDriverKind,
  ProviderInstanceId,
  type ModelSelection,
  type ProviderOptionSelection,
  type ServerProviderModel,
} from "@t3tools/contracts";
import { expect, it, vi } from "vite-plus/test";
import { page, userEvent } from "vite-plus/test/browser";

import { render } from "../../../test/render";
import { TraitsPicker } from "./TraitsPicker";

const instanceId = ProviderInstanceId.make("pi");
const models: ReadonlyArray<ServerProviderModel> = [
  "od-ai/opendoor/opus-5.5",
  "openai-codex/gpt-6.1-sol",
].map((slug) => ({
  slug,
  name: slug,
  isCustom: false,
  capabilities: {
    optionDescriptors: [
      {
        id: "thinking",
        label: "Thinking",
        type: "select",
        options: [
          { id: "low", label: "Low" },
          { id: "medium", label: "Medium" },
          { id: "high", label: "High" },
          { id: "xhigh", label: "Extra High" },
          { id: "max", label: "Max" },
        ],
      },
    ],
  },
}));

it.each<{
  name: string;
  model: string;
  options?: ReadonlyArray<ProviderOptionSelection>;
  reportedOptions?: ModelSelection["options"];
  label: string;
  selected?: string;
}>([
  { name: "new Opus draft", model: models[0]!.slug, label: "Thinking" },
  { name: "new Sol draft", model: models[1]!.slug, label: "Thinking" },
  {
    name: "saved effort",
    model: models[1]!.slug,
    options: [{ id: "thinking", value: "xhigh" }],
    label: "Extra High",
    selected: "Extra High",
  },
  {
    name: "reported effort",
    model: models[1]!.slug,
    reportedOptions: [{ id: "thinking", value: "medium" }],
    label: "Medium",
    selected: "Medium",
  },
])(
  "keeps effort selectable for $name",
  async ({ model, options, reportedOptions, label, selected }) => {
    const onModelOptionsChange = vi.fn();
    const props = {
      provider: ProviderDriverKind.make("pi"),
      instanceId,
      models,
      model,
      planModeEnabled: false,
      isComposerOwned: true,
      onModelOptionsChange,
      ...(reportedOptions
        ? { reportedModelSelection: { instanceId, model, options: reportedOptions } }
        : {}),
    };
    await using screen = await render(<TraitsPicker {...props} modelOptions={options} />, {
      router: false, // The controlled picker does not navigate.
    });
    const trigger = screen.getByRole("button", { name: `Effort: ${label}` });
    await expect.element(trigger).toBeVisible();
    await expect.element(trigger).toHaveTextContent(label);
    await userEvent.click(trigger);
    for (const choice of ["Low", "Medium", "High", "Extra High", "Max"]) {
      await expect
        .element(page.getByRole("menuitemradio", { name: choice, exact: true }))
        .toHaveAttribute("aria-checked", String(choice === selected));
    }
    expect(onModelOptionsChange).not.toHaveBeenCalled();
    await userEvent.click(page.getByRole("menuitemradio", { name: "Max", exact: true }));
    expect(onModelOptionsChange).toHaveBeenCalledExactlyOnceWith([
      { id: "thinking", value: "max" },
    ]);

    await screen.rerender(
      <TraitsPicker {...props} modelOptions={[{ id: "thinking", value: "max" }]} />,
    );
    const updatedTrigger = screen.getByRole("button", { name: "Effort: Max" });
    await expect.element(updatedTrigger).toHaveTextContent("Max");
    await userEvent.click(updatedTrigger);
    await expect
      .element(page.getByRole("menuitemradio", { name: "Max", exact: true }))
      .toHaveAttribute("aria-checked", "true");
    await userEvent.keyboard("{Escape}");
  },
);

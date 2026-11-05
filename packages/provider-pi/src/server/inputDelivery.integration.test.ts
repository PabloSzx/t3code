// @effect-diagnostics nodeBuiltinImport:off -- Exercises the shipped extension against official Pi and isolated native session files.
import * as NodeFSP from "node:fs/promises";
import * as NodeModule from "node:module";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeVM from "node:vm";
import { assert, it } from "@effect/vitest";
import { MessageId } from "@t3tools/contracts";
import * as Pi from "@earendil-works/pi-coding-agent";
import { fauxAssistantMessage, type TranscriptContext } from "@earendil-works/pi-ai";
import { registerFauxProvider, streamSimple } from "@earendil-works/pi-ai/compat";
import { PI_T3_MCP_EXTENSION_SOURCE } from "./mcpExtensionSource.ts";
import {
  PI_T3_INPUT_COMMAND,
  PI_T3_INPUT_MESSAGE_TYPE,
  PI_T3_INPUT_REPLY_PREFIX,
} from "./inputDeliveryProtocol.ts";

const image = {
  type: "image" as const,
  mimeType: "image/png",
  data: "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVQIHWP4z8DwHwAFgAI/ScLbtAAAAABJRU5ErkJggg==",
};

async function createNativeHarness({
  directory,
  sessionFile,
  readSkillFile,
}: {
  directory: string;
  sessionFile?: string;
  readSkillFile?: () => Promise<string>;
}) {
  const skillPath = NodePath.join(directory, "agent", "skills", "alpha", "SKILL.md");
  await NodeFSP.mkdir(NodePath.dirname(skillPath), { recursive: true });
  await NodeFSP.writeFile(
    skillPath,
    "---\nname: alpha\ndescription: Synthetic test skill.\n---\nALPHA_INSTRUCTIONS\n",
  );
  const faux = registerFauxProvider({ provider: "t3-delivery-test", tokensPerSecond: 0 });
  const model = faux.getModel();
  const settingsManager = Pi.SettingsManager.inMemory({
    compaction: { enabled: false, keepRecentTokens: 1 },
  });
  const modelRuntime = await Pi.ModelRuntime.create({
    authPath: NodePath.join(directory, "auth.json"),
    modelsPath: null,
    refreshOnCreate: false,
    allowModelNetwork: false,
  });
  modelRuntime.registerProvider(model.provider, {
    api: faux.api,
    apiKey: "fake-test-key",
    baseUrl: model.baseUrl,
    models: faux.models,
    streamSimple,
  });
  const notifications: string[] = [];
  const inputs: string[] = [];
  const source = NodeModule.stripTypeScriptTypes(
    PI_T3_MCP_EXTENSION_SOURCE.replace(/^import .*;$/gm, "").replace(
      "export default async function",
      "async function",
    ),
  );
  const bridge: Pi.ExtensionFactory = async (pi) => {
    await NodeVM.runInNewContext(`${source}\nt3McpExtension(pi)`, {
      process: { env: {}, argv: [] },
      NodeFSP: {
        ...NodeFSP,
        readFile: (path: string, encoding: "utf8") =>
          path === skillPath && readSkillFile !== undefined
            ? readSkillFile()
            : NodeFSP.readFile(path, encoding),
      },
      NodePath,
      VERSION: Pi.VERSION,
      stripFrontmatter: Pi.stripFrontmatter,
      pi: {
        ...pi,
        registerCommand: (name, command) =>
          pi.registerCommand(name, {
            ...command,
            handler: (args, ctx) =>
              command.handler(
                args,
                new Proxy(ctx, {
                  get: (target, key, receiver) =>
                    key === "ui"
                      ? { ...target.ui, notify: (message: string) => notifications.push(message) }
                      : Reflect.get(target, key, receiver),
                }),
              ),
          }),
      } satisfies Pi.ExtensionAPI,
    });
  };
  const resourceLoader = new Pi.DefaultResourceLoader({
    cwd: directory,
    agentDir: NodePath.join(directory, "agent"),
    settingsManager,
    noExtensions: true,
    noSkills: false,
    noPromptTemplates: true,
    noThemes: true,
    noContextFiles: true,
    extensionFactories: [
      bridge,
      (pi) => {
        pi.on("input", (event) => {
          inputs.push(event.text);
          return { action: "transform", text: `Prepared: ${event.text}` };
        });
      },
    ],
  });
  await resourceLoader.reload();
  const sessionManager =
    sessionFile === undefined
      ? Pi.SessionManager.create(directory, NodePath.join(directory, "sessions"))
      : Pi.SessionManager.open(sessionFile, NodePath.join(directory, "sessions"), directory);
  const { session } = await Pi.createAgentSession({
    cwd: directory,
    agentDir: NodePath.join(directory, "agent"),
    modelRuntime,
    model,
    sessionManager,
    settingsManager,
    resourceLoader,
    noTools: "all",
  });
  await session.bindExtensions({});
  let nextRequestId = 0;
  const request = async (args: Record<string, unknown>) => {
    const requestId = `native-test-${nextRequestId++}`;
    await session.prompt(
      `/${PI_T3_INPUT_COMMAND} ${JSON.stringify({ ...args, requestId, nativeThreadId: sessionManager.getSessionFile() })}`,
    );
    const reply = notifications
      .filter((message) => message.startsWith(PI_T3_INPUT_REPLY_PREFIX))
      .map((message): unknown => JSON.parse(message.slice(PI_T3_INPUT_REPLY_PREFIX.length)))
      .find(
        (value) =>
          typeof value === "object" &&
          value !== null &&
          "requestId" in value &&
          value.requestId === requestId,
      );
    assert.isDefined(reply, "The injected extension must acknowledge its command");
    return reply;
  };
  return { session, sessionManager, faux, inputs, request };
}

it("recovers queued IDs before continuation and never duplicates recorded identical questions", async () => {
  const directory = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-pi-delivery-"));
  const reachedModel = Promise.withResolvers<void>();
  const releaseModel = Promise.withResolvers<ReturnType<typeof fauxAssistantMessage>>();
  const harnesses: Awaited<ReturnType<typeof createNativeHarness>>[] = [];
  let firstRun: Promise<void> | undefined;
  try {
    const first = await createNativeHarness({ directory });
    harnesses.push(first);
    const contexts: TranscriptContext[] = [];
    first.faux.setResponses([
      async (context) => {
        contexts.push(context);
        reachedModel.resolve();
        return releaseModel.promise;
      },
      (context) => {
        contexts.push(context);
        return fauxAssistantMessage("Done");
      },
      (context) => {
        contexts.push(context);
        return fauxAssistantMessage("Done");
      },
    ]);
    firstRun = first.session.prompt("Start");
    await reachedModel.promise;
    const messages = [
      {
        inputId: MessageId.make(`message:thread:${"nested%3A".repeat(40)}:same-question-a`),
        text: "Does this simplify it? $alpha",
        images: [image],
      },
      { inputId: "same-question-b", text: "Does this simplify it? $alpha", images: [] },
    ];
    for (const message of messages) {
      assert.deepInclude(await first.request({ operation: "deliver", mode: "steer", ...message }), {
        success: true,
        data: { inputId: message.inputId, status: "queued" },
      });
    }
    const pendingSnapshot = NodePath.join(directory, "pending.jsonl");
    await NodeFSP.copyFile(first.sessionManager.getSessionFile()!, pendingSnapshot);
    assert.notInclude(await NodeFSP.readFile(pendingSnapshot, "utf8"), "same-question-a");
    releaseModel.resolve(fauxAssistantMessage("First response"));
    await firstRun;
    assert.isFalse(
      first.session.messages.some(
        (message) => message.role === "assistant" && message.stopReason === "error",
      ),
    );
    assert.equal(contexts.at(-1)?.messages.filter((message) => message.role === "user").length, 3);
    assert.notInclude(JSON.stringify(contexts), "same-question-a");

    const recovered = await createNativeHarness({ directory, sessionFile: pendingSnapshot });
    harnesses.push(recovered);
    for (const message of messages) {
      assert.deepInclude(
        await recovered.request({ operation: "deliver", mode: "restore", ...message }),
        { success: true, data: { inputId: message.inputId, status: "recorded" } },
      );
    }
    const recordedSnapshot = NodePath.join(directory, "recorded.jsonl");
    await NodeFSP.copyFile(pendingSnapshot, recordedSnapshot);
    const again = await createNativeHarness({ directory, sessionFile: recordedSnapshot });
    harnesses.push(again);
    const priorBytes = await NodeFSP.readFile(recordedSnapshot, "utf8");
    for (const message of messages) {
      assert.deepInclude(
        await again.request({ operation: "deliver", mode: "restore", ...message }),
        {
          success: true,
          data: { inputId: message.inputId, status: "recorded" },
        },
      );
    }
    assert.equal(await NodeFSP.readFile(recordedSnapshot, "utf8"), priorBytes);
    assert.equal(again.faux.state.callCount, 0);
    let recoveredContext: TranscriptContext | undefined;
    again.faux.setResponses([
      (context) => {
        recoveredContext = context;
        return fauxAssistantMessage("Recovered");
      },
    ]);
    await again.session.prompt("Continue");
    const userMessages = recoveredContext?.messages.filter((message) => message.role === "user");
    assert.equal(userMessages?.length, 4);
    assert.deepEqual(userMessages?.[0]?.content, [{ type: "text", text: "Prepared: Start" }]);
    assert.deepEqual(userMessages?.[3]?.content, [{ type: "text", text: "Prepared: Continue" }]);
    for (const message of userMessages?.slice(1, 3) ?? []) {
      const content = message.content;
      assert.isArray(content);
      const text =
        typeof content === "string"
          ? content
          : content
              .filter((part) => part.type === "text")
              .map((part) => part.text)
              .join("");
      assert.isTrue(text.startsWith("Does this simplify it? $alpha\n\n"));
      assert.equal(text.split("ALPHA_INSTRUCTIONS").length, 2);
    }
    const imageContent = userMessages?.[1]?.content;
    assert.deepEqual(
      typeof imageContent === "string"
        ? undefined
        : imageContent?.find((part) => part.type === "image"),
      image,
    );
    const customEntries = again.sessionManager
      .getBranch()
      .filter((entry) => entry.type === "custom_message");
    assert.equal(
      customEntries.filter((entry) => entry.customType === PI_T3_INPUT_MESSAGE_TYPE).length,
      2,
    );
    assert.notInclude(JSON.stringify(recoveredContext), "same-question-a");
    assert.notInclude(JSON.stringify(recoveredContext), "same-question-b");
    assert.deepEqual(first.inputs, ["Start"]);
    assert.deepEqual(again.inputs, ["Continue"]);
  } finally {
    releaseModel.resolve(fauxAssistantMessage("Cleanup"));
    await firstRun;
    for (const harness of harnesses.reverse()) {
      harness.session.dispose();
      harness.faux.unregister();
    }
    await NodeFSP.rm(directory, { recursive: true, force: true });
  }
});

it("defers delivery during native compaction and records it before the following prompt", async () => {
  const directory = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-pi-compact-"));
  const reachedCompaction = Promise.withResolvers<void>();
  const releaseCompaction = Promise.withResolvers<ReturnType<typeof fauxAssistantMessage>>();
  const harness = await createNativeHarness({ directory });
  let compaction: Promise<unknown> | undefined;
  try {
    harness.faux.setResponses([
      fauxAssistantMessage("First response"),
      fauxAssistantMessage("Second response"),
      () => {
        reachedCompaction.resolve();
        return releaseCompaction.promise;
      },
      fauxAssistantMessage("Summary"),
    ]);
    await harness.session.prompt("Earlier context ".repeat(200));
    await harness.session.prompt("Recent context");
    compaction = harness.session.compact();
    await reachedCompaction.promise;
    const calls = harness.faux.state.callCount;
    const message = { inputId: "compacting-question", text: "Steer after compact", images: [] };
    assert.deepInclude(await harness.request({ operation: "deliver", mode: "steer", ...message }), {
      success: true,
      data: { inputId: message.inputId, status: "busy" },
    });
    assert.equal(harness.faux.state.callCount, calls);
    releaseCompaction.resolve(fauxAssistantMessage("Summary"));
    await compaction;
    assert.deepInclude(
      await harness.request({ operation: "deliver", mode: "restore", ...message }),
      {
        success: true,
        data: { inputId: message.inputId, status: "recorded" },
      },
    );
    let context: TranscriptContext | undefined;
    harness.faux.setResponses([
      (next) => {
        context = next;
        return fauxAssistantMessage("Done");
      },
    ]);
    await harness.session.prompt("Continue");
    assert.deepEqual(
      context?.messages
        .filter((entry) => entry.role === "user")
        .slice(-2)
        .map((entry) => entry.content),
      [
        [{ type: "text", text: "Steer after compact" }],
        [{ type: "text", text: "Prepared: Continue" }],
      ],
    );
  } finally {
    releaseCompaction.resolve(fauxAssistantMessage("Cleanup"));
    await compaction;
    harness.session.dispose();
    harness.faux.unregister();
    await NodeFSP.rm(directory, { recursive: true, force: true });
  }
});

it("persists cancellation before clearing the queue and prevents late skill preparation from enqueueing", async () => {
  const directory = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-pi-cancel-"));
  const reachedModel = Promise.withResolvers<void>();
  const releaseModel = Promise.withResolvers<ReturnType<typeof fauxAssistantMessage>>();
  const reachedSkill = Promise.withResolvers<void>();
  const releaseSkill = Promise.withResolvers<string>();
  const harnesses: Awaited<ReturnType<typeof createNativeHarness>>[] = [];
  let run: Promise<void> | undefined;
  let delivery: Promise<unknown> | undefined;
  try {
    const first = await createNativeHarness({
      directory,
      readSkillFile: () => {
        reachedSkill.resolve();
        return releaseSkill.promise;
      },
    });
    harnesses.push(first);
    first.faux.setResponses([
      () => {
        reachedModel.resolve();
        return releaseModel.promise;
      },
    ]);
    run = first.session.prompt("Start");
    await reachedModel.promise;
    const message = { inputId: "cancelled-question", text: "Check $alpha", images: [] };
    delivery = first.request({ operation: "deliver", mode: "steer", ...message });
    await reachedSkill.promise;
    assert.deepInclude(await first.request({ operation: "cancel", inputIds: ["held-question"] }), {
      success: true,
    });
    first.session.clearQueue();
    const snapshot = NodePath.join(directory, "cancelled.jsonl");
    await NodeFSP.copyFile(first.sessionManager.getSessionFile()!, snapshot);
    releaseSkill.resolve("ALPHA_INSTRUCTIONS");
    assert.deepInclude(await delivery, {
      success: true,
      data: { inputId: message.inputId, status: "cancelled" },
    });
    const stopped = first.session.abort();
    releaseModel.resolve(fauxAssistantMessage("Stopped"));
    await stopped;
    await run;
    assert.equal(first.faux.state.callCount, 1);

    const recovered = await createNativeHarness({ directory, sessionFile: snapshot });
    harnesses.push(recovered);
    const bytes = await NodeFSP.readFile(snapshot, "utf8");
    assert.deepInclude(
      await recovered.request({
        operation: "status",
        inputIds: [message.inputId, "held-question"],
      }),
      {
        success: true,
        data: {
          statuses: [
            { inputId: message.inputId, status: "cancelled" },
            { inputId: "held-question", status: "cancelled" },
          ],
        },
      },
    );
    assert.deepInclude(
      await recovered.request({ operation: "deliver", mode: "restore", ...message }),
      {
        success: true,
        data: { inputId: message.inputId, status: "cancelled" },
      },
    );
    assert.equal(await NodeFSP.readFile(snapshot, "utf8"), bytes);
    assert.equal(recovered.faux.state.callCount, 0);
    assert.isFalse(
      recovered.sessionManager
        .getEntries()
        .some(
          (entry) =>
            entry.type === "custom_message" && entry.customType === PI_T3_INPUT_MESSAGE_TYPE,
        ),
    );
  } finally {
    releaseSkill.resolve("ALPHA_INSTRUCTIONS");
    releaseModel.resolve(fauxAssistantMessage("Cleanup"));
    await delivery;
    await run;
    for (const harness of harnesses.reverse()) {
      harness.session.dispose();
      harness.faux.unregister();
    }
    await NodeFSP.rm(directory, { recursive: true, force: true });
  }
});

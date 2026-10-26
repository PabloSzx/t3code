import type { EnvironmentId, ProviderInstanceId, ThreadId } from "@t3tools/contracts";

export interface McpProviderSessionConfig {
  readonly environmentId: EnvironmentId;
  readonly threadId: ThreadId;
  readonly providerSessionId: string;
  readonly providerInstanceId: ProviderInstanceId;
  readonly endpoint: string;
  readonly authorizationHeader: string;
  /** Omit preview_* instructions when the user withholds agent browser access. */
  readonly browserToolsAvailable: boolean;
  readonly capabilities?: ReadonlySet<string>;
  readonly agentDeviceEnvironment?: Readonly<Record<string, string>>;
}

const T3_MCP_URL_ENV = "T3_MCP_URL";
const T3_MCP_BEARER_ENV = "T3_MCP_BEARER_TOKEN";

interface McpSessionEnvironmentOptions {
  readonly environment: NodeJS.ProcessEnv;
  readonly mcpSession:
    | Pick<McpProviderSessionConfig, "endpoint" | "authorizationHeader">
    | undefined;
}

export function withMcpSessionEnvironment(input: McpSessionEnvironmentOptions): NodeJS.ProcessEnv {
  // Explicit undefined also removes inherited values when a spawner extends the host environment.
  const environment: NodeJS.ProcessEnv = {
    ...input.environment,
    [T3_MCP_URL_ENV]: undefined,
    [T3_MCP_BEARER_ENV]: undefined,
    T3_ACP_MCP_ENDPOINT: undefined,
    T3_ACP_MCP_AUTHORIZATION: undefined,
    T3_ACP_MCP_NODE: undefined,
    T3_ACP_MCP_ENTRYPOINT: undefined,
  };
  const session = input.mcpSession;
  const token = session?.authorizationHeader.match(/^Bearer\s+(.+)$/i)?.[1];
  if (session !== undefined && token !== undefined) {
    environment[T3_MCP_URL_ENV] = session.endpoint;
    environment[T3_MCP_BEARER_ENV] = token;
  }
  return environment;
}

export function withAgentDeviceEnvironment(
  base: NodeJS.ProcessEnv,
  config: Pick<McpProviderSessionConfig, "agentDeviceEnvironment"> | undefined,
): NodeJS.ProcessEnv {
  const extra = config?.agentDeviceEnvironment;
  if (!extra) return base;
  const separator = extra.PATH_SEPARATOR ?? ":";
  const basePath = base.PATH ?? base.Path;
  const { PATH: shimDir, PATH_SEPARATOR: _separator, ...rest } = extra;
  return {
    ...base,
    ...rest,
    ...(shimDir ? { PATH: basePath ? `${shimDir}${separator}${basePath}` : shimDir } : {}),
  };
}

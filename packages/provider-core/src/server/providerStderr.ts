/** Bounded, redacted stderr tails for diagnosing a provider process that failed to start. */
const PROVIDER_STDERR_TAIL_MAX_CHARS = 4_096;

const PAIRING_URL_PATTERN = /https?:\/\/[^\s]*\/pair#[^\s]*/gi;
const SIGN_IN_URL_PATTERN = /https?:\/\/accounts\.google\.com[^\s]*/gi;
const BEARER_TOKEN_PATTERN = /\bBearer\s+[A-Za-z0-9._\-+=/]+/gi;
const BASIC_AUTH_PATTERN = /\bAuthorization:\s*Basic\s+\S+/gi;
const API_KEY_HEADER_PATTERN = /\bx-api-key:\s*\S+/gi;
// Whole names ending in a sensitive word, such as OPENAI_API_KEY or T3_MCP_BEARER_TOKEN, optionally
// quoted as JSON property names.
const SECRET_ASSIGNMENT_PATTERN =
  /((?:["'])?[A-Za-z0-9_.-]*(?:access[_-]?token|api[_-]?key|credential|password|passwd|secret|token|auth)(?:["'])?\s*[:=]\s*)(?:"[^"]*"|'[^']*'|[^\s,;]+)/gi;
const SECRET_TOKEN_PATTERN =
  /\b(?:sk-[A-Za-z0-9][A-Za-z0-9_-]{7,}|ghp_[A-Za-z0-9]+|xox[a-zA-Z]-[A-Za-z0-9-]+)\b/g;

export function appendProviderStderrTail(current: string, chunk: string): string {
  const next = `${current}${chunk}`;
  return next.length <= PROVIDER_STDERR_TAIL_MAX_CHARS
    ? next
    : next.slice(-PROVIDER_STDERR_TAIL_MAX_CHARS);
}

export function sanitizeProviderStderrExcerpt(
  text: string,
  homeDirectory: string,
  environment: NodeJS.ProcessEnv = process.env,
): string {
  let result = text.replaceAll("\0", "");
  const homes = [environment.HOME, environment.USERPROFILE, homeDirectory].filter(
    (value): value is string => typeof value === "string" && value.length > 1,
  );
  for (const home of new Set(homes)) {
    result = result.split(home).join("~");
  }
  result = result
    .replace(PAIRING_URL_PATTERN, "[pairing-url]")
    .replace(SIGN_IN_URL_PATTERN, "[sign-in-url]")
    // Header forms win over generic `key=value` assignments, so their labels survive.
    .replace(BEARER_TOKEN_PATTERN, "Bearer [redacted]")
    .replace(BASIC_AUTH_PATTERN, "Authorization: Basic [redacted]")
    .replace(API_KEY_HEADER_PATTERN, "x-api-key: [redacted]")
    .replace(SECRET_ASSIGNMENT_PATTERN, "$1[redacted]")
    .replace(SECRET_TOKEN_PATTERN, "[redacted]");
  return result.trim();
}

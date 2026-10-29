import { InvalidRequestError } from "../../../application/errors.js";
import { requestBinding } from "../mcp-request-binding.js";

export const CUSTOM_API_RUNTIME_OPERATIONS = {
  describe: "custom_api.describe_environment",
  acquire: "custom_api.acquire_environment",
} as const;
export const CUSTOM_API_RUNTIME_METHODS = {
  describe:
    "kokoro.platform.v1.CustomApiRuntimeService/DescribeCustomApiEnvironment",
  acquire:
    "kokoro.platform.v1.CustomApiRuntimeService/AcquireCustomApiEnvironment",
} as const;
export const CUSTOM_API_RUNTIME_LIMITS = {
  profiles: 32,
  environment: 128,
  environmentBytes: 65_536,
} as const;
export type CustomApiRuntimeProfileRef = Readonly<{
  profileId: string;
  expectedStateVersion: number;
}>;
export type CustomApiRuntimeRequest = Readonly<{
  kind: keyof typeof CUSTOM_API_RUNTIME_OPERATIONS;
  requestId: string;
  invocationId: string;
  profiles: readonly CustomApiRuntimeProfileRef[];
}>;
const id = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,190}$/u;
export function validateCustomApiRuntimeRequest(
  input: CustomApiRuntimeRequest,
): void {
  if (
    !input ||
    !Object.hasOwn(CUSTOM_API_RUNTIME_OPERATIONS, input.kind) ||
    typeof input.requestId !== "string" ||
    !id.test(input.requestId) ||
    typeof input.invocationId !== "string" ||
    !id.test(input.invocationId) ||
    !Array.isArray(input.profiles) ||
    input.profiles.length < 1 ||
    input.profiles.length > CUSTOM_API_RUNTIME_LIMITS.profiles ||
    Reflect.ownKeys(input).some(
      (key) =>
        !["kind", "requestId", "invocationId", "profiles"].includes(
          String(key),
        ),
    )
  )
    throw new InvalidRequestError("Custom API runtime request invalid");
  const seen = new Set<string>();
  const profiles: readonly CustomApiRuntimeProfileRef[] = input.profiles;
  for (const ref of profiles) {
    if (
      !ref ||
      typeof ref.profileId !== "string" ||
      !id.test(ref.profileId) ||
      !Number.isInteger(ref.expectedStateVersion) ||
      ref.expectedStateVersion < 1 ||
      ref.expectedStateVersion > 2_147_483_647 ||
      seen.has(ref.profileId) ||
      Reflect.ownKeys(ref).some(
        (key) => !["profileId", "expectedStateVersion"].includes(String(key)),
      )
    )
      throw new InvalidRequestError(
        "Custom API runtime profile selection invalid",
      );
    seen.add(ref.profileId);
  }
}
export function customApiRuntimeBinding(
  tenantId: string,
  input: CustomApiRuntimeRequest,
): string {
  validateCustomApiRuntimeRequest(input);
  if (typeof tenantId !== "string" || !/^[\x21-\x7e]{1,255}$/u.test(tenantId))
    throw new InvalidRequestError("Custom API runtime tenant invalid");
  return requestBinding(CUSTOM_API_RUNTIME_METHODS[input.kind], tenantId, {
    requestId: input.requestId,
    invocationId: input.invocationId,
    profiles: [...input.profiles].sort((a, b) =>
      Buffer.from(a.profileId).compare(Buffer.from(b.profileId)),
    ),
  });
}

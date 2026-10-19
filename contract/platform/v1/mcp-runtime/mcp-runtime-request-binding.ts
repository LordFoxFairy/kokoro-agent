import { createHash } from "node:crypto";
import { InvalidRequestError } from "../../../application/errors.js";
import {
  mcpConnectorId,
  mcpConnectionId,
  mcpServerId,
  mcpInvocationGrant,
} from "../../../rpc/platform-identities.js";
import { requestBinding, presence } from "../mcp-request-binding.js";

export const MCP_RUNTIME_METHODS = {
  resolve: "kokoro.platform.v1.McpRuntimeService/ResolveMcpRuntimeConnection",
  acquire: "kokoro.platform.v1.McpRuntimeService/AcquireMcpRuntimeCredential",
} as const;
export const MCP_RUNTIME_OPERATIONS = {
  resolve: "mcp.resolve_runtime_connection",
  acquire: "mcp.acquire_runtime_credential",
} as const;
export const MCP_RUNTIME_LIMITS = {
  identifier: 191,
  tenant: 255,
  selector: 512,
  resolutionRef: 8192,
  typedArgumentsBytes: 1_048_576,
  credentialLifetimeSeconds: 30,
} as const;

type Target = Readonly<{
  requestId: string;
  connectorId: string;
  connectionId: string;
  serverId: string;
  toolSelector: string;
  expectedDeclarationDigest: string;
}>;
export type McpRuntimeCredentialUse =
  | Readonly<{ kind: "discovery" }>
  | Readonly<{
      kind: "invocation";
      invocationGrant: string;
      auditRef: string;
      typedArgumentsJson: Uint8Array;
      approvalRef?: string;
      idempotencyKey: string;
    }>;
export type McpRuntimeRequest =
  | (Target & Readonly<{ kind: "resolve" }>)
  | (Target &
      Readonly<{
        kind: "acquire";
        resolutionRef: string;
        use: McpRuntimeCredentialUse;
      }>);

function invalid(): never {
  throw new InvalidRequestError("MCP runtime request binding is invalid");
}
function closed(
  value: unknown,
  required: readonly string[],
  optional: readonly string[] = [],
): void {
  if (typeof value !== "object" || value === null || Array.isArray(value))
    invalid();
  const keys = Reflect.ownKeys(value);
  if (
    required.some((key) => !Object.hasOwn(value, key)) ||
    keys.some(
      (key) =>
        typeof key !== "string" || ![...required, ...optional].includes(key),
    )
  )
    invalid();
}
function text(
  value: unknown,
  max: number = MCP_RUNTIME_LIMITS.identifier,
): string {
  if (
    typeof value !== "string" ||
    !value ||
    value.length > max ||
    Buffer.from(value, "utf8").toString("utf8") !== value ||
    value.trim() !== value ||
    [...value].some(
      (char) => char.charCodeAt(0) < 32 || char.charCodeAt(0) === 127,
    )
  )
    invalid();
  return value;
}
function hash(value: string | Uint8Array): string {
  return createHash("sha256").update(value).digest("hex");
}

/** Exact operation-bound input only; neither issuance nor an execution grant. */
export function mcpRuntimeRequestBinding(
  tenantId: string,
  input: McpRuntimeRequest,
): Readonly<{ operation: string; sha256: string }> {
  text(tenantId, MCP_RUNTIME_LIMITS.tenant);
  const keys = [
    "kind",
    "requestId",
    "connectorId",
    "connectionId",
    "serverId",
    "toolSelector",
    "expectedDeclarationDigest",
  ];
  if (input?.kind === "resolve") closed(input, keys);
  else if (input?.kind === "acquire")
    closed(input, [...keys, "resolutionRef", "use"]);
  else invalid();
  const target = {
    requestId: text(input.requestId),
    connectorId: mcpConnectorId(text(input.connectorId)).value,
    connectionId: mcpConnectionId(text(input.connectionId)).value,
    serverId: mcpServerId(text(input.serverId)).value,
    toolSelector: text(input.toolSelector, MCP_RUNTIME_LIMITS.selector),
    expectedDeclarationDigest: text(input.expectedDeclarationDigest, 64),
  };
  if (!/^[0-9a-f]{64}$/u.test(target.expectedDeclarationDigest)) invalid();
  if (input.kind === "resolve")
    return {
      operation: MCP_RUNTIME_OPERATIONS.resolve,
      sha256: requestBinding(MCP_RUNTIME_METHODS.resolve, tenantId, target),
    };
  const resolutionRefSha256 = hash(
    text(input.resolutionRef, MCP_RUNTIME_LIMITS.resolutionRef),
  );
  const use = input.use;
  if (use?.kind === "discovery") {
    closed(use, ["kind"]);
    return {
      operation: MCP_RUNTIME_OPERATIONS.acquire,
      sha256: requestBinding(MCP_RUNTIME_METHODS.acquire, tenantId, {
        ...target,
        resolutionRefSha256,
        credentialUse: { kind: "discovery" },
      }),
    };
  }
  if (use?.kind !== "invocation") invalid();
  closed(
    use,
    [
      "kind",
      "invocationGrant",
      "auditRef",
      "typedArgumentsJson",
      "idempotencyKey",
    ],
    ["approvalRef"],
  );
  if (
    !(use.typedArgumentsJson instanceof Uint8Array) ||
    use.typedArgumentsJson.byteLength === 0 ||
    use.typedArgumentsJson.byteLength > MCP_RUNTIME_LIMITS.typedArgumentsBytes
  )
    invalid();
  return {
    operation: MCP_RUNTIME_OPERATIONS.acquire,
    sha256: requestBinding(MCP_RUNTIME_METHODS.acquire, tenantId, {
      ...target,
      resolutionRefSha256,
      credentialUse: {
        kind: "invocation",
        invocationGrantSha256: hash(
          mcpInvocationGrant(text(use.invocationGrant)).value,
        ),
        auditRef: text(use.auditRef),
        typedArgumentsSha256: hash(use.typedArgumentsJson),
        approvalRef: presence(
          use.approvalRef === undefined ? undefined : text(use.approvalRef),
        ),
        idempotencyKey: text(use.idempotencyKey),
      },
    }),
  };
}

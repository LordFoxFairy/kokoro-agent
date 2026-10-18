/** Decoded protobuf -> canonical business input; no caller identity or execution. */
import type { DescMessage, Message } from "@bufbuild/protobuf";
import { InvalidRequestError } from "../../../application/errors.js";
import {
  mcpConnectorIdValue,
  mcpConnectionIdValue,
  mcpServerIdValue,
  mcpInvocationGrantValue,
} from "../../../rpc/platform-identities.js";
import {
  McpConnectorIdSchema,
  McpConnectionIdSchema,
  McpServerIdSchema,
  McpInvocationGrantSchema,
} from "../../../generated/proto/kokoro/platform/v1/platform_runtime_pb.js";
import {
  ResolveMcpRuntimeConnectionRequestSchema,
  AcquireMcpRuntimeCredentialRequestSchema,
  McpRuntimeDiscoveryCredentialUseSchema,
  McpRuntimeInvocationCredentialUseSchema,
  type ResolveMcpRuntimeConnectionRequest,
  type AcquireMcpRuntimeCredentialRequest,
} from "../../../generated/proto/kokoro/platform/v1/mcp_runtime_pb.js";
import type { McpRuntimeRequest } from "./mcp-runtime-request-binding.js";

function known(message: Message | undefined, descriptor: DescMessage): void {
  if (
    !message ||
    message.$typeName !== descriptor.typeName ||
    message.$unknown?.length
  )
    throw new InvalidRequestError("MCP runtime wire input is invalid");
  const fields = new Set([
    "$typeName",
    "$unknown",
    ...descriptor.fields.map(
      (field) => field.oneof?.localName ?? field.localName,
    ),
  ]);
  if (
    Reflect.ownKeys(message).some(
      (key) => typeof key !== "string" || !fields.has(key),
    )
  )
    throw new InvalidRequestError("MCP runtime wire input is invalid");
}

export function mcpRuntimeWireRequest(
  request:
    | ResolveMcpRuntimeConnectionRequest
    | AcquireMcpRuntimeCredentialRequest,
): McpRuntimeRequest {
  known(
    request,
    request.$typeName === ResolveMcpRuntimeConnectionRequestSchema.typeName
      ? ResolveMcpRuntimeConnectionRequestSchema
      : AcquireMcpRuntimeCredentialRequestSchema,
  );
  known(request.connectorId, McpConnectorIdSchema);
  known(request.connectionId, McpConnectionIdSchema);
  known(request.serverId, McpServerIdSchema);
  const target = {
    requestId: request.requestId,
    connectorId: mcpConnectorIdValue(request.connectorId),
    connectionId: mcpConnectionIdValue(request.connectionId),
    serverId: mcpServerIdValue(request.serverId),
    toolSelector: request.toolSelector,
    expectedDeclarationDigest: request.expectedDeclarationDigest,
  };
  if (request.$typeName === ResolveMcpRuntimeConnectionRequestSchema.typeName)
    return { ...target, kind: "resolve" };
  const use = request.credentialUse;
  if (use.case === "discovery") {
    known(use.value, McpRuntimeDiscoveryCredentialUseSchema);
    return {
      ...target,
      kind: "acquire",
      resolutionRef: request.resolutionRef,
      use: { kind: "discovery" },
    };
  }
  if (use.case !== "invocation")
    throw new InvalidRequestError("MCP runtime credential purpose is required");
  known(use.value, McpRuntimeInvocationCredentialUseSchema);
  known(use.value.invocationGrant, McpInvocationGrantSchema);
  return {
    ...target,
    kind: "acquire",
    resolutionRef: request.resolutionRef,
    use: {
      kind: "invocation",
      invocationGrant: mcpInvocationGrantValue(use.value.invocationGrant),
      auditRef: use.value.auditRef,
      typedArgumentsJson: Uint8Array.from(use.value.typedArgumentsJson),
      ...(use.value.approvalRef === undefined
        ? {}
        : { approvalRef: use.value.approvalRef }),
      idempotencyKey: use.value.idempotencyKey,
    },
  };
}

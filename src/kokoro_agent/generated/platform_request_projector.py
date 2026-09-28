# Generated from the pinned Platform execution-operation artifact. DO NOT EDIT.
from __future__ import annotations

from kokoro_agent.execution.platform_request_binding_values import (
    PlatformRequestBindingError,
    ProjectedPlatformRequest,
    project_array,
    project_boolean,
    project_command,
    project_identity,
    project_optional_boolean,
    project_optional_string,
    project_owner_scope,
    project_page,
    project_safe_integer,
    project_set,
    project_string,
    project_typed_arguments_digest,
)
from kokoro_agent.generated.kokoro.platform.v1 import platform_runtime_pb as p

FQ_METHOD_BY_OPERATION = {
    'skill.discover_visible_skills': 'kokoro.platform.v1.SkillSourceService/DiscoverVisibleSkills',
    'skill.resolve_visible_skill': 'kokoro.platform.v1.SkillSourceService/ResolveVisibleSkill',
    'skill.get_approved_package_reference': 'kokoro.platform.v1.SkillSourceService/GetApprovedSkillPackageReference',
    'skill.install': 'kokoro.platform.v1.SkillInstallationService/InstallSkill',
    'skill.set_installation_enabled': 'kokoro.platform.v1.SkillInstallationService/SetSkillInstallationEnabled',
    'skill.remove_installation': 'kokoro.platform.v1.SkillInstallationService/RemoveSkillInstallation',
    'skill.get_installation': 'kokoro.platform.v1.SkillInstallationService/GetSkillInstallation',
    'skill.list_installations': 'kokoro.platform.v1.SkillInstallationService/ListSkillInstallations',
    'mcp.list_connector_providers': 'kokoro.platform.v1.McpConnectorProviderService/ListMcpConnectorProviders',
    'mcp.create_connector': 'kokoro.platform.v1.McpConnectorService/CreateMcpConnector',
    'mcp.begin_connector_authorization': 'kokoro.platform.v1.McpConnectorService/BeginMcpConnectorAuthorization',
    'mcp.complete_connector_authorization': 'kokoro.platform.v1.McpConnectorService/CompleteMcpConnectorAuthorization',
    'mcp.list_connectors': 'kokoro.platform.v1.McpConnectorService/ListMcpConnectors',
    'mcp.get_connector': 'kokoro.platform.v1.McpConnectorService/GetMcpConnector',
    'mcp.revoke_connector': 'kokoro.platform.v1.McpConnectorService/RevokeMcpConnector',
    'mcp.get_server': 'kokoro.platform.v1.McpServerService/GetMcpServer',
    'mcp.list_servers': 'kokoro.platform.v1.McpServerService/ListMcpServers',
    'mcp.create_connection': 'kokoro.platform.v1.McpConnectionService/CreateMcpConnection',
    'mcp.list_server_declarations': 'kokoro.platform.v1.McpConnectionService/ListMcpServerDeclarations',
    'mcp.get_connection': 'kokoro.platform.v1.McpConnectionService/GetMcpConnection',
    'mcp.list_connections': 'kokoro.platform.v1.McpConnectionService/ListMcpConnections',
    'mcp.revoke_connection': 'kokoro.platform.v1.McpConnectionService/RevokeMcpConnection',
    'mcp.list_connector_capabilities': 'kokoro.platform.v1.McpAuthorizationService/ListMcpConnectorCapabilities',
    'mcp.authorize_tool': 'kokoro.platform.v1.McpAuthorizationService/AuthorizeMcpTool',
}
OPERATION_BY_FQ_METHOD = {
    method: operation for operation, method in FQ_METHOD_BY_OPERATION.items()
}


def project_request(request: object) -> ProjectedPlatformRequest:
    if type(request) is p.DiscoverVisibleSkillsRequest:
        projected = {
            'query': project_string(request.query, label='query'),
            'tags': project_array(request.tags, label='tags'),
            'scope_kind': project_safe_integer(request.scope_kind, label='scope_kind'),
            'page': project_page(request.page if request.has_field('page') else None, label='page'),
        }
        return ProjectedPlatformRequest(
            operation='skill.discover_visible_skills',
            fq_method='kokoro.platform.v1.SkillSourceService/DiscoverVisibleSkills',
            request_id=request.request_id,
            request=projected,
        )
    elif type(request) is p.ResolveVisibleSkillRequest:
        projected = {
            'source_ref': project_identity(request.source_ref if request.has_field('source_ref') else None, wrapper='SkillSourceRef', label='source_ref'),
        }
        return ProjectedPlatformRequest(
            operation='skill.resolve_visible_skill',
            fq_method='kokoro.platform.v1.SkillSourceService/ResolveVisibleSkill',
            request_id=request.request_id,
            request=projected,
        )
    elif type(request) is p.GetApprovedSkillPackageReferenceRequest:
        projected = {
            'source_ref': project_identity(request.source_ref if request.has_field('source_ref') else None, wrapper='SkillSourceRef', label='source_ref'),
        }
        return ProjectedPlatformRequest(
            operation='skill.get_approved_package_reference',
            fq_method='kokoro.platform.v1.SkillSourceService/GetApprovedSkillPackageReference',
            request_id=request.request_id,
            request=projected,
        )
    elif type(request) is p.InstallSkillRequest:
        projected = {
            'command': project_command(request.command if request.has_field('command') else None, label='command'),
            'source_ref': project_identity(request.source_ref if request.has_field('source_ref') else None, wrapper='SkillSourceRef', label='source_ref'),
            'target_owner_scope': project_owner_scope(request.target_owner_scope if request.has_field('target_owner_scope') else None, label='target_owner_scope'),
        }
        return ProjectedPlatformRequest(
            operation='skill.install',
            fq_method='kokoro.platform.v1.SkillInstallationService/InstallSkill',
            request_id=request.request_id,
            request=projected,
        )
    elif type(request) is p.SetSkillInstallationEnabledRequest:
        projected = {
            'command': project_command(request.command if request.has_field('command') else None, label='command'),
            'installation_id': project_identity(request.installation_id if request.has_field('installation_id') else None, wrapper='SkillInstallationId', label='installation_id'),
            'enabled': project_boolean(request.enabled, label='enabled'),
        }
        return ProjectedPlatformRequest(
            operation='skill.set_installation_enabled',
            fq_method='kokoro.platform.v1.SkillInstallationService/SetSkillInstallationEnabled',
            request_id=request.request_id,
            request=projected,
        )
    elif type(request) is p.RemoveSkillInstallationRequest:
        projected = {
            'command': project_command(request.command if request.has_field('command') else None, label='command'),
            'installation_id': project_identity(request.installation_id if request.has_field('installation_id') else None, wrapper='SkillInstallationId', label='installation_id'),
        }
        return ProjectedPlatformRequest(
            operation='skill.remove_installation',
            fq_method='kokoro.platform.v1.SkillInstallationService/RemoveSkillInstallation',
            request_id=request.request_id,
            request=projected,
        )
    elif type(request) is p.GetSkillInstallationRequest:
        projected = {
            'installation_id': project_identity(request.installation_id if request.has_field('installation_id') else None, wrapper='SkillInstallationId', label='installation_id'),
        }
        return ProjectedPlatformRequest(
            operation='skill.get_installation',
            fq_method='kokoro.platform.v1.SkillInstallationService/GetSkillInstallation',
            request_id=request.request_id,
            request=projected,
        )
    elif type(request) is p.ListSkillInstallationsRequest:
        projected = {
            'target_owner_scope': project_owner_scope(request.target_owner_scope if request.has_field('target_owner_scope') else None, label='target_owner_scope'),
            'enabled': project_optional_boolean(request.enabled if request.has_field('enabled') else None, label='enabled'),
            'installed': project_optional_boolean(request.installed if request.has_field('installed') else None, label='installed'),
            'page': project_page(request.page if request.has_field('page') else None, label='page'),
        }
        return ProjectedPlatformRequest(
            operation='skill.list_installations',
            fq_method='kokoro.platform.v1.SkillInstallationService/ListSkillInstallations',
            request_id=request.request_id,
            request=projected,
        )
    elif type(request) is p.ListMcpConnectorProvidersRequest:
        projected = {
            'page': project_page(request.page if request.has_field('page') else None, label='page'),
        }
        return ProjectedPlatformRequest(
            operation='mcp.list_connector_providers',
            fq_method='kokoro.platform.v1.McpConnectorProviderService/ListMcpConnectorProviders',
            request_id=request.request_id,
            request=projected,
        )
    elif type(request) is p.CreateMcpConnectorRequest:
        projected = {
            'command': project_command(request.command if request.has_field('command') else None, label='command'),
            'owner_scope': project_owner_scope(request.owner_scope if request.has_field('owner_scope') else None, label='owner_scope'),
            'connector_type': project_safe_integer(request.connector_type, label='connector_type'),
            'provider_key': project_string(request.provider_key, label='provider_key'),
            'requested_scopes': project_set(request.requested_scopes, label='requested_scopes'),
        }
        return ProjectedPlatformRequest(
            operation='mcp.create_connector',
            fq_method='kokoro.platform.v1.McpConnectorService/CreateMcpConnector',
            request_id=request.request_id,
            request=projected,
        )
    elif type(request) is p.BeginMcpConnectorAuthorizationRequest:
        projected = {
            'command': project_command(request.command if request.has_field('command') else None, label='command'),
            'connector_id': project_identity(request.connector_id if request.has_field('connector_id') else None, wrapper='McpConnectorId', label='connector_id'),
        }
        return ProjectedPlatformRequest(
            operation='mcp.begin_connector_authorization',
            fq_method='kokoro.platform.v1.McpConnectorService/BeginMcpConnectorAuthorization',
            request_id=request.request_id,
            request=projected,
        )
    elif type(request) is p.CompleteMcpConnectorAuthorizationRequest:
        projected = {
            'command': project_command(request.command if request.has_field('command') else None, label='command'),
            'connector_id': project_identity(request.connector_id if request.has_field('connector_id') else None, wrapper='McpConnectorId', label='connector_id'),
            'authorization_id': project_identity(request.authorization_id if request.has_field('authorization_id') else None, wrapper='McpAuthorizationId', label='authorization_id'),
            'authorization_handle_ref': project_string(request.authorization_handle_ref, label='authorization_handle_ref'),
        }
        return ProjectedPlatformRequest(
            operation='mcp.complete_connector_authorization',
            fq_method='kokoro.platform.v1.McpConnectorService/CompleteMcpConnectorAuthorization',
            request_id=request.request_id,
            request=projected,
        )
    elif type(request) is p.ListMcpConnectorsRequest:
        projected = {
            'owner_scope': project_owner_scope(request.owner_scope if request.has_field('owner_scope') else None, label='owner_scope'),
            'status_filter': project_safe_integer(request.status_filter, label='status_filter'),
            'page': project_page(request.page if request.has_field('page') else None, label='page'),
        }
        return ProjectedPlatformRequest(
            operation='mcp.list_connectors',
            fq_method='kokoro.platform.v1.McpConnectorService/ListMcpConnectors',
            request_id=request.request_id,
            request=projected,
        )
    elif type(request) is p.GetMcpConnectorRequest:
        projected = {
            'connector_id': project_identity(request.connector_id if request.has_field('connector_id') else None, wrapper='McpConnectorId', label='connector_id'),
        }
        return ProjectedPlatformRequest(
            operation='mcp.get_connector',
            fq_method='kokoro.platform.v1.McpConnectorService/GetMcpConnector',
            request_id=request.request_id,
            request=projected,
        )
    elif type(request) is p.RevokeMcpConnectorRequest:
        projected = {
            'command': project_command(request.command if request.has_field('command') else None, label='command'),
            'connector_id': project_identity(request.connector_id if request.has_field('connector_id') else None, wrapper='McpConnectorId', label='connector_id'),
            'reason': project_string(request.reason, label='reason'),
        }
        return ProjectedPlatformRequest(
            operation='mcp.revoke_connector',
            fq_method='kokoro.platform.v1.McpConnectorService/RevokeMcpConnector',
            request_id=request.request_id,
            request=projected,
        )
    elif type(request) is p.GetMcpServerRequest:
        projected = {
            'server_id': project_identity(request.server_id if request.has_field('server_id') else None, wrapper='McpServerId', label='server_id'),
        }
        return ProjectedPlatformRequest(
            operation='mcp.get_server',
            fq_method='kokoro.platform.v1.McpServerService/GetMcpServer',
            request_id=request.request_id,
            request=projected,
        )
    elif type(request) is p.ListMcpServersRequest:
        projected = {
            'provider_key': project_optional_string(request.provider_key if request.has_field('provider_key') else None, label='provider_key'),
            'page': project_page(request.page if request.has_field('page') else None, label='page'),
        }
        return ProjectedPlatformRequest(
            operation='mcp.list_servers',
            fq_method='kokoro.platform.v1.McpServerService/ListMcpServers',
            request_id=request.request_id,
            request=projected,
        )
    elif type(request) is p.CreateMcpConnectionRequest:
        projected = {
            'command': project_command(request.command if request.has_field('command') else None, label='command'),
            'connector_id': project_identity(request.connector_id if request.has_field('connector_id') else None, wrapper='McpConnectorId', label='connector_id'),
            'server_id': project_identity(request.server_id if request.has_field('server_id') else None, wrapper='McpServerId', label='server_id'),
            'allowed_selectors': project_set(request.allowed_selectors, label='allowed_selectors'),
        }
        return ProjectedPlatformRequest(
            operation='mcp.create_connection',
            fq_method='kokoro.platform.v1.McpConnectionService/CreateMcpConnection',
            request_id=request.request_id,
            request=projected,
        )
    elif type(request) is p.ListMcpServerDeclarationsRequest:
        projected = {
            'connector_id': project_identity(request.connector_id if request.has_field('connector_id') else None, wrapper='McpConnectorId', label='connector_id'),
            'page': project_page(request.page if request.has_field('page') else None, label='page'),
        }
        return ProjectedPlatformRequest(
            operation='mcp.list_server_declarations',
            fq_method='kokoro.platform.v1.McpConnectionService/ListMcpServerDeclarations',
            request_id=request.request_id,
            request=projected,
        )
    elif type(request) is p.GetMcpConnectionRequest:
        projected = {
            'connection_id': project_identity(request.connection_id if request.has_field('connection_id') else None, wrapper='McpConnectionId', label='connection_id'),
        }
        return ProjectedPlatformRequest(
            operation='mcp.get_connection',
            fq_method='kokoro.platform.v1.McpConnectionService/GetMcpConnection',
            request_id=request.request_id,
            request=projected,
        )
    elif type(request) is p.ListMcpConnectionsRequest:
        projected = {
            'connector_id': project_identity(request.connector_id if request.has_field('connector_id') else None, wrapper='McpConnectorId', label='connector_id'),
            'page': project_page(request.page if request.has_field('page') else None, label='page'),
        }
        return ProjectedPlatformRequest(
            operation='mcp.list_connections',
            fq_method='kokoro.platform.v1.McpConnectionService/ListMcpConnections',
            request_id=request.request_id,
            request=projected,
        )
    elif type(request) is p.RevokeMcpConnectionRequest:
        projected = {
            'command': project_command(request.command if request.has_field('command') else None, label='command'),
            'connection_id': project_identity(request.connection_id if request.has_field('connection_id') else None, wrapper='McpConnectionId', label='connection_id'),
            'reason': project_string(request.reason, label='reason'),
        }
        return ProjectedPlatformRequest(
            operation='mcp.revoke_connection',
            fq_method='kokoro.platform.v1.McpConnectionService/RevokeMcpConnection',
            request_id=request.request_id,
            request=projected,
        )
    elif type(request) is p.ListMcpConnectorCapabilitiesRequest:
        projected = {
            'connector_id': project_identity(request.connector_id if request.has_field('connector_id') else None, wrapper='McpConnectorId', label='connector_id'),
        }
        return ProjectedPlatformRequest(
            operation='mcp.list_connector_capabilities',
            fq_method='kokoro.platform.v1.McpAuthorizationService/ListMcpConnectorCapabilities',
            request_id=request.request_id,
            request=projected,
        )
    elif type(request) is p.AuthorizeMcpToolRequest:
        projected = {
            'connector_id': project_identity(request.connector_id if request.has_field('connector_id') else None, wrapper='McpConnectorId', label='connector_id'),
            'tool_selector': project_string(request.tool_selector, label='tool_selector'),
            'typed_arguments_sha256': project_typed_arguments_digest(request.typed_arguments_json, label='typed_arguments_json'),
            'approval_ref': project_optional_string(request.approval_ref if request.has_field('approval_ref') else None, label='approval_ref'),
            'idempotency_key': project_string(request.idempotency_key, label='idempotency_key'),
        }
        return ProjectedPlatformRequest(
            operation='mcp.authorize_tool',
            fq_method='kokoro.platform.v1.McpAuthorizationService/AuthorizeMcpTool',
            request_id=request.request_id,
            request=projected,
        )
    else:
        raise PlatformRequestBindingError(
            "request is not one of the 24 tenant-execution Platform messages"
        )

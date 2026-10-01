"""Reviewed production resource manifest, not a request-time import scanner.

Groups mark behavior boundaries; declarations/clients are supplied by their owners.
Every resource is explicit and read from its installed distribution by the P1 codec.
"""

from __future__ import annotations

from dataclasses import dataclass
from importlib import metadata

from kokoro_agent.execution.runtime_profile import (
    JsonValue,
    ToolImplementationSource,
    implementation_descriptor,
)


@dataclass(frozen=True, slots=True, kw_only=True)
class SourceManifest:
    sources: tuple[ToolImplementationSource, ...]
    dynamic_edges: tuple[tuple[str, str, str], ...]
    versions: tuple[tuple[str, str], ...]

    def __post_init__(self) -> None:
        if len({s.source_id for s in self.sources}) != len(self.sources):
            raise ValueError("duplicate source registration")
        if len({name for name, _ in self.versions}) != len(self.versions):
            raise ValueError("duplicate distribution registration")

    def descriptor(self, source_id: str) -> dict[str, JsonValue]:
        for distribution, expected in self.versions:
            if metadata.version(distribution) != expected:
                raise ValueError("unapproved implementation distribution version")
        return implementation_descriptor(source_id, self.sources)


# These are reviewed finite resource lists, not glob patterns. Optional executable
# implementations are selected by the same plan that supplies the actual build.
_LOCAL_GROUPS = (
    (
        "assembly",
        """
__init__.py agent_factory.py policy.py agents/definition.py agents/subagent_catalog.py
agents/subagents.py agents/native_profile.py features/definition.py
execution/runtime_profile.py execution/runtime_profile_plan.py execution/runtime_profile_sources.py
execution/protocols.py tools/toolset.py tools/toolbox.py tools/registry.py
model/factory.py worker/dependencies.py worker/main.py
""",
    ),
    (
        "guards",
        """
tools/guards.py tools/middleware.py tools/permissions.py hitl/__init__.py
hitl/input.py hitl/request.py hitl/presets.py domain/run/models.py
domain/run/repository.py domain/run/repositories.py domain/run/scope.py
protocol/__init__.py protocol/control.py protocol/events.py protocol/streams.py protocol/run_failure_generated.py
""",
    ),
    (
        "skills",
        """
skills/backend.py skills/middleware.py skills/package.py clients/skills.py
clients/skill_package_transport.py clients/platform_credentials.py clients/platform_tokens.py
clients/platform_transport.py execution/execution_proof_keys.py execution/execution_proof_profile.py
execution/execution_proof_signer.py execution/execution_proof_supplier.py
execution/platform_request_binding.py execution/platform_request_binding_values.py
""",
    ),
    ("core", "tools/ask_user_question.py"),
    ("memory", "tools/memory.py"),
    ("web_fetch", "tools/web_fetch.py"),
    ("web_search", "tools/web_search.py"),
    ("mcp", "mcp/tools.py mcp/config.py mcp/servers.py mcp/egress.py clients/mcp.py"),
    (
        "delivery",
        "tools/deliver.py clients/storage.py clients/storage_delivery.py clients/storage_transport.py",
    ),
    ("backend", "sandbox/__init__.py sandbox/backend.py sandbox/workspace.py"),
    ("local_shell", "sandbox/archive.py"),
    ("docker", "sandbox/docker_backend.py sandbox/archive.py"),
    ("e2b", "sandbox/e2b_backend.py sandbox/archive.py"),
    ("custom", "sandbox/custom_backend.py"),
    ("swarm", "swarm.py"),
    ("web_researcher", "prompts/__init__.py prompts/web-researcher.md"),
    ("general_prompt", "prompts/__init__.py prompts/general.md"),
    (
        "composition",
        """
agents/__init__.py
application/__init__.py
application/chat/__init__.py
application/chat/dto.py
application/chat/mappers.py
application/schema.py
clients/__init__.py
clients/system.py
config.py
config_file.py
domain/__init__.py
domain/chat/__init__.py
domain/chat/models.py
domain/chat/projection.py
domain/chat/repositories.py
domain/chat/time.py
domain/run/__init__.py
execution/__init__.py
execution/approvals.py
execution/events.py
execution/failures.py
execution/publish_agent_events.py
execution/run_agent.py
generated/__init__.py
generated/kokoro/__init__.py
generated/kokoro/common/__init__.py
generated/kokoro/common/v1/__init__.py
generated/kokoro/common/v1/common_pb.py
generated/kokoro/platform/__init__.py
generated/kokoro/platform/v1/__init__.py
generated/kokoro/platform/v1/platform_runtime_connect.py
generated/kokoro/platform/v1/platform_runtime_pb.py
generated/kokoro/storage/__init__.py
generated/kokoro/storage/v2/__init__.py
generated/kokoro/storage/v2/storage_connect.py
generated/kokoro/storage/v2/storage_pb.py
generated/platform_request_projector.py
infrastructure/__init__.py
infrastructure/chat_mappers.py
infrastructure/checkpoints.py
infrastructure/memory_store.py
infrastructure/postgres.py
infrastructure/postgres_chat_repository.py
infrastructure/postgres_execution_proof_lease.py
infrastructure/postgres_run_admission.py
infrastructure/postgres_run_context.py
infrastructure/postgres_run_dispatch.py
infrastructure/postgres_run_effects.py
infrastructure/postgres_run_events.py
infrastructure/postgres_run_leases.py
infrastructure/postgres_run_repository.py
infrastructure/postgres_run_sandbox.py
infrastructure/schema.py
infrastructure/sql.py
mcp/__init__.py
metrics.py
model/__init__.py
observability.py
skills/__init__.py
streams/__init__.py
streams/factory.py
streams/protocol.py
streams/redis.py
tools/__init__.py
worker/__init__.py
worker/messages.py
worker/platform.py
worker/supervisor.py
worker/supervisor_context.py
worker/supervisor_control.py
worker/supervisor_execution.py
worker/supervisor_recovery.py
""",
    ),
    (
        "catalog_declarations",
        """
agents/general.py
agents/music.py
features/__init__.py
features/catalog.py
features/chat.py
features/music.py
features/music_chat.py
""",
    ),
)

_SYMBOLS = {
    "assembly": "kokoro_agent.agent_factory.AgentFactory",
    "composition": "kokoro_agent.worker.main.serve",
    "catalog_declarations": "kokoro_agent.features.catalog.FeatureCatalog",
    "guards": "kokoro_agent.tools.guards.build_guard_chains",
    "skills": "kokoro_agent.skills.backend.TypedSkillBackend",
    "core": "kokoro_agent.tools.ask_user_question.ASK_USER_TOOL",
    "memory": "kokoro_agent.tools.memory.make_memory_tools",
    "web_fetch": "kokoro_agent.tools.web_fetch.make_web_fetch_tool",
    "web_search": "kokoro_agent.tools.web_search.make_web_search_tool",
    "mcp": "kokoro_agent.mcp.tools.make_mcp_tools",
    "delivery": "kokoro_agent.tools.deliver.make_deliver_tool",
    "backend": "kokoro_agent.sandbox.backend.make_backend_for_run",
    "local_shell": "kokoro_agent.sandbox.archive.ArchivingLocalShellBackend",
    "docker": "kokoro_agent.sandbox.docker_backend.connect_docker_sandbox",
    "e2b": "kokoro_agent.sandbox.e2b_backend.connect_e2b_sandbox",
    "custom": "kokoro_agent.sandbox.custom_backend.connect_custom_sandbox",
    "swarm": "kokoro_agent.swarm.create_swarm",
    "web_researcher": "kokoro_agent.agents.subagent_catalog.BUILT_IN_SUBAGENTS",
    "general_prompt": "kokoro_agent.agents.general.GENERAL_AGENT",
}

_NATIVE_FILES = """
__init__.py
_api/__init__.py
_api/deprecation.py
_excluded_middleware.py
_messages_reducer.py
_models.py
_subagent_transformer.py
_tools.py
_version.py
backends/__init__.py
backends/composite.py
backends/context_hub.py
backends/filesystem.py
backends/langsmith.py
backends/local_shell.py
backends/protocol.py
backends/sandbox.py
backends/state.py
backends/store.py
backends/utils.py
graph.py
middleware/__init__.py
middleware/_message_eviction.py
middleware/_overflow_clip.py
middleware/_tool_exclusion.py
middleware/_utils.py
middleware/async_subagents.py
middleware/filesystem.py
middleware/memory.py
middleware/patch_tool_calls.py
middleware/permissions.py
middleware/rubric.py
middleware/skills.py
middleware/subagents.py
middleware/summarization.py
profiles/__init__.py
profiles/_builtin_profiles.py
profiles/_keys.py
profiles/harness/__init__.py
profiles/harness/_anthropic_haiku_4_5.py
profiles/harness/_anthropic_opus_4_7.py
profiles/harness/_anthropic_sonnet_4_6.py
profiles/harness/_openai_codex.py
profiles/harness/harness_profiles.py
profiles/provider/__init__.py
profiles/provider/_openai.py
profiles/provider/_openrouter.py
profiles/provider/provider_profiles.py
"""

# Dynamic boundaries are explicit, never inferred from a callable/closure. Runtime
# clients and persistence remain owner-provided capabilities, not copied objects.
_DYNAMIC_EDGES = (
    ("agent_factory.py", "deepagents.graph.create_deep_agent", "official constructor"),
    ("swarm.py", "langgraph_swarm.swarm.create_swarm", "official peer graph"),
    (
        "execution/runtime_profile_plan.py",
        "langgraph_swarm.handoff.create_handoff_tool",
        "actual unbound handoffs",
    ),
    (
        "agents/subagent_catalog.py",
        "prompts/web-researcher.md",
        "selected builtin asset",
    ),
    (
        "agents/general.py",
        "prompts/general.md",
        "registered GENERAL_AGENT identity, declared values also captured",
    ),
    (
        "sandbox/custom_backend.py",
        "custom.factory",
        "reject unless explicit approved source and nonsecret policy",
    ),
    (
        "sandbox/custom_backend.py",
        "custom.teardown",
        "same explicit source approval required",
    ),
    (
        "sandbox/e2b_backend.py",
        "e2b",
        "selected official backend; locked leaf distribution",
    ),
    (
        "profiles/_builtin_profiles.py",
        "deepagents.provider_profiles",
        "entry points: approved set empty",
    ),
    (
        "profiles/_builtin_profiles.py",
        "deepagents.harness_profiles",
        "entry points: approved set empty",
    ),
    (
        "model/factory.py",
        "langchain.chat_models.init_chat_model",
        "current authenticated route, no credential digest",
    ),
    (
        "mcp/tools.py",
        "langchain_mcp_adapters.client.MultiServerMCPClient",
        "current authorization, fixed local wrappers",
    ),
)


def production_manifest() -> SourceManifest:
    """Return source declarations; no plugin/model/client construction happens here."""
    local = tuple(
        ToolImplementationSource(
            source_id=name,
            package="kokoro_agent",
            symbol=_SYMBOLS[name],
            distribution="kokoro-agent",
            resource_paths=tuple(paths.split()),
        )
        for name, paths in _LOCAL_GROUPS
    )
    external = (
        ToolImplementationSource(
            source_id="native",
            package="deepagents",
            symbol="deepagents.graph.create_deep_agent",
            distribution="deepagents",
            resource_paths=tuple(_NATIVE_FILES.split()),
        ),
        ToolImplementationSource(
            source_id="langchain",
            package="langchain",
            symbol="langchain.agents.create_agent",
            distribution="langchain",
            resource_paths=(
                "__init__.py",
                "agents/__init__.py",
                "agents/factory.py",
                "agents/middleware/__init__.py",
                "agents/middleware/_execution.py",
                "agents/middleware/_redaction.py",
                "agents/middleware/_retry.py",
                "agents/middleware/context_editing.py",
                "agents/middleware/file_search.py",
                "agents/middleware/human_in_the_loop.py",
                "agents/middleware/model_call_limit.py",
                "agents/middleware/model_fallback.py",
                "agents/middleware/model_retry.py",
                "agents/middleware/pii.py",
                "agents/middleware/shell_tool.py",
                "agents/middleware/summarization.py",
                "agents/middleware/todo.py",
                "agents/middleware/tool_call_limit.py",
                "agents/middleware/tool_emulator.py",
                "agents/middleware/tool_retry.py",
                "agents/middleware/tool_selection.py",
                "agents/middleware/types.py",
                "agents/structured_output.py",
                "chat_models/__init__.py",
                "chat_models/base.py",
                "tools/__init__.py",
                "tools/tool_node.py",
            ),
        ),
        ToolImplementationSource(
            source_id="handoff",
            package="langgraph_swarm",
            symbol="langgraph_swarm.create_handoff_tool",
            distribution="langgraph-swarm",
            resource_paths=("__init__.py", "handoff.py", "swarm.py"),
        ),
    )
    return SourceManifest(
        sources=(*local, *external),
        dynamic_edges=_DYNAMIC_EDGES,
        versions=(
            ("annotated-types", "0.7.0"),
            ("anthropic", "0.105.2"),
            ("anyio", "4.13.0"),
            ("attrs", "26.1.0"),
            ("backoff", "2.2.1"),
            ("beautifulsoup4", "4.15.0"),
            ("boto3", "1.43.40"),
            ("botocore", "1.43.40"),
            ("bracex", "2.6"),
            ("certifi", "2026.5.20"),
            ("cffi", "2.0.0"),
            ("charset-normalizer", "3.4.7"),
            ("click", "8.4.2"),
            ("connectrpc", "0.12.1"),
            ("cryptography", "50.0.1"),
            ("deepagents", "0.6.6"),
            ("distro", "1.9.0"),
            ("dockerfile-parse", "2.0.1"),
            ("docstring-parser", "0.18.0"),
            ("e2b", "2.30.0"),
            ("filetype", "1.2.0"),
            ("google-auth", "2.53.0"),
            ("google-genai", "2.7.0"),
            ("googleapis-common-protos", "1.75.0"),
            ("h11", "0.16.0"),
            ("h2", "4.3.0"),
            ("hpack", "4.2.0"),
            ("httpcore", "1.0.9"),
            ("httpx", "0.28.1"),
            ("httpx-sse", "0.4.3"),
            ("hyperframe", "6.1.0"),
            ("idna", "3.17"),
            ("jiter", "0.15.0"),
            ("jmespath", "1.1.0"),
            ("jsonpatch", "1.33"),
            ("jsonpointer", "3.1.1"),
            ("jsonschema", "4.26.0"),
            ("jsonschema-specifications", "2025.9.1"),
            ("langchain", "1.3.2"),
            ("langchain-anthropic", "1.4.4"),
            ("langchain-core", "1.4.0"),
            ("langchain-deepseek", "1.1.0"),
            ("langchain-google-genai", "4.2.4"),
            ("langchain-mcp-adapters", "0.3.0"),
            ("langchain-openai", "1.2.2"),
            ("langchain-protocol", "0.0.16"),
            ("langfuse", "4.7.1"),
            ("langgraph", "1.2.2"),
            ("langgraph-checkpoint", "4.1.1"),
            ("langgraph-checkpoint-postgres", "3.1.2"),
            ("langgraph-prebuilt", "1.1.0"),
            ("langgraph-sdk", "0.3.15"),
            ("langgraph-swarm", "0.1.0"),
            ("langsmith", "0.8.7"),
            ("markdown-it-py", "4.2.0"),
            ("mcp", "1.28.1"),
            ("mdurl", "0.1.2"),
            ("mypy-boto3-s3", "1.43.31"),
            ("openai", "2.41.0"),
            ("opentelemetry-api", "1.42.1"),
            ("opentelemetry-exporter-otlp-proto-common", "1.42.1"),
            ("opentelemetry-exporter-otlp-proto-http", "1.42.1"),
            ("opentelemetry-proto", "1.42.1"),
            ("opentelemetry-sdk", "1.42.1"),
            ("opentelemetry-semantic-conventions", "0.63b1"),
            ("orjson", "3.11.9"),
            ("ormsgpack", "1.12.2"),
            ("packaging", "26.2"),
            ("prometheus-client", "0.25.0"),
            ("protobuf", "6.33.6"),
            ("protobuf-py", "0.3.0"),
            ("protobuf-py-ext", "0.3.0"),
            ("psycopg", "3.3.5"),
            ("psycopg-binary", "3.3.5"),
            ("psycopg-pool", "3.3.1"),
            ("pyasn1", "0.6.3"),
            ("pyasn1-modules", "0.4.2"),
            ("pycparser", "3.0"),
            ("pydantic", "2.13.4"),
            ("pydantic-core", "2.46.4"),
            ("pydantic-settings", "2.14.2"),
            ("pygments", "2.20.0"),
            ("pyjwt", "2.14.0"),
            ("pyqwest", "0.10.0"),
            ("python-dateutil", "2.9.0.post0"),
            ("python-dotenv", "1.2.2"),
            ("python-multipart", "0.0.32"),
            ("pyyaml", "6.0.3"),
            ("redis", "8.0.0"),
            ("referencing", "0.37.0"),
            ("regex", "2026.5.9"),
            ("requests", "2.34.2"),
            ("requests-toolbelt", "1.0.0"),
            ("rfc8785", "0.1.4"),
            ("rich", "15.0.0"),
            ("rpds-py", "2026.6.3"),
            ("s3transfer", "0.19.0"),
            ("six", "1.17.0"),
            ("sniffio", "1.3.1"),
            ("soupsieve", "2.8.4"),
            ("sse-starlette", "3.4.5"),
            ("starlette", "1.3.1"),
            ("tenacity", "9.1.4"),
            ("tiktoken", "0.13.0"),
            ("tqdm", "4.67.3"),
            ("typing-extensions", "4.15.0"),
            ("typing-inspection", "0.4.2"),
            ("urllib3", "2.7.0"),
            ("uuid-utils", "0.16.0"),
            ("uvicorn", "0.49.0"),
            ("wcmatch", "10.1"),
            ("websockets", "16.0"),
            ("wrapt", "1.17.3"),
            ("xxhash", "3.7.0"),
            ("zstandard", "0.25.0"),
        ),
    )

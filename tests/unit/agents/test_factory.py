"""AgentFactory 在无外部 owner client 时仍构造真实 DeepAgents Agent。"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterable, Mapping, Sequence
from typing import Any

from langchain_core.callbacks import CallbackManagerForLLMRun
from langchain_core.outputs import ChatResult
from kokoro_agent.clients.skills import ResolvedSkill, PlatformSkillClient
from kokoro_agent.domain.run.models import LeasedRun

import pytest
from langchain_core.messages import AIMessage, BaseMessage, HumanMessage, ToolMessage
from langchain_core.language_models import BaseChatModel
from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.stream import CustomTransformer
from langgraph.store.memory import InMemoryStore

import kokoro_agent.agent_factory as agent_factory_module
from kokoro_agent.agent_factory import AgentFactory
from kokoro_agent.features.catalog import FeatureCatalog, FEATURE_CATALOG
from kokoro_agent.agents.subagent_catalog import build_subagent_catalog
from kokoro_agent.config import AppConfig
from kokoro_agent.protocol import ExecutionIdentity, IdentityRef, RunInput, RunRequest
from kokoro_agent.model.factory import ChatModelSettings
from kokoro_agent.policy import ModelConfig
from kokoro_agent.tools.toolbox import ProcessToolbox, build_toolbox
from kokoro_agent.execution.runtime_profile_plan import RuntimeAssemblyPolicy
from kokoro_agent.execution.runtime_profile_sources import production_manifest
from kokoro_agent.worker.dependencies import WorkerClients, WorkerDependencies
from kokoro_agent.worker.platform import WorkerPlatformRuntime
from support.fakes import FakeRunRepository
from support.local_fake import LocalFakeChatModel
from kokoro_agent.clients.storage import (
    DeliveryClient,
    DeliveryReceipt,
    DeliveryRequest,
    DeliveryRecoveryRequest,
)
from kokoro_agent.clients.system import (
    ModelResolutionError,
    ModelResolver,
    ResolvedModel,
)
from kokoro_agent.clients.skills import SkillClientError
from kokoro_agent.tools.middleware import RunSupersededError


class RouteResolver:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, str | None, str | None]] = []

    async def resolve(
        self,
        *,
        tenant_id: str,
        feature_key: str,
        label: str | None,
        request_id: str | None,
    ) -> ResolvedModel:
        self.calls.append((tenant_id, feature_key, label, request_id))
        return ResolvedModel(
            model_id="model",
            provider_id="provider",
            revision_id="revision",
            revision=1,
            digest="a" * 64,
            generation="1",
            tenant_generation="1",
            provider_model_name="provider-name",
            gateway_model_name="gateway-name",
            feature_key=feature_key,
            label_key=label or "default",
        )


def _request(feature_key: str) -> RunRequest:
    return RunRequest(
        kind="run.request",
        run_id=f"run-{feature_key}",
        session_id="session",
        feature_key=feature_key,
        selected_skill_source_refs=(),
        execution_identity=ExecutionIdentity(
            tenant_ref="tenant",
            actor=IdentityRef(kind="user", opaque_ref="actor"),
            subject=IdentityRef(kind="user", opaque_ref="subject"),
            identity_assertion_ref="assertion",
        ),
        input=RunInput(message_id="message", content="hello"),
    )


def _factory(
    monkeypatch: pytest.MonkeyPatch,
    resolver: ModelResolver | None,
    catalog: FeatureCatalog = FEATURE_CATALOG,
    *,
    delivery: DeliveryClient | None = None,
    platform: WorkerPlatformRuntime | None = None,
) -> tuple[AgentFactory, FakeRunRepository]:
    # The deterministic model is a test driver, not a production configuration option.
    # Inject it at the test boundary while exercising the real AgentFactory/DeepAgents path.
    def test_model(_settings: ChatModelSettings, _model: ModelConfig) -> BaseChatModel:
        assert _model.provider == "litellm"
        assert _model.name == "gateway-name"
        return LocalFakeChatModel()

    monkeypatch.setattr(agent_factory_module, "make_chat_model", test_model)
    config = AppConfig.from_env({})
    clients = WorkerClients()
    repository = FakeRunRepository()
    return AgentFactory(
        WorkerDependencies(
            model=config.model,
            sandbox=config.sandbox,
            runtime_policy=RuntimeAssemblyPolicy.from_settings(
                run_token_budget=config.run_token_budget,
                recursion_limit=config.recursion_limit,
                model=config.model,
                sandbox=config.sandbox,
            ),
            manifest=production_manifest(),
            subagent_catalog=build_subagent_catalog(None),
            toolbox=build_toolbox(fetch_allow_private=False, search=None),
            checkpointer=InMemorySaver(),
            run_repository=repository,
            memory_store=InMemoryStore(),
            mcp_client=clients.mcp,
            delivery=delivery or clients.delivery,
            platform=platform,
            model_resolver=resolver,
        ),
        catalog,
    ), repository


@pytest.mark.parametrize("feature_key", ["chat"])
async def test_builds_native_agent_without_external_clients(
    feature_key: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    resolver = RouteResolver()
    factory, repository = _factory(monkeypatch, resolver)
    run_request = _request(feature_key)
    lease = await repository.try_claim(run_request)
    assert lease is not None
    handle = await factory.build(run_request, lease)

    assert callable(handle.runnable.astream_events)
    assert callable(handle.runnable.aget_state)
    assert "deliver" not in handle.tool_descriptions
    assert resolver.calls
    assert all(call[:3] == ("tenant", feature_key, None) for call in resolver.calls)


async def test_selected_skill_source_fails_before_any_external_construction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resolver = RouteResolver()
    factory, repository = _factory(monkeypatch, resolver)
    request = _request("chat").model_copy(
        update={"selected_skill_source_refs": ("skill:exact-revision",)}
    )
    lease = await repository.try_claim(request)
    assert lease is not None
    called: list[str] = []

    async def forbidden(*args: object, **kwargs: object) -> None:
        called.append("backend")
        raise AssertionError("backend must not be built for unreadable Skill selection")

    monkeypatch.setattr(agent_factory_module, "make_backend_for_run", forbidden)
    monkeypatch.setattr(agent_factory_module, "build_toolset", forbidden)
    with pytest.raises(SkillClientError, match="typed skill source reader unavailable"):
        await factory.build(request, lease)
    assert resolver.calls == []
    assert called == []


async def test_invokes_native_agent_with_model_resolver_without_optional_clients(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The local path exercises the actual DeepAgents loop, not just construction."""
    factory, repository = _factory(monkeypatch, RouteResolver())
    run_request = _request("chat")
    lease = await repository.try_claim(run_request)
    assert lease is not None
    handle = await factory.build(run_request, lease)

    run = await handle.runnable.astream_events(
        {"messages": [HumanMessage(content="hello")]},
        version="v3",
        config={"configurable": {"thread_id": "session"}},
        transformers=[CustomTransformer],
    )
    async with run:
        outputs = await asyncio.gather(
            _collect_messages(run.messages),
            _drain(run.tool_calls),
            _drain(run.subagents),
            _drain(run.custom),
        )
        assert await run.interrupted() is False

    assert any(
        output.startswith("本地预览：DeepAgents 活动流已接通") for output in outputs[0]
    )


async def _collect_messages(messages: AsyncIterable[object]) -> list[str]:
    outputs: list[str] = []
    async for model in messages:
        text = getattr(model, "text")
        reasoning = getattr(model, "reasoning")
        await asyncio.gather(_drain(text), _drain(reasoning))
        output_message = getattr(model, "output_message")
        if output_message is not None:
            outputs.append(str(output_message.text))
    return outputs


async def test_missing_resolver_fails_before_backend_creation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory, repository = _factory(monkeypatch, None)
    request = _request("chat")
    lease = await repository.try_claim(request)
    assert lease is not None
    with pytest.raises(ModelResolutionError, match="MODEL_RESOLVER_NOT_CONFIGURED"):
        await factory.build(request, lease)


async def test_requested_label_is_resolved_and_not_interpreted_locally(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    resolver = RouteResolver()
    factory, repository = _factory(monkeypatch, resolver)
    request = _request("chat").model_copy(
        update={"requested_model_label": "opaque-label", "request_id": "request-1"}
    )
    lease = await repository.try_claim(request)
    assert lease is not None
    with caplog.at_level("INFO", logger="kokoro_agent.agent_factory"):
        await factory.build(request, lease)
    assert resolver.calls == [("tenant", "chat", "opaque-label", "request-1")]
    record = next(
        record for record in caplog.records if record.message == "model route resolved"
    )
    assert getattr(record, "revision_id") == "revision"
    assert getattr(record, "digest") == "a" * 64


async def test_owner_failure_is_not_replaced_by_a_local_model(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class UnavailableResolver(RouteResolver):
        async def resolve(
            self,
            *,
            tenant_id: str,
            feature_key: str,
            label: str | None,
            request_id: str | None,
        ) -> ResolvedModel:
            raise ModelResolutionError("MODEL_UNAVAILABLE", retryable=True)

    factory, repository = _factory(monkeypatch, UnavailableResolver())
    request = _request("chat")
    lease = await repository.try_claim(request)
    assert lease is not None
    with pytest.raises(ModelResolutionError, match="MODEL_UNAVAILABLE"):
        await factory.build(request, lease)


async def _drain(values: AsyncIterable[object]) -> None:
    async for _ in values:
        pass


async def test_all_peers_preflight_before_any_backend_or_model(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from kokoro_agent.agents.definition import Agent
    from kokoro_agent.features.definition import Feature
    from kokoro_agent.features.catalog import FeatureCatalog
    from kokoro_agent.clients.skills import SkillClientError

    feature = Feature(
        key="peer_gate",
        agents=(
            Agent(key="first", prompt="first"),
            Agent(key="second", prompt="second"),
        ),
        entry_agent="first",
        handoffs=(("first", "second"),),
    )
    factory, repository = _factory(
        monkeypatch, RouteResolver(), FeatureCatalog((feature,))
    )
    calls: list[str] = []

    async def forbidden(*args: object, **kwargs: object) -> None:
        calls.append("backend")
        raise AssertionError("sandbox called before all peers passed")

    monkeypatch.setattr(agent_factory_module, "make_backend_for_run", forbidden)
    request = _request("peer_gate").model_copy(
        update={"selected_skill_source_refs": ("skill:required",)}
    )
    lease = await repository.try_claim(request)
    assert lease is not None
    with pytest.raises(SkillClientError):
        await factory.build(request, lease)
    assert calls == []


@pytest.mark.parametrize("feature_key", ["music", "music_chat"])
async def test_selected_features_require_external_clients(
    feature_key: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    from kokoro_agent.clients.skills import SkillClientError

    resolver = RouteResolver()
    factory, repository = _factory(monkeypatch, resolver)
    request = _request(feature_key).model_copy(
        update={"selected_skill_source_refs": ("skill:required",)}
    )
    lease = await repository.try_claim(request)
    assert lease is not None
    with pytest.raises(SkillClientError):
        await factory.build(request, lease)
    assert resolver.calls == []


class RecordingDelivery:
    """Storage boundary double; native workspace and tool loop remain real."""

    def __init__(self) -> None:
        self.requests: list[DeliveryRequest] = []

    async def recover(self, request: DeliveryRecoveryRequest) -> DeliveryReceipt | None:
        return None

    async def publish(self, request: DeliveryRequest) -> DeliveryReceipt:
        self.requests.append(request)
        return DeliveryReceipt(
            artifact_id="artifact-native",
            asset_id="asset-native",
            artifact_kind="document",
            content_sha256=request.content_sha256,
            size_bytes=len(request.content),
            mime_type=request.mime_type,
        )


async def _native_script(
    monkeypatch: pytest.MonkeyPatch,
    script: list[AIMessage],
    delivery: DeliveryClient,
    *,
    catalog: FeatureCatalog = FEATURE_CATALOG,
) -> tuple[list[BaseMessage], FakeRunRepository]:
    factory, repository = _factory(
        monkeypatch, RouteResolver(), catalog, delivery=delivery
    )

    def scripted_model(
        _settings: ChatModelSettings, _model: ModelConfig
    ) -> BaseChatModel:
        return LocalFakeChatModel.with_script(script)

    monkeypatch.setattr(agent_factory_module, "make_chat_model", scripted_model)
    request = _request("chat")
    lease = await repository.try_claim(request)
    assert lease is not None
    handle = await factory.build(request, lease)
    config: RunnableConfig = {"configurable": {"thread_id": request.session_id}}
    run = await handle.runnable.astream_events(
        {"messages": [HumanMessage(content="Create and deliver a workspace file.")]},
        version="v3",
        config=config,
        transformers=[CustomTransformer],
    )
    async with run:
        await asyncio.gather(
            _collect_messages(run.messages),
            _drain(run.tool_calls),
            _drain(run.subagents),
            _drain(run.custom),
        )
        assert await run.interrupted() is False
    state = await handle.runnable.aget_state(config)
    messages: list[BaseMessage] = state.values["messages"]
    return messages, repository


async def test_general_native_write_read_and_deliver_share_state_workspace(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import hashlib
    from kokoro_agent.tools.deliver import DeliverResult

    client = RecordingDelivery()
    messages, _repository = await _native_script(
        monkeypatch,
        [
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "write_file",
                        "id": "write-1",
                        "args": {
                            "file_path": "/report.txt",
                            "content": "native workspace bytes",
                        },
                    }
                ],
            ),
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "read_file",
                        "id": "read-1",
                        "args": {"file_path": "/report.txt"},
                    }
                ],
            ),
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "deliver",
                        "id": "deliver-1",
                        "args": {"path": "/report.txt", "title": "Native report"},
                    }
                ],
            ),
            AIMessage(content="done"),
        ],
        client,
    )
    tools = {
        message.name: message
        for message in messages
        if isinstance(message, ToolMessage)
    }
    assert len(client.requests) == 1, {
        name: message.text for name, message in tools.items()
    }
    request = client.requests[0]
    assert request.content == b"native workspace bytes"
    assert request.tool_call_id == "deliver-1"
    assert request.lease is not None
    assert request.run_id == "run-chat"
    assert request.identity == _request("chat").execution_identity
    assert "native workspace bytes" in tools["read_file"].text
    result = DeliverResult.model_validate_json(tools["deliver"].text)
    assert result.artifact_id == "artifact-native"
    assert result.content_hash == hashlib.sha256(request.content).hexdigest()


async def test_general_workspace_write_does_not_make_skill_packages_writable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = RecordingDelivery()
    messages, _repository = await _native_script(
        monkeypatch,
        [
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "write_file",
                        "id": "skill-write",
                        "args": {
                            "file_path": "/.skills/music/SKILL.md",
                            "content": "replace skill",
                        },
                    }
                ],
            ),
            AIMessage(content="done"),
        ],
        client,
    )
    writes = [
        message
        for message in messages
        if isinstance(message, ToolMessage) and message.name == "write_file"
    ]
    assert len(writes) == 1
    assert "permission" in writes[0].text.lower()
    assert client.requests == []


async def test_other_agent_native_write_is_still_denied(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from kokoro_agent.agents.definition import Agent
    from kokoro_agent.features.definition import Feature

    restricted = Agent(key="restricted", prompt="Keep the default read-only policy.")
    catalog = FeatureCatalog(
        (Feature(key="chat", agents=(restricted,), entry_agent="restricted"),)
    )
    messages, _repository = await _native_script(
        monkeypatch,
        [
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "write_file",
                        "id": "denied-write",
                        "args": {"file_path": "/report.txt", "content": "denied"},
                    }
                ],
            ),
            AIMessage(content="done"),
        ],
        RecordingDelivery(),
        catalog=catalog,
    )
    writes = [
        message
        for message in messages
        if isinstance(message, ToolMessage) and message.name == "write_file"
    ]
    assert len(writes) == 1
    assert "permission denied" in writes[0].text.lower()


@pytest.mark.parametrize("field", ["permissions", "filesystem", "agent", "backend"])
def test_run_wire_cannot_override_workspace_policy(field: str) -> None:
    from pydantic import ValidationError

    payload = _request("chat").model_dump()
    payload[field] = {"filesystem": "workspace_write"}
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        RunRequest.model_validate(payload)


@pytest.mark.parametrize("selected", [False, True])
async def test_factory_exact_refs_preflight_and_empty_zero_skill_dependencies(
    selected: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    import base64
    import json
    from pathlib import Path
    from kokoro_agent.clients.skills import PlatformSkillClient
    from kokoro_agent.clients.platform_transport import (
        PlatformRequest,
        PlatformResponse,
    )
    from kokoro_agent.generated.kokoro.platform.v1 import platform_runtime_pb as pb

    vector = json.loads(
        (
            Path(__file__).parents[3]
            / "contract/platform/v1/execution-operations/v4/vectors/zip-v1.json"
        ).read_bytes()
    )["vectors"][0]
    calls: list[str] = []

    class Sender:
        async def send(
            self, request: PlatformRequest, *, timeout_s: float = 10
        ) -> PlatformResponse:
            calls.append(type(request).__name__)
            if isinstance(request, pb.ResolveVisibleSkillRequest):
                assert (
                    request.source_ref is not None
                    and request.source_ref.value == "skill:skill-1"
                )
                return pb.ResolveVisibleSkillResponse(
                    source=pb.SkillSource(
                        source_ref=pb.SkillSourceRef(value="skill:skill-1"),
                        skill_id=pb.SkillId(value="skill-1"),
                        series_id=pb.SkillSeriesId(value="series"),
                        revision=1,
                        scope_kind=pb.SkillScopeKind.PERSONAL,
                        package_asset_ref="asset",
                        content_digest=vector["zipSha256"],
                        manifest_identity=vector["manifestIdentity"],
                    )
                )
            return pb.GetApprovedSkillPackageReferenceResponse(
                asset_ref="asset",
                content_digest=vector["zipSha256"],
                manifest_identity=vector["manifestIdentity"],
                transfer_reference=pb.PackageTransferReference(method="GET"),
            )

    class Transfer:
        async def get(
            self, reference: pb.PackageTransferReference, content_digest: str
        ) -> bytes:
            calls.append("GET")
            return base64.b64decode(vector["zipBase64"])

    class Platform(WorkerPlatformRuntime):
        def __init__(self) -> None:
            pass

        def skills_for_run(self, leased_run: LeasedRun) -> PlatformSkillClient:
            assert leased_run.request.selected_skill_source_refs == ("skill:skill-1",)
            assert leased_run.lease.generation > 0
            calls.append("for_run")
            return PlatformSkillClient(Sender(), Transfer())

    class Resolver(RouteResolver):
        async def resolve(
            self,
            *,
            tenant_id: str,
            feature_key: str,
            label: str | None,
            request_id: str | None,
        ) -> ResolvedModel:
            assert calls == (
                [
                    "for_run",
                    "ResolveVisibleSkillRequest",
                    "GetApprovedSkillPackageReferenceRequest",
                    "GET",
                ]
                if selected
                else []
            )
            return await super().resolve(
                tenant_id=tenant_id,
                feature_key=feature_key,
                label=label,
                request_id=request_id,
            )

    factory, repository = _factory(monkeypatch, Resolver(), platform=Platform())
    request = _request("chat").model_copy(
        update={"selected_skill_source_refs": ("skill:skill-1",) if selected else ()}
    )
    lease = await repository.try_claim(request)
    assert lease is not None
    handle = await factory.build(request, lease)
    assert handle.runnable is not None


class CheckpointSkillReader(PlatformSkillClient):
    """External-owner test double; native backend/loader/parser remain production."""

    def __init__(self) -> None:
        self.calls: list[str] = []
        self.failure: BaseException | None = None

    async def resolve(self, source_refs: Sequence[str]) -> tuple[ResolvedSkill, ...]:
        self.calls.append("resolve")
        return tuple(
            ResolvedSkill(
                source_ref=ref,
                skill_id=ref.removeprefix("skill:"),
                revision=1,
                asset_ref="asset",
                content_digest="a" * 64,
                manifest_identity="zip-v1:sha256:" + "b" * 64,
            )
            for ref in source_refs
        )

    async def load_package(self, skill: ResolvedSkill) -> Mapping[str, bytes]:
        self.calls.append(skill.skill_id)
        if self.failure is not None:
            raise self.failure
        return {
            "SKILL.md": (
                f"---\nname: {skill.skill_id}\ndescription: capability-{skill.skill_id}\n"
                "---\nOriginal instructions.\n"
            ).encode()
        }


class CheckpointSkillPlatform(WorkerPlatformRuntime):
    reader: CheckpointSkillReader
    runs: list[str]

    def __init__(self) -> None:
        object.__setattr__(self, "reader", CheckpointSkillReader())
        object.__setattr__(self, "runs", [])

    def skills_for_run(self, leased_run: LeasedRun) -> PlatformSkillClient:
        self.runs.append(leased_run.request.run_id)
        return self.reader


class PromptRecordingModel(LocalFakeChatModel):
    observed: list[str] = []

    def _generate(
        self,
        messages: list[BaseMessage],
        stop: list[str] | None = None,
        run_manager: CallbackManagerForLLMRun | None = None,
        **kwargs: Any,
    ) -> ChatResult:
        self.observed.append("\n".join(m.text for m in messages if m.type == "system"))
        return super()._generate(messages, stop, run_manager, **kwargs)


@pytest.mark.parametrize(
    ("first", "second"),
    [
        ((), ("alpha",)),
        (("alpha",), ("beta",)),
        (("alpha",), ()),
        (("alpha",), ("alpha",)),
    ],
)
async def test_factory_skill_metadata_is_current_run_not_session(
    monkeypatch: pytest.MonkeyPatch, first: tuple[str, ...], second: tuple[str, ...]
) -> None:
    platform = CheckpointSkillPlatform()
    factory, repository = _factory(monkeypatch, RouteResolver(), platform=platform)
    model = PromptRecordingModel()

    def make_model(*_: object) -> BaseChatModel:
        return model

    monkeypatch.setattr(agent_factory_module, "make_chat_model", make_model)
    config: RunnableConfig = {"configurable": {"thread_id": "shared-session"}}
    for index, selected in enumerate((first, second)):
        request = _request("chat").model_copy(
            update={
                "run_id": f"metadata-{index}",
                "selected_skill_source_refs": tuple(
                    f"skill:{name}" for name in selected
                ),
            }
        )
        lease = await repository.try_claim(request)
        assert lease is not None
        handle = await factory.build(request, lease)
        native: Any = handle.runnable  # Public LangGraph invocation/update boundary.
        if index:
            await native.aupdate_state(
                config, {"skills_load_errors": ["previous-run-error"]}
            )
        platform.reader.calls.clear()
        await native.ainvoke({"messages": [HumanMessage(content="next")]}, config)
        snapshot = await handle.runnable.aget_state(config)
        assert [item["name"] for item in snapshot.values["skills_metadata"]] == list(
            selected
        )
        assert snapshot.values.get("skills_load_errors", []) == []
        prompt = model.observed[-1]
        for name in ("alpha", "beta"):
            assert (f"capability-{name}" in prompt) == (name in selected)
        assert "previous-run-error" not in prompt
        assert bool(platform.reader.calls) == bool(selected)
    assert len(platform.runs) == sum(bool(value) for value in (first, second))


@pytest.mark.parametrize("terminal", [False, True])
async def test_factory_skill_metadata_survives_same_run_hitl_with_guard(
    monkeypatch: pytest.MonkeyPatch, terminal: bool
) -> None:
    from langgraph.types import Command
    from kokoro_agent.agents.definition import Agent
    from kokoro_agent.features.definition import Feature
    from kokoro_agent.policy import Permissions

    agent = Agent(
        key="skill_hitl",
        prompt="Test approved write.",
        permissions=Permissions(
            filesystem="workspace_write", approval_tools=("write_file",)
        ),
    )
    catalog = FeatureCatalog(
        (Feature(key="chat", agents=(agent,), entry_agent=agent.key),)
    )
    platform = CheckpointSkillPlatform()
    factory, repository = _factory(
        monkeypatch, RouteResolver(), catalog, platform=platform
    )
    model = PromptRecordingModel.with_script(
        [
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "write_file",
                        "id": "approved-write",
                        "args": {"file_path": "/proof.txt", "content": "approved"},
                    }
                ],
            ),
            AIMessage(content="done"),
        ]
    )
    assert isinstance(model, PromptRecordingModel)

    def make_model(*_: object) -> BaseChatModel:
        return model

    monkeypatch.setattr(agent_factory_module, "make_chat_model", make_model)
    request = _request("chat").model_copy(
        update={"selected_skill_source_refs": ("skill:alpha",)}
    )
    lease = await repository.try_claim(request)
    assert lease is not None
    handle = await factory.build(request, lease)
    native: Any = handle.runnable
    config: RunnableConfig = {"configurable": {"thread_id": "same-run-hitl"}}
    await native.ainvoke({"messages": [HumanMessage(content="write")]}, config)
    before = await handle.runnable.aget_state(config)
    assert before.interrupts
    assert [item["name"] for item in before.values["skills_metadata"]] == ["alpha"]
    calls_before = list(platform.reader.calls)
    model_calls_before = len(model.observed)
    # Worker reconstructs the production graph for the same Run on resume.
    resumed = await factory.build(request, lease)
    assert len(platform.reader.calls) > len(
        calls_before
    )  # fresh preflight authorization
    platform.reader.calls.clear()
    if terminal:
        repository.terminals.add(request.run_id)
    resume_native: Any = resumed.runnable
    command = Command(resume={"decisions": [{"type": "approve"}]})
    if terminal:
        with pytest.raises(RunSupersededError, match="lease"):
            await resume_native.ainvoke(command, config)
        assert len(model.observed) == model_calls_before
    else:
        await resume_native.ainvoke(command, config)
        after = await resumed.runnable.aget_state(config)
        assert not after.interrupts
        assert after.values["skills_metadata"] == before.values["skills_metadata"]
        assert "capability-alpha" in model.observed[-1]
        assert any(
            isinstance(message, ToolMessage) and message.name == "write_file"
            for message in after.values["messages"]
        )
    assert platform.reader.calls == []  # resume is not a new graph entry


@pytest.mark.parametrize("cancel", [False, True])
async def test_factory_checkpoint_skill_refresh_failure_never_uses_old_metadata(
    monkeypatch: pytest.MonkeyPatch, cancel: bool
) -> None:
    platform = CheckpointSkillPlatform()
    factory, repository = _factory(monkeypatch, RouteResolver(), platform=platform)
    model = PromptRecordingModel()

    def make_model(*_: object) -> BaseChatModel:
        return model

    monkeypatch.setattr(agent_factory_module, "make_chat_model", make_model)
    config: RunnableConfig = {"configurable": {"thread_id": "failed-refresh"}}
    for index, name in enumerate(("alpha", "beta")):
        request = _request("chat").model_copy(
            update={
                "run_id": f"failure-{index}",
                "selected_skill_source_refs": (f"skill:{name}",),
            }
        )
        lease = await repository.try_claim(request)
        assert lease is not None
        handle = await factory.build(request, lease)
        native: Any = handle.runnable
        if not index:
            await native.ainvoke({"messages": [HumanMessage(content="start")]}, config)
            continue
        count = len(model.observed)
        platform.reader.failure = (
            asyncio.CancelledError()
            if cancel
            else SkillClientError("current authorization revoked")
        )
        with pytest.raises(asyncio.CancelledError if cancel else SkillClientError):
            await native.ainvoke(
                {"messages": [HumanMessage(content="new run")]}, config
            )
        assert len(model.observed) == count
        assert platform.reader.calls[-1] == "beta"
        # Failed discovery does not commit partial replacement or invoke a model.
        snapshot = await handle.runnable.aget_state(config)
        assert [item["name"] for item in snapshot.values["skills_metadata"]] == [
            "alpha"
        ]
        platform.reader.failure = None
        await native.ainvoke(None, config)
        recovered = await handle.runnable.aget_state(config)
        assert [item["name"] for item in recovered.values["skills_metadata"]] == [
            "beta"
        ]
        assert "capability-beta" in model.observed[-1]
        assert "capability-alpha" not in model.observed[-1]


@pytest.mark.parametrize("asynchronous", [False, True])
async def test_skill_lifecycle_adapter_preserves_native_warnings_without_mutating_state(
    asynchronous: bool,
) -> None:
    from deepagents.backends.protocol import BackendProtocol, LsResult
    from deepagents.middleware.skills import SkillsState
    from langgraph.runtime import Runtime
    from kokoro_agent.skills.middleware import RunSkillsMiddleware

    class WarningBackend(BackendProtocol):
        def ls(self, path: str) -> LsResult:
            return LsResult(error="current-source-unavailable")

        async def als(self, path: str) -> LsResult:
            return self.ls(path)

    middleware = RunSkillsMiddleware(backend=WarningBackend(), sources=["/skills/"])
    state: SkillsState = {
        "messages": [],
        "skills_metadata": [],
        "skills_load_errors": ["old-error"],
    }
    runtime = Runtime()
    result = (
        await middleware.abefore_agent(state, runtime, {})
        if asynchronous
        else middleware.before_agent(state, runtime, {})
    )
    assert result["skills_metadata"] == []
    assert "skills_load_errors" in result
    assert result["skills_load_errors"]
    assert "current-source-unavailable" in result["skills_load_errors"][0]
    assert state == {
        "messages": [],
        "skills_metadata": [],
        "skills_load_errors": ["old-error"],
    }


def test_subagent_materialization_uses_the_same_pure_selection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from kokoro_agent.agents import subagents as module
    from kokoro_agent.agents.subagent_catalog import RegisteredSubagent, SubagentCatalog
    from kokoro_agent.tools.toolset import Toolset
    from langchain_core.tools import StructuredTool

    assert callable(getattr(module, "plan_subagents", None)), (
        "shared subagent plan missing"
    )
    planner = module.plan_subagents
    calls: list[object] = []

    def record(
        catalog: SubagentCatalog,
        available_tools: frozenset[str],
        *,
        selected: frozenset[str] | None = None,
    ) -> module.SubagentSelectionPlan:
        plan = planner(catalog, available_tools, selected=selected)
        calls.append(plan)
        return plan

    monkeypatch.setattr(module, "plan_subagents", record)
    from pydantic import BaseModel

    class NoArgs(BaseModel):
        pass

    def invoke() -> str:
        return "ok"

    tool = StructuredTool(
        func=invoke, name="available", description="available", args_schema=NoArgs
    )
    catalog = SubagentCatalog(
        (
            RegisteredSubagent(
                name="kept",
                description="kept",
                system_prompt="kept",
                source="built-in",
                tools=("available",),
            ),
            RegisteredSubagent(
                name="missing",
                description="missing",
                system_prompt="missing",
                source="built-in",
                tools=("absent",),
            ),
            RegisteredSubagent(
                name="unselected",
                description="unselected",
                system_prompt="unselected",
                source="built-in",
            ),
        )
    )
    bundle = module.build_subagent_bundle(
        Toolset.from_tools((tool,)),
        module.plan_subagents(
            catalog, frozenset({"available"}), selected=frozenset({"missing", "kept"})
        ),
        (),
    )
    assert len(calls) == 1
    plan = planner(
        catalog, frozenset({"available"}), selected=frozenset({"missing", "kept"})
    )
    assert tuple(spec.name for spec in plan.specs) == ("kept",)
    assert tuple(sub["name"] for sub in bundle.subagents) == ("general-purpose", "kept")
    assert bundle.declared == frozenset({"kept"})
    assert "tools" in bundle.subagents[1]
    assert bundle.subagents[1]["tools"] == [tool]
    assert plan.missing == (("missing", ("absent",)),)


async def test_native_factory_build_uses_both_shared_planners(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from kokoro_agent.tools import toolset as tool_module
    from kokoro_agent.agents import subagents as sub_module
    from kokoro_agent.agents.definition import Agent
    from kokoro_agent.agents.subagent_catalog import SubagentCatalog

    tool_planner = tool_module.plan_toolset
    sub_planner = sub_module.plan_subagents
    observed: list[str] = []

    def tool_plan(
        *, agent: Agent, toolbox: ProcessToolbox, delivery_available: bool
    ) -> tool_module.ToolSelectionPlan:
        observed.append("tools")
        return tool_planner(
            agent=agent, toolbox=toolbox, delivery_available=delivery_available
        )

    def sub_plan(
        catalog: SubagentCatalog,
        available_tools: frozenset[str],
        *,
        selected: frozenset[str] | None = None,
    ) -> sub_module.SubagentSelectionPlan:
        observed.append("subagents")
        return sub_planner(catalog, available_tools, selected=selected)

    monkeypatch.setattr(tool_module, "plan_toolset", tool_plan)
    monkeypatch.setattr(sub_module, "plan_subagents", sub_plan)
    resolver = RouteResolver()
    factory, repository = _factory(monkeypatch, resolver)
    request = _request("chat")
    lease = await repository.try_claim(request)
    assert lease is not None
    built = await factory.build(request, lease)
    assert built.runnable
    assert resolver.calls
    assert observed == ["tools", "subagents"]


async def test_all_peer_guard_validation_precedes_any_preflight(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from kokoro_agent.agents.definition import Agent
    from kokoro_agent.features.definition import Feature
    from kokoro_agent.policy import Permissions

    feature = Feature(
        key="bad",
        agents=(
            Agent(key="one", prompt="one"),
            Agent(
                key="two",
                prompt="two",
                permissions=Permissions(review_tools=("ask_user_question",)),
            ),
        ),
        entry_agent="one",
        handoffs=(("one", "two"),),
    )
    factory, repository = _factory(
        monkeypatch, RouteResolver(), FeatureCatalog((feature,))
    )
    calls: list[str] = []

    async def forbidden(*args: object, **kwargs: object) -> object:
        calls.append("preflight")
        raise AssertionError("guard validation must precede all preflight")

    monkeypatch.setattr(agent_factory_module, "_preflight", forbidden)
    request = _request("bad")
    lease = await repository.try_claim(request)
    assert lease is not None
    with pytest.raises(ValueError, match="result-review"):
        await factory.build(request, lease)
    assert calls == []


def test_dependencies_reject_drift_between_actual_settings_and_policy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dataclasses import replace

    factory, _ = _factory(monkeypatch, RouteResolver())
    dependencies: WorkerDependencies = getattr(factory, "_dependencies")
    with pytest.raises(ValueError, match="policy"):
        replace(
            dependencies,
            model=dependencies.model.model_copy(
                update={"disable_streaming": not dependencies.model.disable_streaming}
            ),
        )


async def test_real_factory_materializers_consume_exact_prepared_objects(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from kokoro_agent.execution.runtime_profile_plan import PreparedFeaturePlan

    observed: list[PreparedFeaturePlan] = []
    prepare = agent_factory_module.prepare_feature
    materialize = agent_factory_module.build_toolset
    bundle = agent_factory_module.build_subagent_bundle

    def prepare_record(*args: Any, **kwargs: Any) -> PreparedFeaturePlan:
        plan = prepare(*args, **kwargs)
        observed.append(plan)
        return plan

    async def tools_record(*args: Any, **kwargs: Any):
        assert kwargs["plan"] is observed[0].peers[0].tools
        return await materialize(*args, **kwargs)

    def subagents_record(*args: Any, **kwargs: Any):
        assert args[1] is observed[0].peers[0].subagents
        return bundle(*args, **kwargs)

    monkeypatch.setattr(agent_factory_module, "prepare_feature", prepare_record)
    monkeypatch.setattr(agent_factory_module, "build_toolset", tools_record)
    monkeypatch.setattr(agent_factory_module, "build_subagent_bundle", subagents_record)
    factory, repository = _factory(monkeypatch, RouteResolver())
    request = _request("chat")
    lease = await repository.try_claim(request)
    assert lease is not None
    await factory.build(request, lease)
    assert len(observed) == 1


@pytest.mark.parametrize("feature_key", ["chat", "music", "music_chat"])
async def test_static_recipe_must_commit_before_any_external_preflight(
    feature_key: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    factory, repository = _factory(monkeypatch, RouteResolver())
    run_request = _request(feature_key)
    lease = await repository.try_claim(run_request)
    assert lease is not None
    calls: list[str] = []

    async def refuse_commit(*args: object) -> None:
        calls.append("freeze")
        raise RuntimeError("recipe transaction rejected")

    async def external(*args: object) -> None:
        calls.append("external")
        raise RuntimeError("external called before recipe commit")

    monkeypatch.setattr(
        repository, "freeze_or_verify_static_recipe", refuse_commit, raising=False
    )
    monkeypatch.setattr(agent_factory_module, "_preflight", external)
    with pytest.raises(RuntimeError, match="recipe transaction rejected"):
        await factory.build(run_request, lease)
    assert calls == ["freeze"]


@pytest.mark.parametrize("mutation", ["order", "space", "escape", "default", "unknown"])
async def test_static_request_identity_is_original_text_not_json_equivalence(
    mutation: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    import json
    from kokoro_agent.domain.run.models import StaticRecipeIncompatible

    resolver = RouteResolver()
    factory, repository = _factory(monkeypatch, resolver)
    req = _request("chat")
    lease = await repository.try_claim(req)
    assert lease is not None
    raw = req.model_dump_json()
    value = json.loads(raw)
    if mutation == "order":
        raw = json.dumps(dict(reversed(tuple(value.items()))), separators=(",", ":"))
    elif mutation == "space":
        raw = raw + " "
    elif mutation == "escape":
        raw = raw.replace('"chat"', '"\\u0063hat"')
    elif mutation == "default":
        value.pop("requested_model_label")
        raw = json.dumps(value, separators=(",", ":"))
    else:
        value["unknown"] = None
        raw = json.dumps(value, separators=(",", ":"))
    repository.request_json[req.run_id] = raw
    with pytest.raises(StaticRecipeIncompatible):
        await factory.build(req, lease)
    assert resolver.calls == []
    assert repository.static_recipes == {}


async def test_static_recipe_same_run_restore_and_drift(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from kokoro_agent.domain.run.models import StaticRecipeIncompatible

    resolver = RouteResolver()
    factory, repository = _factory(monkeypatch, resolver)
    req = _request("chat")
    lease = await repository.try_claim(req)
    assert lease is not None
    await factory.build(req, lease)
    original = repository.static_recipes[req.run_id]
    assert await repository.pause(req.run_id, lease)
    restored = await repository.adopt(req.run_id, "next")
    assert restored is not None
    await factory.build(req, restored)
    assert repository.static_recipes[req.run_id] is original
    repository.static_recipes[req.run_id] = type(original)(
        canonical_bytes=original.canonical_bytes, fingerprint="0" * 64
    )
    resolver.calls.clear()
    with pytest.raises(StaticRecipeIncompatible):
        await factory.build(req, restored)
    assert not resolver.calls


@pytest.mark.parametrize("fact", ["started", "usage", "sandbox"])
async def test_executed_run_with_missing_static_binding_is_rejected(
    fact: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    from kokoro_agent.domain.run.models import StaticRecipeIncompatible

    resolver = RouteResolver()
    factory, repository = _factory(monkeypatch, resolver)
    req = _request("chat")
    lease = await repository.try_claim(req)
    assert lease is not None
    if fact == "started":
        repository.event_index_counter[req.run_id] = 1
    elif fact == "usage":
        repository.usage_totals[req.run_id] = (1, 0)
    else:
        repository.sandbox_ids[req.run_id] = "box"
    with pytest.raises(StaticRecipeIncompatible):
        await factory.build(req, lease)
    assert not resolver.calls


async def test_external_preflight_waits_for_the_successful_recipe_commit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from kokoro_agent.domain.run.models import StaticRecipeBinding
    from kokoro_agent.domain.run.repository import LeaseFence

    factory, repository = _factory(monkeypatch, RouteResolver())
    req = _request("chat")
    lease = await repository.try_claim(req)
    assert lease is not None
    entered, release = asyncio.Event(), asyncio.Event()
    events: list[str] = []
    original = repository.freeze_or_verify_static_recipe

    async def delayed_commit(
        request: RunRequest, fence: LeaseFence, binding: StaticRecipeBinding
    ) -> str:
        entered.set()
        await release.wait()
        result = await original(request, fence, binding)
        events.append("committed")
        return result

    async def preflight(*args: object) -> None:
        events.append("external")
        raise RuntimeError("stop after preflight boundary")

    monkeypatch.setattr(repository, "freeze_or_verify_static_recipe", delayed_commit)
    monkeypatch.setattr(agent_factory_module, "_preflight", preflight)
    task = asyncio.create_task(factory.build(req, lease))
    try:
        await entered.wait()
        assert not events
        release.set()
        with pytest.raises(RuntimeError, match="stop after preflight"):
            await task
        assert events == ["committed", "external"]
    finally:
        release.set()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)

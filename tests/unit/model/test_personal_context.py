"""Personal material contract, privacy and full-context capacity boundaries."""

from __future__ import annotations
from typing import Any

import json

import pytest
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_core.messages.utils import count_tokens_approximately
from pydantic import ValidationError, SecretStr

from kokoro_agent.protocol.control import RunRequest, PersonalContext
from kokoro_agent.model.personal_context import (
    append_personal_context,
    validate_personal_context_window,
    PersonalContextWindowExceeded,
)
from kokoro_agent.model.call_models import ModelCallInputInvalid
from kokoro_agent.observability import trace_config, ObservabilitySettings


def material() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "owner": {"tenant_ref": "tenant", "user_ref": "actor"},
        "profile": {
            "revision": 1,
            "nickname": "NICKNAME_PRIVATE",
            "occupation": "work",
            "about": "about",
            "instructions": "be concise",
        },
        "knowledge": {
            "usage_revision": 1,
            "enabled": True,
            "items": [
                {
                    "knowledge_id": "00000000-0000-4000-8000-000000000001",
                    "revision": 2,
                    "name": "name",
                    "when_to_use": "when",
                    "content": "KNOWLEDGE_PRIVATE",
                }
            ],
        },
    }


def request(context: dict[str, Any] | None = None) -> RunRequest:
    body: dict[str, Any] = {
        "kind": "run.request",
        "run_id": "run",
        "session_id": "session",
        "feature_key": "chat",
        "project_ref": "project",
        "selected_skill_source_refs": [],
        "execution_identity": {
            "tenant_ref": "tenant",
            "actor": {"kind": "user", "opaque_ref": "actor"},
            "subject": {"kind": "project", "opaque_ref": "project"},
            "identity_assertion_ref": "assertion",
        },
        "input": {"message_id": "message", "content": "original input"},
    }
    if context is not None:
        body["personal_context"] = context
    return RunRequest.model_validate_json(json.dumps(body))


def test_absent_optional_context_keeps_old_bytes_and_present_freezes_versions() -> None:
    old = request()
    assert "personal_context" not in old.model_dump(mode="json")
    assert old.model_copy(update={"personal_context": None}).model_dump(
        mode="json"
    ) == old.model_dump(mode="json")
    current = request(material())
    assert current.model_dump(mode="json")["personal_context"] == material()
    assert current.input.content == "original input"
    assert current.execution_identity.subject.kind == "project"


@pytest.mark.parametrize(
    "path,value",
    [
        (("owner", "tenant_ref"), "other"),
        (("owner", "user_ref"), "project"),
        (("schema_version",), True),
        (("profile", "revision"), True),
        (("knowledge", "enabled"), 1),
        (("knowledge", "usage_revision"), -1),
        (("knowledge", "enabled"), False),
        (("profile", "nickname"), "\ud800"),
        (("profile", "about"), "a" * 2001),
    ],
)
def test_strict_identity_revision_and_material_validation(
    path: tuple[str, ...], value: object
) -> None:
    context = material()
    target = context
    for segment in path[:-1]:
        target = target[segment]
    target[path[-1]] = value
    with pytest.raises(ValidationError):
        request(context)


def test_duplicates_unknown_fields_and_nonhuman_actor_rejected() -> None:
    context = material()
    context["knowledge"]["items"] *= 2
    with pytest.raises(ValidationError):
        request(context)
    context = material()
    context["unexpected"] = True
    with pytest.raises(ValidationError):
        request(context)
    current = request().model_dump(mode="json")
    current["personal_context"] = material()
    current["execution_identity"]["actor"]["kind"] = "service"
    with pytest.raises(ValidationError):
        RunRequest.model_validate_json(json.dumps(current))


def test_disabled_knowledge_and_absent_profile_are_explicit_not_dropped() -> None:
    context = material()
    context["profile"] = None
    context["knowledge"] = {"usage_revision": 0, "enabled": False, "items": []}
    assert request(context).model_dump(mode="json")["personal_context"] == context


def test_complete_render_preserves_all_material_and_original_prompt() -> None:
    context = PersonalContext.model_validate_json(json.dumps(material()))
    rendered = append_personal_context("original policy", context)
    assert rendered.startswith("original policy")
    assert (
        json.loads(
            rendered.split("<personal_context_data>\n")[1].split(
                "\n</personal_context_data>"
            )[0]
        )
        == material()
    )
    assert append_personal_context("original policy", None) == "original policy"
    assert "KNOWLEDGE_PRIVATE" not in repr(request(material()))


def test_window_uses_system_messages_tools_and_output_reservation() -> None:
    messages = [SystemMessage(content="private " * 200), HumanMessage(content="hello")]
    from langchain_core.tools import BaseTool

    tools: list[BaseTool | dict[str, Any]] = [
        {
            "type": "function",
            "function": {
                "name": "x",
                "description": "tool " * 200,
                "parameters": {"type": "object"},
            },
        }
    ]
    total = count_tokens_approximately(messages, tools=tools)
    validate_personal_context_window(
        messages, tools=tools, context_window=total + 10, max_output_tokens=10
    )
    with pytest.raises(PersonalContextWindowExceeded):
        validate_personal_context_window(
            messages, tools=tools, context_window=total + 9, max_output_tokens=10
        )
    with pytest.raises(ModelCallInputInvalid):
        validate_personal_context_window(
            messages, tools=tools, context_window=None, max_output_tokens=10
        )


def test_personal_material_disables_langfuse_creation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def forbidden():
        raise AssertionError("content callback created")

    monkeypatch.setattr("kokoro_agent.observability.CallbackHandler", forbidden)
    settings = ObservabilitySettings(
        langfuse_public_key=SecretStr("test"), langfuse_secret_key=SecretStr("test")
    )
    assert trace_config(settings, request(material())) is None


def test_counter_failure_is_explicit_and_never_guesses(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def unavailable(*args: object, **kwargs: object) -> int:
        raise RuntimeError("private counter failure")

    monkeypatch.setattr(
        "kokoro_agent.model.personal_context.count_tokens_approximately", unavailable
    )
    with pytest.raises(
        ModelCallInputInvalid, match="estimate is unavailable"
    ) as denied:
        validate_personal_context_window(
            [HumanMessage(content="private")],
            tools=[],
            context_window=4096,
            max_output_tokens=256,
        )
    assert denied.value.__cause__ is None

"""kind=input 的消费侧包装：request_input = request_human(kind="input") + schema 校验 + 重问循环。

任何工具在执行中途需要一份结构化回应时调用 `request_input`：首跑挂起（request_human），
resume 回灌的 value 靠近消费点按请求 schema 校验——不合法不炸 run，附 validation_error
原地重新 interrupt（同一 request_id，人重填），合法则返回。reject 决策映射为拒绝语义。

resume 载荷形态（supervisor 侧 submit_resume_value 产出）：list[{request_id, type, value?/reason?}]，
本层按 request_id 自取其项（同 review 的 list[decision] 回灌形态，多 pending 各取己项）。
"""

# jsonschema 无 py.typed：校验调用与 ValidationError 属性均为未解类型（上游缺口，边界隔离于本函数）。
# pyright: reportUnknownMemberType=false, reportUnknownArgumentType=false, reportUnknownVariableType=false

from __future__ import annotations

from typing import Literal, Union

import jsonschema
from pydantic import BaseModel, ConfigDict, JsonValue, TypeAdapter

from kokoro_agent.hitl.request import request_human


class InputSubmitted(BaseModel):
    """人回灌并通过 schema 校验的结构化 value。"""

    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    value: dict[str, JsonValue]


class InputRejected(BaseModel):
    """人拒绝本次请求（MCP 场景映射 decline）。"""

    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    reason: str | None = None


HumanInput = Union[InputSubmitted, InputRejected]


class _InputResumeItem(BaseModel):
    """resume 载荷单项：与 approvals.submit_resume_value 产出的 dict 同构。"""

    model_config = ConfigDict(strict=True, extra="forbid")

    request_id: str
    type: Literal["submit", "reject"]
    value: dict[str, JsonValue] | None = None
    reason: str | None = None


_INPUT_RESUME_ADAPTER: TypeAdapter[list[_InputResumeItem]] = TypeAdapter(list[_InputResumeItem])


def _pick(raw: object, request_id: str) -> _InputResumeItem:
    # resume 值是框架 Any 边界：strict 洗净为决策列表，按 request_id 自取己项；缺项 fail-loud。
    items = _INPUT_RESUME_ADAPTER.validate_python(raw)
    mine = next((item for item in items if item.request_id == request_id), None)
    if mine is None:
        raise ValueError(f"input resume missing decision for request {request_id!r}")
    return mine


def _schema_error(value: dict[str, JsonValue], schema: dict[str, JsonValue]) -> str | None:
    # jsonschema 校验靠近消费点：不合法返回错误文案（供人重填），不抛出、不炸 run。
    try:
        jsonschema.validate(value, schema)
    except jsonschema.ValidationError as exc:
        return str(exc.message)
    return None


def request_input(
    *,
    request_id: str,
    schema: dict[str, JsonValue] | None = None,
    context: dict[str, JsonValue] | None = None,
) -> HumanInput:
    """工具执行中途请求一份结构化回应：挂起 → resume 回灌 value → schema 校验 → 返回。

    submit 且通过校验 → `InputSubmitted`；reject → `InputRejected`；submit 但不合法 →
    附 `validation_error` 原地重新 interrupt（同 request_id，幂等锚不变），人重填后再校验。
    无 schema 的请求跳过校验直接采纳。同步调用（与 request_human 一致，async 工具体内无需 await）。
    """
    base = dict(context or {})
    attempt_context = dict(base)
    while True:
        raw = request_human(
            kind="input", request_id=request_id, schema=schema, context=attempt_context
        )
        item = _pick(raw, request_id)
        if item.type == "reject":
            return InputRejected(reason=item.reason)
        value = dict(item.value or {})
        error = None if schema is None else _schema_error(value, schema)
        if error is None:
            return InputSubmitted(value=value)
        # 不合法：附 validation_error 重新 interrupt（人重填），基础 context 不累积旧错。
        attempt_context = {**base, "validation_error": error}

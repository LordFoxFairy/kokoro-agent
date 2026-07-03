"""入站帧薄解析：contract 校验 + 坏帧安全丢弃（skip-and-continue）。"""

from __future__ import annotations

import logging

from pydantic import JsonValue, ValidationError

from kokoro_agent.contract import InboundMessage, inbound_adapter

LOGGER = logging.getLogger(__name__)


def parse_inbound(raw: dict[str, JsonValue]) -> InboundMessage | None:
    """解析入站帧；未知 kind 或结构非法时返回 None 并记录警告（skip-and-continue）。"""
    try:
        return inbound_adapter.validate_python(raw)
    except ValidationError as exc:
        LOGGER.warning("dropping malformed inbound message: %s", exc)
        return None

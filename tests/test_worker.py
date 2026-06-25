"""worker 入口引导：main → _serve 装配 + 空请求流即收束（此前 0%）。"""

from __future__ import annotations

from collections.abc import AsyncIterator, Mapping, Sequence
from typing import TypeVar

import pytest
from pydantic import JsonValue

from kokoro_agent.application.protocols.stream import StreamItem
from kokoro_agent.interfaces import worker

_T = TypeVar("_T")


async def _aiter(items: Sequence[_T]) -> AsyncIterator[_T]:
    for item in items:
        yield item


class _EmptyBus:
    """空请求流：subscribe 立即收束，使 supervisor.serve 的 async for 不阻塞而返回。"""

    async def publish(self, stream: str, event: Mapping[str, JsonValue]) -> StreamItem:
        return StreamItem(cursor="0", event=dict(event))

    async def read_all(self, stream: str) -> list[StreamItem]:
        return []

    def subscribe(
        self, stream: str, from_cursor: str | None = None
    ) -> AsyncIterator[StreamItem]:
        return _aiter([])


def test_main_assembles_and_returns_on_empty_stream(monkeypatch: pytest.MonkeyPatch) -> None:
    # main → asyncio.run(_serve())：memory 后端 + 空请求流 → 装配完成、serve 的 async for 即收束、不挂起。
    # load_dotenv 打桩为 no-op，免读到真实 .env 干扰；后端选 memory 免落盘。
    monkeypatch.setenv("KOKORO_CHECKPOINT_BACKEND", "memory")
    monkeypatch.setenv("KOKORO_RUN_STATE_BACKEND", "memory")
    monkeypatch.setattr(worker, "make_stream", lambda: _EmptyBus())
    monkeypatch.setattr(worker, "load_dotenv", lambda: None)
    worker.main()

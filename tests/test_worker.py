"""worker 入口引导：main → _serve 装配 + 空请求流即收束（此前 0%）。"""

from __future__ import annotations

from collections.abc import AsyncIterator, Mapping, Sequence
from pathlib import Path
from typing import TypeVar

import pytest
from pydantic import JsonValue

from kokoro_agent.streams.protocol import StreamItem
from kokoro_agent.worker import main as worker

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


def test_main_assembles_and_returns_on_empty_stream(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    # main → asyncio.run(_serve())：临时 SQLite run_state + 空请求流 → 装配完成后立即收束。
    monkeypatch.setenv("KOKORO_CHECKPOINT_BACKEND", "memory")
    monkeypatch.setenv("KOKORO_RUN_STATE_BACKEND", "sqlite")
    monkeypatch.setenv("KOKORO_RUN_STATE_DB", str(tmp_path / "run_state.db"))
    monkeypatch.setattr(worker, "make_stream", lambda: _EmptyBus())
    monkeypatch.setattr(worker, "load_dotenv", lambda: None)
    worker.main()

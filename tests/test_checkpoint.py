"""make_checkpointer 工厂：backend 选择、sqlite 跨工厂周期持久性、未知后端显式失败。"""

from __future__ import annotations

from pathlib import Path

import pytest
from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.base import CheckpointMetadata, empty_checkpoint
from langgraph.checkpoint.memory import InMemorySaver

from kokoro_agent.infrastructure.checkpoint import make_checkpointer

_CFG: RunnableConfig = {"configurable": {"thread_id": "t1", "checkpoint_ns": ""}}
_META: CheckpointMetadata = {"source": "input", "step": 1, "parents": {}}


async def test_memory_backend_yields_in_memory_saver(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KOKORO_CHECKPOINT_BACKEND", "memory")
    async with make_checkpointer() as saver:
        assert isinstance(saver, InMemorySaver)


async def test_sqlite_persists_across_factory_reentry(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    db = str(tmp_path / "ckpt.db")
    monkeypatch.setenv("KOKORO_CHECKPOINT_BACKEND", "sqlite")
    monkeypatch.setenv("KOKORO_CHECKPOINT_DB", db)
    checkpoint = empty_checkpoint()
    checkpoint["channel_values"] = {"x": 42}
    # 写：首个工厂周期（模拟 pod A / 重启前）。
    async with make_checkpointer() as saver:
        await saver.aput(_CFG, checkpoint, _META, {})
    # 读：全新工厂周期（模拟重启 / 另一 pod）从同一文件续读，证明跨进程持久。
    async with make_checkpointer() as saver:
        got = await saver.aget(_CFG)
    assert got is not None
    assert got["channel_values"] == {"x": 42}


async def test_unknown_backend_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KOKORO_CHECKPOINT_BACKEND", "bogus")
    with pytest.raises(ValueError, match="bogus"):
        async with make_checkpointer():
            pass

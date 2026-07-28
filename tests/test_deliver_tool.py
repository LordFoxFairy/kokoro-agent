"""deliver 工具规格（真件：真 tmp 工作区 + LocalPackageStore 冻结件驱动）。"""

# BaseTool.ainvoke 上游注解含未解泛型（langchain-core 边界，test_skill_tools 同款豁免）。
# pyright: reportUnknownMemberType=false

from __future__ import annotations

import asyncio
import hashlib
import json
import os
from collections.abc import Callable
from pathlib import Path
from typing import ParamSpec, TypeVar

import pytest
from pydantic import BaseModel

from kokoro_agent.skills.hub import LocalPackageStore
from kokoro_agent.tools.deliver import (
    MAX_DELIVERY_BYTES,
    DeliveryPathError,
    DeliverResult,
    delivery_ref,
    make_deliver_tool,
    read_delivery_bytes_beneath,
)

_NS = "local:s1"
_P = ParamSpec("_P")
_R = TypeVar("_R")


def _store(tmp_path: Path) -> LocalPackageStore:
    return LocalPackageStore(str(tmp_path / "deliveries"))


def _workspace(tmp_path: Path) -> Path:
    ws = tmp_path / "ws"
    ws.mkdir()
    return ws


async def test_deliver_freezes_bytes_into_content_addressed_store(tmp_path: Path) -> None:
    ws = _workspace(tmp_path)
    (ws / "report.pdf").write_bytes(b"final report")
    store = _store(tmp_path)
    tool = make_deliver_tool(ws, store, _NS)

    raw = await tool.ainvoke({"path": "/report.pdf", "title": "Report", "note": "v1"})
    result = DeliverResult.model_validate_json(raw)
    assert result.status == "delivered"
    assert result.size == len(b"final report")
    assert result.content_hash == hashlib.sha256(b"final report").hexdigest()
    assert result.mime == "application/pdf"
    assert await store.get(delivery_ref(_NS, result.content_hash)) == b"final report"


async def test_deliver_frozen_against_source_mutation(tmp_path: Path) -> None:
    # 交付后改写源文件：旧成果指向冻结副本，内容不变（异内容=异 hash=异 key）。
    ws = _workspace(tmp_path)
    (ws / "a.txt").write_bytes(b"original")
    store = _store(tmp_path)
    tool = make_deliver_tool(ws, store, _NS)

    first = DeliverResult.model_validate_json(
        await tool.ainvoke({"path": "/a.txt", "title": "A"})
    )
    (ws / "a.txt").write_bytes(b"mutated later")
    second = DeliverResult.model_validate_json(
        await tool.ainvoke({"path": "/a.txt", "title": "A"})
    )

    assert first.content_hash != second.content_hash
    assert await store.get(delivery_ref(_NS, first.content_hash)) == b"original"
    assert await store.get(delivery_ref(_NS, second.content_hash)) == b"mutated later"


async def test_deliver_same_content_is_idempotent(tmp_path: Path) -> None:
    ws = _workspace(tmp_path)
    (ws / "a.txt").write_bytes(b"same")
    store = _store(tmp_path)
    tool = make_deliver_tool(ws, store, _NS)

    one = DeliverResult.model_validate_json(await tool.ainvoke({"path": "/a.txt", "title": "A"}))
    two = DeliverResult.model_validate_json(await tool.ainvoke({"path": "/a.txt", "title": "A"}))

    assert one.content_hash == two.content_hash
    root = tmp_path / "deliveries" / "deliveries" / _NS
    assert [p.name for p in root.iterdir()] == [one.content_hash]  # 单份，无重复。


async def test_deliver_rejects_path_traversal(tmp_path: Path) -> None:
    ws = _workspace(tmp_path)
    (tmp_path / "secret.txt").write_bytes(b"secret")  # 工作区外。
    tool = make_deliver_tool(ws, _store(tmp_path), _NS)
    out = await tool.ainvoke({"path": "../secret.txt", "title": "X"})
    assert "越出工作区" in out


@pytest.mark.parametrize("relative_path", [Path(""), Path("../secret.txt"), Path("/absolute")])
def test_secure_reader_rejects_empty_parent_and_absolute_paths(
    tmp_path: Path, relative_path: Path
) -> None:
    ws = _workspace(tmp_path)

    with pytest.raises(DeliveryPathError):
        read_delivery_bytes_beneath(ws, relative_path)


async def test_deliver_rejects_preexisting_final_symlink(tmp_path: Path) -> None:
    ws = _workspace(tmp_path)
    outside = tmp_path / "outside-secret.txt"
    outside.write_bytes(b"must-never-be-delivered")
    (ws / "report.txt").symlink_to(outside)
    store = _store(tmp_path)

    out = await make_deliver_tool(ws, store, _NS).ainvoke(
        {"path": "/report.txt", "title": "Report"}
    )

    assert "error:" in out
    assert not [path for path in (tmp_path / "deliveries").rglob("*") if path.is_file()]


async def test_deliver_rejects_preexisting_parent_symlink(tmp_path: Path) -> None:
    ws = _workspace(tmp_path)
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "report.txt").write_bytes(b"must-never-be-delivered")
    (ws / "drafts").symlink_to(outside, target_is_directory=True)
    store = _store(tmp_path)

    out = await make_deliver_tool(ws, store, _NS).ainvoke(
        {"path": "/drafts/report.txt", "title": "Report"}
    )

    assert "error:" in out
    assert not [path for path in (tmp_path / "deliveries").rglob("*") if path.is_file()]


def test_secure_reader_fails_closed_without_required_flags(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ws = _workspace(tmp_path)
    (ws / "report.txt").write_bytes(b"safe")
    monkeypatch.delattr(os, "O_NOFOLLOW")

    with pytest.raises(DeliveryPathError):
        read_delivery_bytes_beneath(ws, Path("report.txt"))


def test_secure_reader_closes_root_parent_and_file_descriptors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ws = _workspace(tmp_path)
    parent = ws / "drafts"
    parent.mkdir()
    (parent / "report.txt").write_bytes(b"safe")
    real_close = os.close
    closed: list[int] = []

    def recording_close(descriptor: int) -> None:
        closed.append(descriptor)
        real_close(descriptor)

    monkeypatch.setattr(os, "close", recording_close)

    assert read_delivery_bytes_beneath(ws, Path("drafts/report.txt")) == b"safe"
    assert len(closed) == 3
    assert len(set(closed)) == 3


async def test_deliver_rejects_final_symlink_swapped_after_path_checks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ws = _workspace(tmp_path)
    target = ws / "report.txt"
    target.write_bytes(b"safe")
    outside = tmp_path / "outside-secret.txt"
    outside.write_bytes(b"must-never-be-delivered")
    store = _store(tmp_path)
    swapped = False
    real_to_thread = asyncio.to_thread

    async def swap_then_call(
        function: Callable[_P, _R], *args: _P.args, **kwargs: _P.kwargs
    ) -> _R:
        nonlocal swapped
        if not swapped:
            target.unlink()
            target.symlink_to(outside)
            swapped = True
        return await real_to_thread(function, *args, **kwargs)

    monkeypatch.setattr("kokoro_agent.tools.deliver.asyncio.to_thread", swap_then_call)
    out = await make_deliver_tool(ws, store, _NS).ainvoke(
        {"path": "/report.txt", "title": "Report"}
    )

    assert "error:" in out
    assert not [path for path in (tmp_path / "deliveries").rglob("*") if path.is_file()]


async def test_deliver_rejects_parent_symlink_swapped_after_path_checks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ws = _workspace(tmp_path)
    parent = ws / "drafts"
    parent.mkdir()
    (parent / "report.txt").write_bytes(b"safe")
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "report.txt").write_bytes(b"must-never-be-delivered")
    store = _store(tmp_path)
    swapped = False
    real_to_thread = asyncio.to_thread

    async def swap_then_call(
        function: Callable[_P, _R], *args: _P.args, **kwargs: _P.kwargs
    ) -> _R:
        nonlocal swapped
        if not swapped:
            parent.rename(ws / "drafts-before-swap")
            parent.symlink_to(outside, target_is_directory=True)
            swapped = True
        return await real_to_thread(function, *args, **kwargs)

    monkeypatch.setattr("kokoro_agent.tools.deliver.asyncio.to_thread", swap_then_call)
    out = await make_deliver_tool(ws, store, _NS).ainvoke(
        {"path": "/drafts/report.txt", "title": "Report"}
    )

    assert "error:" in out
    assert not [path for path in (tmp_path / "deliveries").rglob("*") if path.is_file()]


def test_delivery_cap_matches_the_session_reader() -> None:
    # kokoro-session src/workspace/files.ts MAX_READ_BYTES 同数：那侧超限拒读，
    # 这侧再冻结更大的成果也是死件。两侧同数才不会存得进去、下不回来。
    assert MAX_DELIVERY_BYTES == 25 * 1024 * 1024


async def test_deliver_rejects_oversized_file(tmp_path: Path) -> None:
    ws = _workspace(tmp_path)
    (ws / "huge.bin").write_bytes(b"x" * (MAX_DELIVERY_BYTES + 1))
    store = _store(tmp_path)
    tool = make_deliver_tool(ws, store, _NS)

    out = await tool.ainvoke({"path": "/huge.bin", "title": "Huge"})
    assert "超过交付上限" in out
    assert not list((tmp_path / "deliveries").rglob("*"))  # 超限件不得落进冻结存储。


async def test_deliver_caps_the_read_when_stat_under_reports(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # stat 只是廉价预筛：沙箱后台进程（如 `curl -o big.bin &`）可以在 stat 之后继续写。
    # 磁盘文件保持很小让 stat 通过，随后 open 返回远大于上限的增长后来源。
    ws = _workspace(tmp_path)
    target = ws / "grows.bin"
    target.write_bytes(b"x")

    class ObservedGrowingSource:
        def __init__(self, total_bytes: int) -> None:
            self.remaining = total_bytes
            self.read_sizes: list[int] = []

        def read(self, size: int = -1) -> bytes:
            self.read_sizes.append(size)
            consumed = self.remaining if size < 0 else min(size, self.remaining)
            self.remaining -= consumed
            return b"x" * consumed

    total_bytes = MAX_DELIVERY_BYTES * 4
    source = ObservedGrowingSource(total_bytes)

    def controlled_read(_descriptor: int, size: int) -> bytes:
        return source.read(size)

    monkeypatch.setattr("kokoro_agent.tools.deliver.os.read", controlled_read)
    tool = make_deliver_tool(ws, _store(tmp_path), _NS)

    out = await tool.ainvoke({"path": "/grows.bin", "title": "Grows"})
    assert "超过交付上限" in out
    assert source.read_sizes == [MAX_DELIVERY_BYTES + 1]
    assert source.remaining == total_bytes - (MAX_DELIVERY_BYTES + 1)


async def test_deliver_accepts_file_at_the_cap(tmp_path: Path) -> None:
    # 上限是含边界的：恰好等于上限的成果必须交付成功（封顶不得多截一字节）。
    ws = _workspace(tmp_path)
    payload = b"y" * MAX_DELIVERY_BYTES
    (ws / "edge.bin").write_bytes(payload)
    store = _store(tmp_path)
    tool = make_deliver_tool(ws, store, _NS)

    result = DeliverResult.model_validate_json(
        await tool.ainvoke({"path": "/edge.bin", "title": "Edge"})
    )
    assert result.size == MAX_DELIVERY_BYTES
    assert await store.get(delivery_ref(_NS, result.content_hash)) == payload


async def test_deliver_missing_file_returns_error(tmp_path: Path) -> None:
    tool = make_deliver_tool(_workspace(tmp_path), _store(tmp_path), _NS)
    out = await tool.ainvoke({"path": "/nope.txt", "title": "X"})
    assert "不存在" in out


async def test_deliver_without_workspace_degrades(tmp_path: Path) -> None:
    tool = make_deliver_tool(None, _store(tmp_path), _NS)
    out = await tool.ainvoke({"path": "/a.txt", "title": "X"})
    assert "不支持交付" in out


async def test_deliver_without_store_degrades(tmp_path: Path) -> None:
    tool = make_deliver_tool(_workspace(tmp_path), None, _NS)
    out = await tool.ainvoke({"path": "/a.txt", "title": "X"})
    assert "未配置交付存储" in out


def test_deliver_tool_surface_identical_regardless_of_config(tmp_path: Path) -> None:
    # D9：有无 workspace/store 两种构造，工具 name/description/schema 逐字节相同。
    def surface(workspace: Path | None, store: LocalPackageStore | None) -> tuple[str, str, str]:
        tool = make_deliver_tool(workspace, store, _NS)
        schema = tool.args_schema
        assert isinstance(schema, type) and issubclass(schema, BaseModel)
        return (tool.name, tool.description, json.dumps(schema.model_json_schema(), sort_keys=True))

    assert surface(_workspace(tmp_path), _store(tmp_path)) == surface(None, None)

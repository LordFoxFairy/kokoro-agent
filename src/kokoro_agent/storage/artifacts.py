"""产物库：字节的共享真源（backend FS 服务模型，本库服务人；session 按 id 出体）。"""

from __future__ import annotations

from pathlib import Path
from typing import Literal, Protocol

from gridfs.asynchronous import AsyncGridFSBucket
from pydantic import BaseModel, ConfigDict

from kokoro_agent.contract import Artifact
from kokoro_agent.storage.mongo import make_mongo_collection


def artifact_id_for(run_id: str, tool_call_id: str, name: str) -> str:
    # 确定性 id：HITL resume/崩溃重拾重跑工具时 put 幂等覆盖，不产孤儿副本。
    return f"{run_id}/{tool_call_id}-{name}"


class ArtifactStore(Protocol):
    async def put(self, run_id: str, tool_call_id: str, name: str, mime: str, data: bytes) -> Artifact:
        # 写入（幂等覆盖同 id）并返回 wire 引用。
        ...

    async def get(self, artifact_id: str) -> tuple[str, bytes] | None:
        # (mime, data)；未知 id 返 None。
        ...


class DirArtifactStore:
    """本地目录后端（单机开发档）：mime 存 sidecar，路径穿越即视为未知 id。"""

    def __init__(self, root: str) -> None:
        self._root = Path(root).resolve()
        self._root.mkdir(parents=True, exist_ok=True)

    def _safe_path(self, artifact_id: str) -> Path | None:
        candidate = (self._root / artifact_id).resolve()
        # 防路径穿越：resolve 后必须仍在根内（`..`/绝对路径注入 → None=未知 id）。
        return candidate if candidate.is_relative_to(self._root) else None

    async def put(self, run_id: str, tool_call_id: str, name: str, mime: str, data: bytes) -> Artifact:
        artifact_id = artifact_id_for(run_id, tool_call_id, name)
        path = self._safe_path(artifact_id)
        # 写侧更严：产物必须锁在本 run 子目录内（恶意 name 带 ../ 即 fail-loud）。
        run_root = (self._root / run_id).resolve()
        if path is None or not path.is_relative_to(run_root):
            raise ValueError(f"artifact name escapes store root: {name!r}")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        path.with_suffix(path.suffix + ".mime").write_text(mime, encoding="utf-8")
        return Artifact(artifact_id=artifact_id, name=name, mime=mime, bytes=len(data))

    async def get(self, artifact_id: str) -> tuple[str, bytes] | None:
        path = self._safe_path(artifact_id)
        if path is None or not path.is_file():
            return None
        mime_path = path.with_suffix(path.suffix + ".mime")
        if not mime_path.is_file():
            return None
        return (mime_path.read_text(encoding="utf-8"), path.read_bytes())


class GridFsArtifactStore:
    """mongo GridFS 后端（多 pod 档）：filename=artifact_id，读取最新版本（重放覆盖语义）。"""

    def __init__(self, bucket: AsyncGridFSBucket) -> None:
        self._bucket = bucket

    async def put(self, run_id: str, tool_call_id: str, name: str, mime: str, data: bytes) -> Artifact:
        artifact_id = artifact_id_for(run_id, tool_call_id, name)
        await self._bucket.upload_from_stream(artifact_id, data, metadata={"mime": mime})
        return Artifact(artifact_id=artifact_id, name=name, mime=mime, bytes=len(data))

    async def get(self, artifact_id: str) -> tuple[str, bytes] | None:
        try:
            stream = await self._bucket.open_download_stream_by_name(artifact_id)
        except Exception:  # noqa: BLE001 — NoFile 等一律视为未知 id
            return None
        data = await stream.read()
        metadata = stream.metadata or {}
        mime = metadata.get("mime")
        if not isinstance(mime, str) or not mime:
            return None
        return (mime, data)


class ArtifactSettings(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    backend: Literal["dir", "mongo"]
    dir_root: str
    mongo_url: str
    mongo_db: str


def make_artifact_store(settings: ArtifactSettings) -> ArtifactStore:
    if settings.backend == "dir":
        return DirArtifactStore(settings.dir_root)
    client, collection = make_mongo_collection(settings.mongo_url, settings.mongo_db)
    del client  # 生命周期随进程（与 checkpoints/ledger 的 mongo 客户端同策略）
    return GridFsArtifactStore(AsyncGridFSBucket(collection.database, bucket_name="kokoro_artifacts"))

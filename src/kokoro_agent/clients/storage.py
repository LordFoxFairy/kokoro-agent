"""GA 使用的 Storage public contract 窄协议。"""

from __future__ import annotations

from typing import Protocol

from pydantic import BaseModel, ConfigDict

from kokoro_agent.contract import ExecutionIdentity


class StorageClientError(RuntimeError):
    """Storage public client 在发布产物时不可用或拒绝请求。"""


class DeliveryRequest(BaseModel):
    """GA 交给 Storage facade 的一次产物发布请求。"""

    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    request_id: str
    run_id: str
    namespace: str
    identity: ExecutionIdentity
    path: str
    title: str
    note: str
    mime_type: str
    content_sha256: str
    content: bytes


class DeliveryReceipt(BaseModel):
    """Storage 完成 upload/asset/artifact 生命周期后的稳定回执。"""

    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    artifact_id: str
    asset_id: str
    content_sha256: str
    size_bytes: int
    mime_type: str
    replayed: bool = False


class DeliveryClient(Protocol):
    """Storage Artifact public facade；不向 GA 暴露 bucket/key/签名 URL。"""

    async def publish(self, request: DeliveryRequest) -> DeliveryReceipt: ...


class PackageStore(Protocol):
    """Skill fixture/package reader 的字节面，不用于 GA 产物交付。"""

    async def put(self, ref: str, data: bytes) -> None: ...

    async def get(self, ref: str) -> bytes: ...

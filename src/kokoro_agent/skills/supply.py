"""skills 资产物化布局（backend 内虚拟路径）与物化所需的 backend 能力面。

点前缀：skills 是能力供给不是用户产物——session 文件清单与 S3 归档按隐藏目录跳过。
物化时机=装配期 reconcile（graph state 账本驱动增量），不再是 read 时单包按需。
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from deepagents.backends.protocol import ExecuteResponse, FileUploadResponse, LsResult

SKILLS_ROOT = "/.skills/"


class MaterializeBackend(Protocol):
    """物化 reconcile 依赖的 backend 能力面（BackendProtocol 结构化满足）：
    upload 写包体、als 探目录（自愈/GC 列举）；删除走 aexecute（BackendProtocol 无删除面）,
    仅在沙箱档（SandboxBackendProtocol）可用,故 GC 删除处按能力探测降级。"""

    async def aupload_files(self, files: list[tuple[str, bytes]]) -> list[FileUploadResponse]: ...

    async def als(self, path: str) -> LsResult: ...


@runtime_checkable
class ExecCapableBackend(Protocol):
    """沙箱档追加的 shell 能力（GC 删除用；虚拟根 backend 的 execute cwd=工作区根,
    与文件面 /.skills 映射同锚,故相对 rm 落点一致）。runtime_checkable：GC 处 isinstance 探测降级。"""

    async def aexecute(self, command: str, *, timeout: int | None = None) -> ExecuteResponse: ...

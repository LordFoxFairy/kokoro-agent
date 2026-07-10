"""skills 资产供给布局（backend 内虚拟路径）与供给端口。

点前缀：skills 是能力供给不是用户产物——session 文件清单与 S3 归档按隐藏目录跳过。
供给时机=read_skill 按需单包（渐进披露全链路），不再有装配期全量物化。
"""

from __future__ import annotations

from typing import Protocol

from deepagents.backends.protocol import FileUploadResponse

SKILLS_ROOT = "/.skills/"


class UploadCapableBackend(Protocol):
    """供给只依赖 upload_files 能力面（BackendProtocol 全家桶结构化满足）。"""

    def upload_files(self, files: list[tuple[str, bytes]]) -> list[FileUploadResponse]: ...

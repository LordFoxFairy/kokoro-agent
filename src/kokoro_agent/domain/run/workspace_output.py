"""The frozen namespace for user-visible process output, not a general file API."""

from hashlib import sha256
from typing import Literal, TypeAlias

from kokoro_agent.domain.chat.projection import safe_delivery_display_path

OUTPUT_ROOT = "/outputs/"
OUTPUT_TOOLS = frozenset({"write_file", "edit_file"})
OUTPUT_PROOF = "kokoro_workspace_output_path_sha256_v1"
OUTPUT_METADATA = "kokoro_workspace_output_root"
MAX_OUTPUT_BYTES = 8 * 1024 * 1024
MAX_OUTPUT_VERSIONS = 128
WorkspaceOutputMime: TypeAlias = Literal[
    "text/plain", "text/markdown", "text/csv", "application/json"
]


def workspace_output_mime(path: str) -> WorkspaceOutputMime:
    extension = path.rsplit(".", 1)[-1].lower()
    if extension in {"md", "markdown"}:
        return "text/markdown"
    if extension == "csv":
        return "text/csv"
    if extension == "json":
        return "application/json"
    return "text/plain"


def output_path_digest(path: str) -> str | None:
    if (
        not path.startswith(OUTPUT_ROOT)
        or not safe_delivery_display_path(path)
        or len(path.rsplit("/", 1)[-1]) > 255
    ):
        return None
    return sha256(path.encode("utf-8")).hexdigest()

"""The frozen namespace for user-visible process output, not a general file API."""

from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from hashlib import sha256
import json
from typing import Annotated, Literal, Self, TypeAlias

from pydantic import (
    BaseModel,
    ConfigDict,
    StringConstraints,
    ValidationError,
    field_validator,
)

from kokoro_agent.domain.chat.projection import safe_delivery_display_path

OUTPUT_ROOT = "/outputs/"
OUTPUT_TOOLS = frozenset({"write_file", "edit_file"})
OUTPUT_PROOF = "kokoro_workspace_output_path_sha256_v1"
OUTPUT_METADATA = "kokoro_workspace_output_root"
MAX_OUTPUT_BYTES = 8 * 1024 * 1024
MAX_OUTPUT_VERSIONS = 128
WorkspaceOutputGuard: TypeAlias = Callable[[str, bytes], Awaitable[bool]]


_Digest = Annotated[
    str, StringConstraints(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")
]
NativeOutputTool: TypeAlias = Literal["write_file", "edit_file", "execute"]


def _canonical_digest(parts: tuple[str | int | None, ...]) -> str:
    material = json.dumps(parts, ensure_ascii=True, separators=(",", ":"))
    return sha256(material.encode("ascii")).hexdigest()


class WorkspaceExecuteIntent(BaseModel):
    """A keep-first command binding without the command or its environment."""

    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    path: str
    command_sha256: _Digest
    key: _Digest

    @field_validator("path")
    @classmethod
    def _output_path(cls, value: str) -> str:
        if output_path_digest(value) is None or "/" in value[len(OUTPUT_ROOT) :]:
            raise ValueError("invalid workspace script output path")
        return value

    @classmethod
    def bind(
        cls,
        *,
        path: str,
        command: str,
        timeout: int | None,
        run_id: str,
        tool_call_id: str,
        identity: str,
    ) -> Self:
        if (
            not command.strip()
            or "\0" in command
            or len(command.encode("utf-8")) > 131_072
            or (
                timeout is not None
                and (type(timeout) is not int or not 1 <= timeout <= 300)
            )
        ):
            raise ValueError("invalid workspace script arguments")
        return cls(
            path=path,
            command_sha256=_canonical_digest((command, timeout)),
            key=_canonical_digest(
                ("docker-workspace-output:v1", run_id, tool_call_id, identity)
            ),
        )

    @classmethod
    def from_journal(cls, raw: object) -> Self | None:
        # The only producer uses model_dump_json. Exact round-trip also rejects
        # repeated keys that the JSON decoder could otherwise collapse.
        if not isinstance(raw, str) or len(raw) > 9 * 1024:
            return None
        try:
            intent = cls.model_validate_json(raw)
        except ValidationError:
            return None
        return intent if intent.model_dump_json() == raw else None


@dataclass(frozen=True, slots=True)
class NativeWorkspaceWrite:
    path: str
    content: bytes = field(repr=False)
    occurrences: int | None = None
    script: bool = False

    def __post_init__(self) -> None:
        if type(self.script) is not bool or (
            self.script and self.occurrences is not None
        ):
            raise ValueError("invalid native workspace script capture")
        if self.occurrences is not None and (
            type(self.occurrences) is not int or self.occurrences < 1
        ):
            raise ValueError("invalid native workspace edit occurrence count")

    @property
    def tool_name(self) -> NativeOutputTool:
        if self.script:
            return "execute"
        return "edit_file" if self.occurrences is not None else "write_file"

    @property
    def acknowledgement(self) -> str:
        if self.script:
            return f"Captured workspace output {self.path}"
        if self.occurrences is None:
            return f"Updated file {self.path}"
        return f"Successfully replaced {self.occurrences} instance(s) of the string in '{self.path}'"


def matches_native_output_ack(name: object, path: str, result: object) -> bool:
    if name == "execute":
        return result == f"Captured workspace output {path}"
    if name == "write_file":
        return result == f"Updated file {path}"
    prefix = "Successfully replaced "
    if (
        name != "edit_file"
        or not isinstance(result, str)
        or not result.startswith(prefix)
    ):
        return False
    count, _, rest = result[len(prefix) :].partition(" instance(s) of the string in ")
    return (
        rest == f"'{path}'"
        and 1 <= len(count) <= 10
        and count.isascii()
        and count.isdecimal()
        and not count.startswith("0")
    )


def native_output_version_id(
    run_id: str, tool_call_id: str, path: str, name: NativeOutputTool
) -> str:
    domain = {
        "write_file": "native-workspace-write:v1",
        "edit_file": "native-workspace-edit:v1",
        "execute": "native-workspace-script:v1",
    }[name]
    return "wsv_" + _canonical_digest((domain, run_id, tool_call_id, path))


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

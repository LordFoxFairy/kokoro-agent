"""Exact ownership, daemon identity and reclamation for one Docker script output."""

from __future__ import annotations

import hashlib
import json
import math
import re
import shutil
import subprocess
import time
from collections.abc import Generator, Mapping
from contextlib import ExitStack, contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol, TypedDict

from pydantic import BaseModel, ConfigDict, ValidationError, with_config

from kokoro_agent.sandbox.execution_environment import ExecutionBoundaryError
from kokoro_agent.sandbox.file_egress import open_local_directory

_MOUNT_POINT = "/workspace"


@dataclass(slots=True)
class WorkspaceOutputCapture:
    owner: object = field(repr=False)
    path: str
    key: str
    recover: bool
    content: bytes | None = field(default=None, repr=False, init=False)
    _closed: bool = field(default=False, init=False)
    _used: bool = field(default=False, init=False)

    def claim(self) -> bool:
        if self._closed or self._used:
            return False
        self._used = True
        return True

    def close(self) -> None:
        self._closed = True


_WORKSPACE_OUTPUT_CAPTURE: ContextVar[WorkspaceOutputCapture | None] = ContextVar(
    "docker_workspace_output_capture", default=None
)


@contextmanager
def capture_workspace_output(
    owner: object, path: str, key: str, *, recover: bool
) -> Generator[WorkspaceOutputCapture]:
    capture = WorkspaceOutputCapture(owner=owner, path=path, key=key, recover=recover)
    token = _WORKSPACE_OUTPUT_CAPTURE.set(capture)
    try:
        yield capture
    finally:
        # A spawned task may inherit this object even after our context resets.
        capture.close()
        _WORKSPACE_OUTPUT_CAPTURE.reset(token)


def current_workspace_output(owner: object) -> WorkspaceOutputCapture | None:
    capture = _WORKSPACE_OUTPUT_CAPTURE.get()
    return capture if capture is not None and capture.owner is owner else None


class DockerOperation(Protocol):
    def __call__(
        self,
        *args: str,
        docker_cli_environment: Mapping[str, str] | None,
        timeout: int = 30,
    ) -> subprocess.CompletedProcess[str]: ...


OUTPUT_UNAVAILABLE = "sandbox_workspace_output_unavailable"
_OUTPUT_INSPECT = (
    '{"id":{{json .Id}},"name":{{json .Name}},'
    '"key":{{json (index .Config.Labels "kokoro-workspace-output")}},'
    '"status":{{json .State.Status}},"exit":{{json .State.ExitCode}},'
    '"mounts":{{json .Mounts}}}'
)
_CONTAINER_ID = re.compile(r"[0-9a-f]{64}")
_DAEMON_ID = re.compile(r"[A-Za-z0-9:_-]{6,128}")


@with_config(ConfigDict(strict=True, extra="ignore"))
class _OutputMount(TypedDict):
    Type: str
    Source: str
    Destination: str
    RW: bool


class _OutputContainer(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")
    id: str
    name: str
    key: str | None
    status: str
    exit: int
    mounts: list[_OutputMount]


def output_names(key: str) -> tuple[str, str]:
    if _CONTAINER_ID.fullmatch(key) is None:
        raise ExecutionBoundaryError(OUTPUT_UNAVAILABLE)
    return "kokoro-output-" + key, ".kokoro-output-" + key


def remaining(deadline: float) -> int:
    remaining = math.ceil(deadline - time.monotonic())
    if remaining <= 0:
        raise ExecutionBoundaryError(OUTPUT_UNAVAILABLE)
    return remaining


def workspace_output_identity(
    docker: DockerOperation,
    workspace_root: Path,
    docker_cli_environment: Mapping[str, str] | None,
) -> str:
    """Bind the private staging location to this Docker daemon and host root."""
    try:
        base = workspace_root.resolve(strict=True)
        if not base.is_dir():
            raise ExecutionBoundaryError(OUTPUT_UNAVAILABLE)
        result = docker(
            "info",
            "--format",
            "{{.ID}}",
            docker_cli_environment=docker_cli_environment,
            timeout=5,
        )
        daemon = result.stdout.strip()
        if result.returncode or not _DAEMON_ID.fullmatch(daemon):
            raise ExecutionBoundaryError(OUTPUT_UNAVAILABLE)
        material = json.dumps(
            (daemon, str(base)), ensure_ascii=True, separators=(",", ":")
        )
        return hashlib.sha256(material.encode("ascii")).hexdigest()
    except (OSError, RuntimeError, subprocess.SubprocessError, ValueError):
        raise ExecutionBoundaryError(OUTPUT_UNAVAILABLE) from None


def inspect_workspace_output(
    docker: DockerOperation,
    key: str,
    base: Path,
    context: Mapping[str, str] | None,
    deadline: float,
) -> _OutputContainer | None:
    name, scratch = output_names(key)
    result = docker(
        "inspect",
        "--type",
        "container",
        "--format",
        _OUTPUT_INSPECT,
        name,
        timeout=remaining(deadline),
        docker_cli_environment=context,
    )
    if result.returncode:
        inventory = docker(
            "ps",
            "-aq",
            "--no-trunc",
            "--filter",
            f"name=^/{name}$",
            timeout=remaining(deadline),
            docker_cli_environment=context,
        )
        if inventory.returncode or inventory.stdout.strip():
            raise ExecutionBoundaryError(OUTPUT_UNAVAILABLE)
        return None
    try:
        row = _OutputContainer.model_validate_json(result.stdout)
    except ValidationError:
        raise ExecutionBoundaryError(OUTPUT_UNAVAILABLE) from None
    mounts = {mount["Destination"]: mount for mount in row.mounts}
    output = mounts.get(_MOUNT_POINT + "/outputs")
    workspace = mounts.get(_MOUNT_POINT)
    if (
        _CONTAINER_ID.fullmatch(row.id) is None
        or row.name != "/" + name
        or row.key != key
        or len(row.mounts) != 2
        or output is None
        or output
        != {
            "Type": "bind",
            "Source": str(base / scratch),
            "Destination": _MOUNT_POINT + "/outputs",
            "RW": True,
        }
        or workspace is None
        or workspace["Type"] != "bind"
        or workspace["RW"]
        or not Path(workspace["Source"]).is_absolute()
        or Path(workspace["Source"]).parent != base
        or Path(workspace["Source"]).name == scratch
    ):
        raise ExecutionBoundaryError(OUTPUT_UNAVAILABLE)
    return row


def cleanup_workspace_output(
    docker: DockerOperation,
    key: str,
    workspace_root: Path,
    docker_cli_environment: Mapping[str, str] | None,
) -> None:
    """Only reclaim this exact output sandbox and its private host directory."""
    _, scratch = output_names(key)
    deadline = time.monotonic() + 8
    try:
        base = workspace_root.resolve(strict=True)
        row = inspect_workspace_output(
            docker, key, base, docker_cli_environment, deadline
        )
        if row is not None:
            removed = docker(
                "rm",
                "-f",
                row.id,
                timeout=remaining(deadline),
                docker_cli_environment=docker_cli_environment,
            )
            if (
                removed.returncode
                and inspect_workspace_output(
                    docker, key, base, docker_cli_environment, deadline
                )
                is not None
            ):
                raise ExecutionBoundaryError(OUTPUT_UNAVAILABLE)
        with ExitStack() as stack:
            root_fd = open_local_directory(base, stack)
            try:
                shutil.rmtree(scratch, dir_fd=root_fd)
            except FileNotFoundError:
                pass
    except (OSError, RuntimeError, subprocess.SubprocessError, ValueError):
        raise ExecutionBoundaryError(OUTPUT_UNAVAILABLE) from None

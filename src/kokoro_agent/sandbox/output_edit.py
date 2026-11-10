"""Exact postimage of an admitted native edit, not a workspace download."""

from __future__ import annotations

from collections.abc import Generator
from contextlib import ExitStack, contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
import os
from pathlib import Path
import secrets
import stat

from deepagents.backends.local_shell import LocalShellBackend
from deepagents.backends.protocol import EditResult
from deepagents.backends.utils import perform_string_replacement

from kokoro_agent.domain.run.workspace_output import (
    MAX_OUTPUT_BYTES,
    NativeWorkspaceWrite,
)
from kokoro_agent.sandbox.execution_environment import ExecutionBoundaryError
from kokoro_agent.sandbox.file_egress import (
    FILE_UNAVAILABLE,
    open_local_directory,
    open_local_parent,
    read_local_bytes,
    relative_path,
)


@dataclass(slots=True)
class _EditCapture:
    path: str
    postimage: NativeWorkspaceWrite | None = None


_EDIT_CAPTURE: ContextVar[_EditCapture | None] = ContextVar(
    "workspace_native_edit", default=None
)


@contextmanager
def capture_output_edit(path: str | None) -> Generator[_EditCapture | None]:
    # The SDK offloads edit() with to_thread: the context copies the reference to
    # this one-call slot, not a value assigned inside the child thread.
    capture = _EditCapture(path) if path is not None else None
    token = _EDIT_CAPTURE.set(capture)
    try:
        yield capture
    finally:
        _EDIT_CAPTURE.reset(token)


def current_output_edit(path: str) -> _EditCapture | None:
    capture = _EDIT_CAPTURE.get()
    return capture if capture is not None and capture.path == path else None


def _identity(info: os.stat_result) -> tuple[int, int]:
    return info.st_dev, info.st_ino


def _revision(info: os.stat_result) -> tuple[int, int, int, int, int]:
    return (*_identity(info), info.st_size, info.st_mtime_ns, info.st_ctime_ns)


def _check_location(
    staging: int, workspace: str, root: int, parent: int, parts: list[str]
) -> os.stat_result:
    with ExitStack() as stack:
        current_root = open_local_directory(workspace, stack, parent=staging)
        if _identity(os.fstat(current_root)) != _identity(os.fstat(root)):
            raise ExecutionBoundaryError(FILE_UNAVAILABLE)
        current = open_local_parent(current_root, parts, stack)
        if _identity(os.fstat(current)) != _identity(os.fstat(parent)):
            raise ExecutionBoundaryError(FILE_UNAVAILABLE)
        entry = os.stat(parts[-1], dir_fd=current, follow_symlinks=False)
        if not stat.S_ISREG(entry.st_mode) or entry.st_nlink != 1:
            raise ExecutionBoundaryError(FILE_UNAVAILABLE)
        return entry


def _newlines(text: str) -> str:
    return text.replace("\r\n", "\n").replace("\r", "\n")


def _edit(
    root: Path, capture: _EditCapture, old: str, new: str, replace_all: bool
) -> EditResult:
    parts = relative_path(capture.path).split("/")
    with ExitStack() as stack:
        staging = open_local_directory(root.parent, stack)
        root_fd = open_local_directory(root.name, stack, parent=staging)
        parent = open_local_parent(root_fd, parts, stack)
        fd = os.open(
            parts[-1], os.O_RDWR | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent
        )
        stack.callback(os.close, fd)
        initial = os.fstat(fd)
        if not stat.S_ISREG(initial.st_mode) or initial.st_nlink != 1:
            raise ExecutionBoundaryError(FILE_UNAVAILABLE)
        if max(len(old), len(new)) > MAX_OUTPUT_BYTES:
            raise ExecutionBoundaryError(FILE_UNAVAILABLE)
        original = _newlines(read_local_bytes(fd, MAX_OUTPUT_BYTES).decode("utf-8"))
        old, new = _newlines(old), _newlines(new)
        matches = original.count(old)
        if (matches == 1 or replace_all) and len(original) + matches * (
            len(new) - len(old)
        ) > MAX_OUTPUT_BYTES:
            raise ExecutionBoundaryError(FILE_UNAVAILABLE)
        replaced = perform_string_replacement(original, old, new, replace_all)
        if isinstance(replaced, str):
            return EditResult(error=replaced)
        updated, occurrences = replaced
        if len(updated) > MAX_OUTPUT_BYTES or occurrences < 1:
            raise ExecutionBoundaryError(FILE_UNAVAILABLE)
        raw = updated.encode("utf-8")
        if len(raw) > MAX_OUTPUT_BYTES:
            raise ExecutionBoundaryError(FILE_UNAVAILABLE)
        # A Docker command can mutate its mounted workspace, but not this
        # host-side staging entry. Rename installs one exact full postimage.
        if os.fstat(staging).st_dev != initial.st_dev:
            raise ExecutionBoundaryError(FILE_UNAVAILABLE)
        token = ".kokoro-output-" + secrets.token_hex(16)
        stage_fd = os.open(
            token,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
            0o600,
            dir_fd=staging,
        )
        installed = False
        try:
            with os.fdopen(stage_fd, "wb") as staged:
                staged.write(raw)
                staged.flush()
                os.fchmod(staged.fileno(), stat.S_IMODE(initial.st_mode) & 0o777)
                staged_info = os.fstat(staged.fileno())
                staged_identity = _identity(staged_info)
                if (
                    staged_info.st_size != len(raw)
                    or _revision(
                        _check_location(staging, root.name, root_fd, parent, parts)
                    )
                    != _revision(initial)
                    or _identity(os.stat(token, dir_fd=staging, follow_symlinks=False))
                    != staged_identity
                ):
                    raise ExecutionBoundaryError(FILE_UNAVAILABLE)
                os.replace(token, parts[-1], src_dir_fd=staging, dst_dir_fd=parent)
                installed = True
                current = _check_location(staging, root.name, root_fd, parent, parts)
                if (
                    _identity(current) != staged_identity
                    or current.st_size != len(raw)
                    or current.st_mtime_ns != staged_info.st_mtime_ns
                ):
                    raise ExecutionBoundaryError(FILE_UNAVAILABLE)
        finally:
            if not installed:
                os.unlink(token, dir_fd=staging)
        capture.postimage = NativeWorkspaceWrite(capture.path, raw, occurrences)
        return EditResult(path=capture.path, occurrences=occurrences)


class WorkspaceOutputLocalShellBackend(LocalShellBackend):
    """The ordinary backend except for the single journal-authorized output edit."""

    def edit(
        self,
        file_path: str,
        old_string: str,
        new_string: str,
        replace_all: bool = False,
    ) -> EditResult:
        capture = current_output_edit(file_path)
        if capture is None:
            return super().edit(file_path, old_string, new_string, replace_all)
        if not self.virtual_mode or capture.postimage is not None:
            return EditResult(error=FILE_UNAVAILABLE)
        try:
            return _edit(self.cwd, capture, old_string, new_string, replace_all)
        except (OSError, UnicodeError, ExecutionBoundaryError):
            return EditResult(error=FILE_UNAVAILABLE)


# Shared E2B mutation program: no login shell or user text in argv.
# Both operations verify the installed bytes on the same open descriptor.
E2B_OUTPUT_MUTATION = r"""
from contextlib import ExitStack
from hashlib import sha256
import os, secrets, signal, stat, sys

signal.alarm(30)
path, size, digest, *edit = sys.argv[1:]
size = int(size)
raw = sys.stdin.buffer.read(size)
if len(raw) != size or sha256(raw).hexdigest() != digest:
    sys.exit(1)
parts = path.split("/")
flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
maximum = 8 * 1024 * 1024

def revision(info):
    return info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns

def parent(stack, root, create):
    for part in parts[:-1]:
        if create:
            try:
                os.mkdir(part, 0o700, dir_fd=root)
            except FileExistsError:
                pass
        root = os.open(part, flags, dir_fd=root)
        stack.callback(os.close, root)
    return root

def edited(stack, dest, name, raw):
    fd = os.open(name, os.O_RDWR | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=dest)
    source = stack.enter_context(os.fdopen(fd, "rb"))
    info = os.fstat(fd)
    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_size > maximum:
        sys.exit(1)
    text = source.read(maximum + 1)
    if len(text) != info.st_size or revision(os.fstat(fd)) != revision(info):
        sys.exit(1)
    text = text.decode("utf-8")
    offset = int(edit[0])
    old, new = raw[:offset].decode("utf-8"), raw[offset:].decode("utf-8")
    def crlf(value):
        return value.replace("\r\n", "\n").replace("\n", "\r\n")
    for find, replace in ((old, new), (crlf(old), crlf(new)),
                          (old.replace("\r\n", "\n"), new.replace("\r\n", "\n"))):
        count = text.count(find)
        if count:
            break
    if not count or (count != 1 and edit[1] != "1"):
        sys.exit(1)
    if len(text) + count * (len(replace) - len(find)) > maximum:
        sys.exit(1)
    updated = text.replace(find, replace, count).encode("utf-8")
    if len(updated) > maximum:
        sys.exit(1)
    return updated, count, info

try:
    with ExitStack() as stack:
        root = os.open(".", flags)
        stack.callback(os.close, root)
        dest = parent(stack, root, not edit)
        if edit:
            raw, count, initial = edited(stack, dest, parts[-1], raw)
            size, digest = len(raw), sha256(raw).hexdigest()
        temp = ".kokoro-write-" + secrets.token_hex(16)
        fd = os.open(temp, os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                     0o600, dir_fd=dest)
        try:
            with os.fdopen(fd, "w+b") as output:
                if output.write(raw) != size:
                    sys.exit(1)
                output.flush()
                if edit:
                    os.fchmod(output.fileno(), stat.S_IMODE(initial.st_mode) & 0o777)
                written = os.fstat(output.fileno())
                if written.st_size != size:
                    sys.exit(1)
                if edit:
                    current = os.stat(parts[-1], dir_fd=parent(stack, root, False),
                                      follow_symlinks=False)
                    staging = os.stat(temp, dir_fd=dest, follow_symlinks=False)
                    if revision(current) != revision(initial) or revision(staging) != revision(written):
                        sys.exit(1)
                    os.replace(temp, parts[-1], src_dir_fd=dest, dst_dir_fd=dest)
                else:
                    os.link(temp, parts[-1], src_dir_fd=dest, dst_dir_fd=dest,
                            follow_symlinks=False)
                linked = os.fstat(output.fileno())
                output.seek(0)
                actual = output.read(size + 1)
                verified_digest = sha256(actual).hexdigest()
                current = os.stat(parts[-1], dir_fd=parent(stack, root, False),
                                  follow_symlinks=False)
                if len(actual) != size or verified_digest != digest or (
                    current.st_dev, current.st_ino, current.st_size,
                    current.st_mtime_ns, current.st_ctime_ns
                ) != (
                    written.st_dev, written.st_ino, size,
                    written.st_mtime_ns, linked.st_ctime_ns
                ):
                    sys.exit(1)
        finally:
            try:
                os.unlink(temp, dir_fd=dest)
            except FileNotFoundError:
                if not edit:
                    raise
except (OSError, UnicodeError):
    sys.exit(1)

if edit:
    sys.stdout.buffer.write(f"{count}:{size}:{verified_digest}\n".encode("ascii"))
    sys.stdout.buffer.write(actual)
else:
    print(f"{size}:{verified_digest}")
"""

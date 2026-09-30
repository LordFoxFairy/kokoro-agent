"""Read-only DeepAgents backend route for typed exact Skills."""

from __future__ import annotations

import fnmatch
from collections.abc import Mapping, Sequence
from pathlib import PurePosixPath
from typing import Protocol

from deepagents.backends.protocol import (
    FILE_NOT_FOUND,
    PERMISSION_DENIED,
    BackendProtocol,
    EditResult,
    FileDownloadResponse,
    FileInfo,
    FileUploadResponse,
    GlobResult,
    GrepMatch,
    GrepResult,
    LsResult,
    ReadResult,
    WriteResult,
)
from deepagents.backends.utils import create_file_data, slice_read_response

from kokoro_agent.clients.skills import ResolvedSkill, SkillClientError

SKILLS_ROOT = "/.skills/"


class PackageReader(Protocol):
    async def load_package(self, skill: ResolvedSkill) -> Mapping[str, bytes]: ...


class TypedSkillBackend(BackendProtocol):
    """Expose authorized Skill references as a native read-only backend.

    The enclosing ``CompositeBackend`` maps ``/.skills/`` to this backend, so
    paths received here are rooted at ``/``. Packages stay behind the public
    ``PackageReader`` contract and are fetched lazily; GA never copies them into a
    sandbox or creates a second Skill loader.
    """

    def __init__(
        self, initial: Sequence[ResolvedSkill], reader: PackageReader | None
    ) -> None:
        self._skills = {skill.path_segment: skill for skill in initial}
        self._reader = reader

    async def _package(self, name: str) -> Mapping[str, bytes] | None:
        skill = self._skills.get(name)
        if skill is None:
            return None
        if self._reader is None:
            raise SkillClientError("SKILL_READER_UNAVAILABLE")
        return await self._reader.load_package(skill)

    @staticmethod
    def _valid_path(path: str) -> bool:
        return path == "/" or (
            path.startswith("/")
            and "\\" not in path
            and all(
                part not in {"", ".", ".."}
                for part in path.removesuffix("/")[1:].split("/")
            )
        )

    @classmethod
    def _parts(cls, path: str) -> tuple[str, str] | None:
        if not cls._valid_path(path) or path.endswith("/"):
            return None
        parts = path[1:].split("/")
        if len(parts) < 2:
            return None
        return parts[0], "/".join(parts[1:])

    async def als(self, path: str) -> LsResult:
        if not self._valid_path(path):
            return LsResult(error=FILE_NOT_FOUND)
        normalized = path.removesuffix("/") or "/"
        if normalized == "/":
            for name in self._skills:
                await self._package(name)
            return LsResult(
                entries=[
                    FileInfo(path=f"/{name}/", is_dir=True)
                    for name in sorted(self._skills)
                ]
            )
        parts = PurePosixPath(normalized).parts
        if len(parts) < 2 or parts[0] != "/" or parts[1] not in self._skills:
            return LsResult(error=FILE_NOT_FOUND)
        name = parts[1]
        package = await self._package(name)
        if package is None:
            return LsResult(error=FILE_NOT_FOUND)
        directory = "/".join(parts[2:])
        prefix = f"{directory}/" if directory else ""
        children: dict[str, FileInfo] = {}
        for relative, content in sorted(package.items()):
            if not relative.startswith(prefix):
                continue
            remainder = relative[len(prefix) :]
            child, separator, _ = remainder.partition("/")
            if not child:
                continue
            child_path = f"/{name}/{prefix}{child}"
            if separator:
                children[child] = FileInfo(path=f"{child_path}/", is_dir=True)
            else:
                children[child] = FileInfo(
                    path=child_path,
                    is_dir=False,
                    size=len(content),
                )
        package_directories = {
            str(PurePosixPath(relative).parent).removeprefix("./")
            for relative in package
        }
        if not children and directory not in package_directories:
            return LsResult(error=FILE_NOT_FOUND)
        return LsResult(entries=list(children.values()))

    async def adownload_files(self, paths: list[str]) -> list[FileDownloadResponse]:
        if len(paths) > 128:
            raise SkillClientError("SKILL_READ_LIMIT")
        total = 0
        responses: list[FileDownloadResponse] = []
        for path in paths:
            parsed = self._parts(path)
            if parsed is None:
                responses.append(FileDownloadResponse(path=path, error=FILE_NOT_FOUND))
                continue
            name, relative = parsed
            package = await self._package(name)
            content = package.get(relative) if package is not None else None
            total += len(content) if content is not None else 0
            if total > 134217728:
                raise SkillClientError("SKILL_READ_LIMIT")
            del package
            responses.append(
                FileDownloadResponse(
                    path=path,
                    content=content,
                    error=None if content is not None else FILE_NOT_FOUND,
                )
            )
        return responses

    async def aread(
        self, file_path: str, offset: int = 0, limit: int = 2000
    ) -> ReadResult:
        response = (await self.adownload_files([file_path]))[0]
        if response.content is None:
            return ReadResult(error=f"File {file_path!r} not found")
        try:
            content = response.content.decode("utf-8")
        except UnicodeDecodeError:
            return ReadResult(error=f"File {file_path!r} is not UTF-8 text")
        data = create_file_data(content)
        sliced = slice_read_response(data, offset, limit)
        if isinstance(sliced, ReadResult):
            return sliced
        return ReadResult(file_data=create_file_data(sliced))

    async def aglob(self, pattern: str, path: str = "/") -> GlobResult:
        if not self._valid_path(path):
            return GlobResult(error=FILE_NOT_FOUND)
        prefix = path.rstrip("/") + "/"
        matches: list[FileInfo] = []
        for name in self._skills:
            package = await self._package(name)
            if package is None:
                continue
            for relative, content in sorted(package.items()):
                file_path = f"/{name}/{relative}"
                if file_path.startswith(prefix) and fnmatch.fnmatch(
                    file_path.lstrip("/"), pattern
                ):
                    matches.append(
                        FileInfo(path=file_path, is_dir=False, size=len(content))
                    )
            del package
        return GlobResult(matches=matches)

    async def agrep(
        self, pattern: str, path: str | None = None, glob: str | None = None
    ) -> GrepResult:
        if not self._valid_path(path or "/"):
            return GrepResult(error=FILE_NOT_FOUND)
        prefix = (path or "/").rstrip("/") + "/"
        matches: list[GrepMatch] = []
        output_bytes = 0
        for name in self._skills:
            package = await self._package(name)
            if package is None:
                continue
            for relative, content in sorted(package.items()):
                file_path = f"/{name}/{relative}"
                if not file_path.startswith(prefix) or (
                    glob is not None and not fnmatch.fnmatch(file_path, glob)
                ):
                    continue
                try:
                    text = content.decode("utf-8")
                except UnicodeDecodeError:
                    continue
                for line_number, line in enumerate(text.splitlines(), start=1):
                    if pattern in line:
                        output_bytes += len(line.encode("utf-8"))
                        if len(matches) >= 1000 or output_bytes > 1048576:
                            return GrepResult(error="SKILL_READ_LIMIT")
                        matches.append(
                            GrepMatch(path=file_path, line=line_number, text=line)
                        )
            del package
        return GrepResult(matches=matches)

    async def awrite(self, file_path: str, content: str) -> WriteResult:
        del file_path, content
        return WriteResult(error=PERMISSION_DENIED)

    async def aedit(
        self,
        file_path: str,
        old_string: str,
        new_string: str,
        replace_all: bool = False,
    ) -> EditResult:
        del file_path, old_string, new_string, replace_all
        return EditResult(error=PERMISSION_DENIED)

    async def aupload_files(
        self, files: list[tuple[str, bytes]]
    ) -> list[FileUploadResponse]:
        return [
            FileUploadResponse(path=path, error=PERMISSION_DENIED) for path, _ in files
        ]


__all__ = ["TypedSkillBackend", "SKILLS_ROOT"]

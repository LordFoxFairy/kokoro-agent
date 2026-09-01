"""Read-only DeepAgents backend route for Capability-resolved Skills."""

from __future__ import annotations

import fnmatch
from collections.abc import Mapping, Sequence
from pathlib import PurePosixPath

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

from kokoro_agent.clients.skills import ResolvedSkill, SkillClientError, SkillReader

SKILLS_ROOT = "/.skills/"


class CapabilitySkillBackend(BackendProtocol):
    """Expose authorized Skill references as a native read-only backend.

    The enclosing ``CompositeBackend`` maps ``/.skills/`` to this backend, so
    paths received here are rooted at ``/``. Packages stay behind the public
    ``SkillReader`` contract and are fetched lazily; GA never copies them into a
    sandbox or creates a second Skill loader.
    """

    def __init__(
        self, initial: Sequence[ResolvedSkill], reader: SkillReader
    ) -> None:
        self._skills = {skill.name: skill for skill in initial}
        self._reader = reader
        self._packages: dict[str, Mapping[str, str]] = {}

    async def _package(self, name: str) -> Mapping[str, str] | None:
        skill = self._skills.get(name)
        if skill is None:
            return None
        cached = self._packages.get(name)
        if cached is not None:
            return cached
        try:
            package = await self._reader.load_package(
                skill.scope, skill.name, skill.content_hash
            )
        except SkillClientError:
            return None
        self._packages[name] = package
        return package

    @staticmethod
    def _parts(path: str) -> tuple[str, str] | None:
        parts = PurePosixPath(path).parts
        if len(parts) < 3 or parts[0] != "/" or ".." in parts:
            return None
        return parts[1], "/".join(parts[2:])

    async def als(self, path: str) -> LsResult:
        normalized = str(PurePosixPath(path))
        if normalized == "/":
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
                    size=len(content.encode("utf-8")),
                )
        package_directories = {
            str(PurePosixPath(relative).parent).removeprefix("./")
            for relative in package
        }
        if not children and directory not in package_directories:
            return LsResult(error=FILE_NOT_FOUND)
        return LsResult(entries=list(children.values()))

    async def adownload_files(
        self, paths: list[str]
    ) -> list[FileDownloadResponse]:
        responses: list[FileDownloadResponse] = []
        for path in paths:
            parsed = self._parts(path)
            if parsed is None:
                responses.append(FileDownloadResponse(path=path, error=FILE_NOT_FOUND))
                continue
            name, relative = parsed
            package = await self._package(name)
            content = package.get(relative) if package is not None else None
            responses.append(
                FileDownloadResponse(
                    path=path,
                    content=content.encode("utf-8") if content is not None else None,
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
        files = await self._all_files()
        prefix = str(PurePosixPath(path)).rstrip("/") + "/"
        return GlobResult(
            matches=[
                FileInfo(path=name, is_dir=False, size=len(content.encode("utf-8")))
                for name, content in sorted(files.items())
                if name.startswith(prefix) and fnmatch.fnmatch(name.lstrip("/"), pattern)
            ]
        )

    async def agrep(
        self, pattern: str, path: str | None = None, glob: str | None = None
    ) -> GrepResult:
        files = await self._all_files()
        prefix = (path or "/").rstrip("/") + "/"
        matches: list[GrepMatch] = []
        for file_path, content in sorted(files.items()):
            if not file_path.startswith(prefix):
                continue
            if glob is not None and not fnmatch.fnmatch(file_path, glob):
                continue
            for line_number, line in enumerate(content.splitlines(), start=1):
                if pattern in line:
                    matches.append(
                        GrepMatch(
                            path=file_path,
                            line=line_number,
                            text=line,
                        )
                    )
        return GrepResult(matches=matches)

    async def _all_files(self) -> dict[str, str]:
        files: dict[str, str] = {}
        for name in self._skills:
            package = await self._package(name)
            if package is None:
                continue
            files.update(
                (f"/{name}/{relative}", content)
                for relative, content in package.items()
            )
        return files

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
        return [FileUploadResponse(path=path, error=PERMISSION_DENIED) for path, _ in files]


__all__ = ["CapabilitySkillBackend", "SKILLS_ROOT"]

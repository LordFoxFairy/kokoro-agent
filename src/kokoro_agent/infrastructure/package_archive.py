"""Bounded ZIP32 content reader shared by independently authorized package consumers."""

from __future__ import annotations

import re
import struct
import unicodedata
import zlib
from collections.abc import Mapping
from types import MappingProxyType
from typing import NoReturn
from dataclasses import dataclass, field


class PackageArchiveError(ValueError):
    """Stable package boundary failure; never includes package content."""


def _fail(reason: str = "package_zip_invalid") -> NoReturn:
    raise PackageArchiveError(reason)


@dataclass(frozen=True, slots=True, kw_only=True)
class PackageMember:
    content: bytes = field(repr=False)
    executable: bool
    directory: bool


def read_package_archive(payload: bytes) -> Mapping[str, PackageMember]:
    """Return only fully verified original file bytes; no partial package escapes."""
    length = len(payload)
    if length > 33554432:
        _fail("package_limit_exceeded")

    def region(offset: int, size: int) -> bytes:
        if offset < 0 or size < 0 or offset + size > length:
            _fail()
        return payload[offset : offset + size]

    def u16(offset: int) -> int:
        return struct.unpack("<H", region(offset, 2))[0]

    def u32(offset: int) -> int:
        return struct.unpack("<I", region(offset, 4))[0]

    def extra(offset: int, size: int) -> None:
        region(offset, size)
        end = offset + size
        seen: set[int] = set()
        while offset < end:
            if offset + 4 > end:
                _fail()
            tag, count = u16(offset), u16(offset + 2)
            offset += 4
            if offset + count > end or tag in seen or tag in {1, 0x7075}:
                _fail()
            seen.add(tag)
            offset += count

    ends = [
        i
        for i in range(length - 22, max(-1, length - 65558), -1)
        if u32(i) == 0x06054B50 and i + 22 + u16(i + 20) == length
    ]
    if len(ends) != 1:
        _fail()
    end = ends[0]
    count, central_size, central = u16(end + 10), u32(end + 12), u32(end + 16)
    if (
        u16(end + 4)
        or u16(end + 6)
        or u16(end + 8) != count
        or count == 65535
        or central_size == 0xFFFFFFFF
        or central == 0xFFFFFFFF
        or central + central_size != end
    ):
        _fail()
    if count > 128:
        _fail("package_limit_exceeded")
    names: dict[str, bool] = {}
    files: dict[str, PackageMember] = {}
    spans: list[tuple[int, int]] = []
    cursor, total = central, 0
    for _ in range(count):
        region(cursor, 46)
        if u32(cursor) != 0x02014B50:
            _fail()
        flags, method = u16(cursor + 8), u16(cursor + 10)
        crc, compressed, size = u32(cursor + 16), u32(cursor + 20), u32(cursor + 24)
        name_size, extra_size, comment_size = (
            u16(cursor + 28),
            u16(cursor + 30),
            u16(cursor + 32),
        )
        local, attributes = u32(cursor + 42), u32(cursor + 38)
        if u16(cursor + 34) or 0xFFFFFFFF in {compressed, size, local}:
            _fail()
        if method not in {0, 8} or flags & ~(0x808 | (6 if method == 8 else 0)):
            _fail("package_zip_unsafe")
        total += size
        if size > 16777216 or total > 134217728 or name_size > 256:
            _fail("package_limit_exceeded")
        region(cursor + 46, name_size + extra_size + comment_size)
        raw_name = region(cursor + 46, name_size)
        try:
            name = raw_name.decode("utf-8")
        except UnicodeError:
            _fail("package_zip_unsafe")
        directory = name.endswith("/")
        path = name[:-1] if directory else name
        if (
            name.startswith("\ufeff")
            or (not flags & 0x800 and not raw_name.isascii())
            or unicodedata.normalize("NFC", name) != name
            or not path
            or re.match(r"[A-Za-z]:", path)
            or "\\" in path
            or "\0" in path
            or any(p in {"", ".", ".."} for p in path.split("/"))
            or path in names
        ):
            _fail("package_zip_unsafe")
        for other, is_directory in names.items():
            if (
                path.startswith(other + "/")
                and not is_directory
                or other.startswith(path + "/")
                and not directory
            ):
                _fail("package_zip_unsafe")
        names[path] = directory
        kind = (attributes >> 16) & 0xF000
        if (
            kind not in {0, 0x8000, 0x4000}
            or kind == 0x8000
            and directory
            or kind == 0x4000
            and not directory
            or bool(attributes & 0x10) != directory
            or directory
            and size != 0
        ):
            _fail("package_zip_unsafe")
        extra(cursor + 46 + name_size, extra_size)
        region(local, 30)
        if (
            u32(local) != 0x04034B50
            or u16(local + 4) != u16(cursor + 6)
            or u16(cursor + 6) > 20
            or u16(local + 6) != flags
            or u16(local + 8) != method
            or u16(local + 26) != name_size
            or region(local + 30, name_size) != raw_name
        ):
            _fail()
        local_extra = u16(local + 28)
        extra(local + 30 + name_size, local_extra)
        start = local + 30 + name_size + local_extra
        encoded = region(start, compressed)
        actual = [u32(local + 14), u32(local + 18), u32(local + 22)]
        wanted = [crc, compressed, size]
        if actual != wanted and (not flags & 8 or actual != [0, 0, 0]):
            _fail()
        finish = start + compressed
        if flags & 8:
            matches: list[int] = []
            for signature in (False, True):
                d = finish + (4 if signature else 0)
                if (
                    d + 12 <= central
                    and (not signature or u32(finish) == 0x08074B50)
                    and [u32(d + i * 4) for i in range(3)] == wanted
                ):
                    matches.append(d + 12)
            if len(matches) != 1:
                _fail()
            finish = matches[0]
        spans.append((local, finish))
        try:
            if method == 8:
                inflater = zlib.decompressobj(-15)
                data = inflater.decompress(encoded, size + 1)
                if not inflater.eof or inflater.unused_data or inflater.unconsumed_tail:
                    _fail()
            else:
                data = encoded
        except zlib.error:
            _fail()
        if len(data) != size or zlib.crc32(data) != crc:
            _fail()
        files[path] = PackageMember(
            content=data,
            executable=bool((attributes >> 16) & 0o111),
            directory=directory,
        )
        cursor += 46 + name_size + extra_size + comment_size
    if cursor != end:
        _fail()
    position = 0
    for start, finish in sorted(spans):
        if start != position:
            _fail()
        position = finish
    if position != central:
        _fail()
    return MappingProxyType(files)

"""What bytes a stored file holds, as stated by the backend that owns it."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import ClassVar

from metaclass_registry import AutoRegisterMeta

HASH_CHUNK_BYTES = 1024 * 1024
SHA256_HEX_LENGTH = 64
LOWERCASE_HEX_DIGITS = frozenset("0123456789abcdef")


class FileContent(metaclass=AutoRegisterMeta):
    """Either the hash and size of a file's bytes, or the reason there is none.

    The two answers are registered under ``content_kind``; a stored answer names
    its class by that key.
    """

    __registry_key__ = "content_kind"
    __slots__ = ()

    content_kind: ClassVar[str | None] = None


@dataclass(frozen=True, slots=True)
class HashedFileContent(FileContent):
    """The SHA-256 and byte count of one physical file."""

    content_kind: ClassVar[str] = "hashed"

    sha256: str
    size_bytes: int

    def __post_init__(self) -> None:
        if (
            not isinstance(self.sha256, str)
            or len(self.sha256) != SHA256_HEX_LENGTH
            or not LOWERCASE_HEX_DIGITS.issuperset(self.sha256)
        ):
            raise ValueError(
                f"sha256 must be {SHA256_HEX_LENGTH} lowercase hex digits: {self.sha256!r}."
            )
        if (
            not isinstance(self.size_bytes, int)
            or isinstance(self.size_bytes, bool)
            or self.size_bytes < 0
        ):
            raise ValueError(f"size_bytes must be a nonnegative integer: {self.size_bytes!r}.")

    @classmethod
    def of_physical_file(cls, path: Path) -> HashedFileContent:
        """Hash a file chunk by chunk, counting the bytes that were hashed."""

        digest = hashlib.sha256()
        size_bytes = 0
        with Path(path).open("rb") as stream:
            while chunk := stream.read(HASH_CHUNK_BYTES):
                digest.update(chunk)
                size_bytes += len(chunk)
        return cls(sha256=digest.hexdigest(), size_bytes=size_bytes)


@dataclass(frozen=True, slots=True)
class UnhashedFileContent(FileContent):
    """Why a backend cannot state the bytes of what a path names."""

    content_kind: ClassVar[str] = "not_hashed"

    reason: str

    def __post_init__(self) -> None:
        if not isinstance(self.reason, str) or not self.reason.strip():
            raise ValueError("An unhashed file content answer must give a reason.")

"""What bytes a stored file holds, as stated by the backend that owns it."""

from __future__ import annotations

import hashlib
from abc import ABC, abstractmethod
from collections.abc import Mapping
from dataclasses import dataclass, fields
from pathlib import Path
from typing import ClassVar

HASH_CHUNK_BYTES = 1024 * 1024
SHA256_HEX_LENGTH = 64
LOWERCASE_HEX_DIGITS = frozenset("0123456789abcdef")


class FileContent(ABC):
    """Either the hash and size of a file's bytes, or the reason there is none."""

    __slots__ = ()

    hashed: ClassVar[bool]

    _kind_by_hashed: ClassVar[dict[bool, type[FileContent]]] = {}

    def __init_subclass__(cls, **kwargs: object) -> None:
        super().__init_subclass__(**kwargs)
        if "hashed" in vars(cls):
            FileContent._kind_by_hashed[cls.hashed] = cls

    @abstractmethod
    def to_record(self) -> dict[str, object]:
        """Return this answer as a JSON-native record."""

    @classmethod
    def from_record(cls, record: Mapping[str, object]) -> FileContent:
        """Rebuild the answer whose kind the record's ``hashed`` boolean states."""

        values = dict(record)
        hashed = values.pop("hashed", None)
        if not isinstance(hashed, bool):
            raise ValueError(f"File content record must state a boolean 'hashed': {record!r}.")
        kind = FileContent._kind_by_hashed[hashed]
        expected = {field.name for field in fields(kind)}
        if set(values) != expected:
            raise ValueError(
                f"{kind.__name__} record must hold exactly {sorted(expected)}: {record!r}."
            )
        return kind(**values)


@dataclass(frozen=True, slots=True)
class HashedFileContent(FileContent):
    """The SHA-256 and byte count of one physical file."""

    hashed: ClassVar[bool] = True

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

    def to_record(self) -> dict[str, object]:
        return {"hashed": True, "sha256": self.sha256, "size_bytes": self.size_bytes}


@dataclass(frozen=True, slots=True)
class UnhashedFileContent(FileContent):
    """Why a backend cannot state the bytes of what a path names."""

    hashed: ClassVar[bool] = False

    reason: str

    def __post_init__(self) -> None:
        if not isinstance(self.reason, str) or not self.reason.strip():
            raise ValueError("An unhashed file content answer must give a reason.")

    def to_record(self) -> dict[str, object]:
        return {"hashed": False, "reason": self.reason}

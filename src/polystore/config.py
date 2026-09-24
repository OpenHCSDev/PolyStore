"""Configuration owners for PolyStore storage backends."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import ClassVar

from metaclass_registry import AutoRegisterMeta
from numcodecs import LZ4, Blosc, Zlib, Zstd
from numcodecs.abc import Codec

from .formats import FileFormat


class ZarrCompressor(Enum):
    """Compression algorithms supported by :class:`ZarrStorageBackend`."""

    BLOSC = "blosc"
    ZLIB = "zlib"
    LZ4 = "lz4"
    ZSTD = "zstd"
    NONE = "none"


class ZarrCompressorFactory(ABC, metaclass=AutoRegisterMeta):
    """Nominal strategy owner for Zarr compressor construction."""

    __registry_key__ = "compressor"
    __skip_if_no_key__ = True
    __registry__: ClassVar[dict[ZarrCompressor, type[ZarrCompressorFactory]]] = {}

    compressor: ClassVar[ZarrCompressor | None] = None

    @abstractmethod
    def create(
        self,
        compression_level: int,
        shuffle: bool = True,
    ) -> Codec | None:
        """Create the codec for this compressor variant."""


class NoZarrCompressorFactory(ZarrCompressorFactory):
    """Factory for disabling Zarr compression."""

    compressor = ZarrCompressor.NONE

    def create(
        self,
        compression_level: int,
        shuffle: bool = True,
    ) -> None:
        return None


class BloscZarrCompressorFactory(ZarrCompressorFactory):
    """Factory for Blosc-backed Zarr compression."""

    compressor = ZarrCompressor.BLOSC

    def create(
        self,
        compression_level: int,
        shuffle: bool = True,
    ) -> Blosc:
        return Blosc(
            cname="lz4",
            clevel=compression_level,
            shuffle=shuffle,
        )


class ZlibZarrCompressorFactory(ZarrCompressorFactory):
    """Factory for zlib-backed Zarr compression."""

    compressor = ZarrCompressor.ZLIB

    def create(
        self,
        compression_level: int,
        shuffle: bool = True,
    ) -> Zlib:
        return Zlib(level=compression_level)


class Lz4ZarrCompressorFactory(ZarrCompressorFactory):
    """Factory for LZ4-backed Zarr compression."""

    compressor = ZarrCompressor.LZ4

    def create(
        self,
        compression_level: int,
        shuffle: bool = True,
    ) -> LZ4:
        return LZ4(acceleration=compression_level)


class ZstdZarrCompressorFactory(ZarrCompressorFactory):
    """Factory for Zstandard-backed Zarr compression."""

    compressor = ZarrCompressor.ZSTD

    def create(
        self,
        compression_level: int,
        shuffle: bool = True,
    ) -> Zstd:
        return Zstd(level=compression_level)


class ZarrChunkStrategy(Enum):
    """Chunking strategies supported by :class:`ZarrStorageBackend`."""

    WELL = "well"
    FILE = "file"


class TiffCompression(Enum):
    """Lossless compression choices for disk-backed TIFF output."""

    NONE = ("none", None)
    DEFLATE = ("deflate", "zlib")

    def __new__(cls, value: str, tifffile_codec: str | None):
        member = object.__new__(cls)
        member._value_ = value
        member.tifffile_codec = tifffile_codec
        return member


@dataclass(frozen=True)
class TiffConfig:
    """TIFF writer settings; defaults preserve the uncompressed file contract."""

    compression: TiffCompression = TiffCompression.NONE
    compression_level: int = 1

    def __post_init__(self) -> None:
        if not isinstance(self.compression, TiffCompression):
            raise TypeError("compression must be a TiffCompression value")
        if isinstance(self.compression_level, bool) or not isinstance(
            self.compression_level, int
        ) or not 1 <= self.compression_level <= 9:
            raise ValueError("compression_level must be an integer from 1 to 9")

    def tifffile_write_kwargs(self) -> dict[str, object]:
        """Return only codec options accepted by tifffile.imwrite."""

        codec = self.compression.tifffile_codec
        if codec is None:
            return {}
        return {
            "compression": codec,
            "compressionargs": {"level": self.compression_level},
        }

    def applies_to_path(self, path: str | Path) -> bool:
        """Whether this configured codec applies to this disk output path."""

        return (
            self.compression is not TiffCompression.NONE
            and Path(path).suffix.lower() in FileFormat.TIFF.extensions
        )


def tiff_write_batches(
    paths: Sequence[str | Path],
    config: TiffConfig | None,
) -> tuple[tuple[tuple[int, ...], TiffConfig | None], ...]:
    """Partition consecutive writes by their exact TIFF codec requirement."""

    if not paths:
        return ()
    if config is None or config.compression is TiffCompression.NONE:
        return ((tuple(range(len(paths))), None),)
    batches: list[tuple[tuple[int, ...], TiffConfig | None]] = []
    start = 0
    active = config.applies_to_path(paths[0])
    for index, path in enumerate(paths[1:], start=1):
        applies = config.applies_to_path(path)
        if applies != active:
            batches.append((tuple(range(start, index)), config if active else None))
            start = index
            active = applies
    batches.append((tuple(range(start, len(paths))), config if active else None))
    return tuple(batches)


@dataclass(frozen=True)
class ZarrConfig:
    """Framework-independent configuration for Zarr storage.

    Args:
        compressor: Compression algorithm used for stored arrays; select
            :attr:`ZarrCompressor.NONE` to disable compression.
        compression_level: Algorithm-specific compression level passed to the
            selected compressor factory.
        chunk_strategy: Store each well as one chunk or split its field,
            channel, and Z planes into file-sized chunks.
    """

    compressor: ZarrCompressor = ZarrCompressor.ZLIB
    compression_level: int = 3
    chunk_strategy: ZarrChunkStrategy = ZarrChunkStrategy.WELL

    @property
    def compressor_factory(self) -> ZarrCompressorFactory:
        """Return the registered factory for the selected compressor."""
        return ZarrCompressorFactory.__registry__[self.compressor]()

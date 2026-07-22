"""Configuration owners for PolyStore storage backends."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Any, ClassVar

from metaclass_registry import AutoRegisterMeta


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
    ) -> Any | None:
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
    ) -> Any:
        import zarr

        return zarr.Blosc(
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
    ) -> Any:
        import zarr

        return zarr.Zlib(level=compression_level)


class Lz4ZarrCompressorFactory(ZarrCompressorFactory):
    """Factory for LZ4-backed Zarr compression."""

    compressor = ZarrCompressor.LZ4

    def create(
        self,
        compression_level: int,
        shuffle: bool = True,
    ) -> Any:
        import zarr

        return zarr.LZ4(acceleration=compression_level)


class ZstdZarrCompressorFactory(ZarrCompressorFactory):
    """Factory for Zstandard-backed Zarr compression."""

    compressor = ZarrCompressor.ZSTD

    def create(
        self,
        compression_level: int,
        shuffle: bool = True,
    ) -> Any:
        import zarr

        return zarr.Zstd(level=compression_level)


class ZarrChunkStrategy(Enum):
    """Chunking strategies supported by :class:`ZarrStorageBackend`."""

    WELL = "well"
    FILE = "file"


@dataclass(frozen=True)
class ZarrConfig:
    """Framework-independent configuration for Zarr storage."""

    compressor: ZarrCompressor = ZarrCompressor.ZLIB
    compression_level: int = 3
    chunk_strategy: ZarrChunkStrategy = ZarrChunkStrategy.WELL

    @property
    def compressor_factory(self) -> ZarrCompressorFactory:
        """Return the registered factory for the selected compressor."""
        return ZarrCompressorFactory.__registry__[self.compressor]()

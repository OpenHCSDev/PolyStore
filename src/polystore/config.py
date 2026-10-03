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


class TiffPhotometric(Enum):
    """Declared TIFF pixel interpretation, independent of array dimensions."""

    MINISBLACK = "minisblack"
    RGB = "rgb"


class TiffPlanarConfig(Enum):
    """Declared storage layout for TIFF sample dimensions."""

    CONTIG = "contig"
    SEPARATE = "separate"


@dataclass(frozen=True)
class TiffConfig:
    """TIFF codec and declared pixel axes; defaults preserve legacy writes.

    Args:
        compression: Lossless disk-TIFF codec. ``NONE`` preserves the existing
            uncompressed output; ``DEFLATE`` compresses TIFF pixels exactly.
        compression_level: Deflate level from 1 to 9. Ignored when compression
            is ``NONE``.
        photometric: Explicit grayscale or RGB pixel interpretation.
        axes: Dimension identities such as ``ZYX`` or ``YXS``. Requires
            photometric; ``S`` declares samples rather than a spatial axis.
        planarconfig: Sample layout. Declared axes select the layout when absent.
    """

    compression: TiffCompression = TiffCompression.NONE
    compression_level: int = 1
    photometric: TiffPhotometric | None = None
    axes: str | None = None
    planarconfig: TiffPlanarConfig | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.compression, TiffCompression):
            raise TypeError("compression must be a TiffCompression value")
        if (
            isinstance(self.compression_level, bool)
            or not isinstance(self.compression_level, int)
            or not 1 <= self.compression_level <= 9
        ):
            raise ValueError("compression_level must be an integer from 1 to 9")
        if self.photometric is not None and not isinstance(self.photometric, TiffPhotometric):
            raise TypeError("photometric must be a TiffPhotometric value")
        if self.planarconfig is not None and not isinstance(self.planarconfig, TiffPlanarConfig):
            raise TypeError("planarconfig must be a TiffPlanarConfig value")
        if self.axes is not None:
            if (
                not isinstance(self.axes, str)
                or not self.axes
                or not self.axes.isascii()
                or not self.axes.isalpha()
                or not self.axes.isupper()
                or len(set(self.axes)) != len(self.axes)
            ):
                raise ValueError("axes must contain distinct uppercase dimension identities")
            if self.photometric is None:
                raise ValueError("axes require an explicit photometric interpretation")
            sample_axis = self.axes.find("S")
            if sample_axis >= 0:
                if sample_axis == len(self.axes) - 1:
                    layout = TiffPlanarConfig.CONTIG
                elif sample_axis == len(self.axes) - 3:
                    layout = TiffPlanarConfig.SEPARATE
                else:
                    raise ValueError("TIFF sample axis must be last or immediately before YX")
                if self.planarconfig is not None and self.planarconfig is not layout:
                    raise ValueError("planarconfig disagrees with the declared sample axis")
                object.__setattr__(self, "planarconfig", layout)
            elif self.photometric is TiffPhotometric.RGB or self.planarconfig is not None:
                raise ValueError("RGB and planar TIFF layouts require a declared S sample axis")

    def validate_shape(self, shape: Sequence[int]) -> None:
        """Reject a declared axis roster that cannot describe the actual pixels."""
        if self.axes is not None and len(self.axes) != len(shape):
            raise ValueError(
                f"Declared TIFF axes {self.axes!r} do not match pixel shape {tuple(shape)!r}"
            )

    def tifffile_write_kwargs(self) -> dict[str, object]:
        """Encode codec options and declared pixel semantics for tifffile.imwrite."""
        options: dict[str, object] = {}
        codec = self.compression.tifffile_codec
        if codec is not None:
            options.update(compression=codec, compressionargs={"level": self.compression_level})
        if self.photometric is not None:
            options["photometric"] = self.photometric.value
        if self.axes is not None:
            options["metadata"] = {"axes": self.axes}
        if self.planarconfig is not None:
            options["planarconfig"] = self.planarconfig.value
        return options

    def applies_to_path(self, path: str | Path) -> bool:
        """Whether these configured TIFF options apply to this disk path."""

        return (
            bool(self.tifffile_write_kwargs())
            and Path(path).suffix.lower() in FileFormat.TIFF.extensions
        )


def tiff_write_batches(
    paths: Sequence[str | Path],
    config: TiffConfig | None,
) -> tuple[tuple[tuple[int, ...], TiffConfig | None], ...]:
    """Partition consecutive writes by their exact TIFF configuration."""

    if not paths:
        return ()
    if config is None or not config.tifffile_write_kwargs():
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

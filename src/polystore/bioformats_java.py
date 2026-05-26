"""Shared Java Bio-Formats bridge for metadata discovery and plane loading."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from threading import Lock
from typing import Any, Callable

import numpy as np


class BioFormatsJavaUnavailableError(RuntimeError):
    """Raised when the Java Bio-Formats runtime cannot be initialized."""


@dataclass(frozen=True, slots=True)
class BioFormatsOpenedReader:
    """Open Bio-Formats reader plus its OME metadata store."""

    reader: Any
    metadata: Any

    def close(self) -> None:
        self.reader.close()


class BioFormatsJavaContext:
    """Lazy JVM/ImageJ context for Bio-Formats Java access."""

    _lock = Lock()
    _instance: "BioFormatsJavaContext | None" = None

    def __init__(self, imagej_module: Any, scyjava_module: Any):
        self.imagej = imagej_module
        self.scyjava = scyjava_module
        self.ij = None
        self.ImageReader = None
        self.MetadataTools = None
        self.FormatTools = None

    @classmethod
    def instance(cls) -> "BioFormatsJavaContext":
        with cls._lock:
            if cls._instance is None:
                cls._instance = cls._create()
            return cls._instance

    @classmethod
    def _create(cls) -> "BioFormatsJavaContext":
        try:
            import imagej
            import scyjava
        except ImportError as exc:
            raise BioFormatsJavaUnavailableError(
                "Bio-Formats support requires the optional bioformats/fiji dependencies."
            ) from exc
        return cls(imagej, scyjava)

    def ensure_initialized(self) -> None:
        if self.ij is not None:
            return
        try:
            self.ij = self.imagej.init("sc.fiji:fiji", mode="headless")
            self.ImageReader = self.scyjava.jimport("loci.formats.ImageReader")
            self.MetadataTools = self.scyjava.jimport("loci.formats.MetadataTools")
            self.FormatTools = self.scyjava.jimport("loci.formats.FormatTools")
        except Exception as exc:
            raise BioFormatsJavaUnavailableError(
                "Could not initialize Fiji/Bio-Formats through pyimagej."
            ) from exc

    def open_reader(self, source_path: str | Path) -> BioFormatsOpenedReader:
        self.ensure_initialized()
        metadata = self.MetadataTools.createOMEXMLMetadata()
        reader = self.ImageReader()
        try:
            reader.setMetadataStore(metadata)
            reader.setId(str(source_path))
            return BioFormatsOpenedReader(reader=reader, metadata=metadata)
        except Exception:
            reader.close()
            raise


def java_int(value: Any) -> int | None:
    """Convert nullable Java primitive wrappers to Python int."""
    return OptionalJavaScalar.from_java(value, JAVA_SCALAR_PROJECTOR.readers).convert(int)


def java_float(value: Any) -> float | None:
    """Convert nullable Java numeric wrappers to Python float."""
    return OptionalJavaScalar.from_java(value, JAVA_SCALAR_PROJECTOR.readers).convert(float)


def java_str(value: Any) -> str | None:
    """Convert nullable Java strings to Python strings."""
    if value is None:
        return None
    return str(value)


def _read_java_value(value: Any) -> Any:
    return value.value()


def _read_java_get_value(value: Any) -> Any:
    return value.getValue()


@dataclass(frozen=True, slots=True)
class JavaScalarProjector:
    """Project nullable Java scalar wrappers to Python scalar values."""

    readers: tuple[Callable[[Any], Any], ...]

    def unwrap(self, value: Any) -> Any:
        for reader in self.readers:
            try:
                return reader(value)
            except AttributeError:
                continue
        return value


@dataclass(frozen=True, slots=True)
class OptionalJavaScalar:
    """Nullable Java scalar after wrapper unwrapping."""

    value: Any | None

    @classmethod
    def from_java(
        cls,
        value: Any,
        readers: tuple[Callable[[Any], Any], ...],
    ) -> "OptionalJavaScalar":
        if value is None:
            return cls(None)
        return cls(JavaScalarProjector(readers).unwrap(value))

    def convert(self, converter: Callable[[Any], Any]) -> Any | None:
        if self.value is None:
            return None
        return converter(self.value)


JAVA_SCALAR_PROJECTOR = JavaScalarProjector(
    readers=(
        _read_java_value,
        _read_java_get_value,
    )
)


def load_bioformats_plane(
    *,
    source_path: Path,
    series_index: int,
    plane_index: int,
) -> np.ndarray:
    """Load a single 2D Bio-Formats plane through the Java ImageReader."""
    context = BioFormatsJavaContext.instance()
    opened = context.open_reader(source_path)
    reader = opened.reader
    try:
        reader.setSeries(series_index)
        if reader.getRGBChannelCount() != 1:
            raise ValueError(
                "Bio-Formats RGB/interleaved planes are not yet representable as "
                "OpenHCS scalar channel planes."
            )
        raw = bytes(reader.openBytes(plane_index))
        dtype = PixelDtypeCatalog.from_format_tools(context.FormatTools).dtype(
            pixel_type=int(reader.getPixelType()),
            little_endian=bool(reader.isLittleEndian()),
        )
        array = np.frombuffer(raw, dtype=dtype)
        return array.reshape((int(reader.getSizeY()), int(reader.getSizeX())))
    finally:
        opened.close()


@dataclass(frozen=True, slots=True)
class PixelDtypeSpec:
    """NumPy dtype projection for one Bio-Formats pixel type."""

    key: int
    dtype_code: str
    endian_sensitive: bool = True

    def dtype(self, *, little_endian: bool) -> np.dtype:
        if not self.endian_sensitive:
            return np.dtype(self.dtype_code)
        endian = "<" if little_endian else ">"
        return np.dtype(endian + self.dtype_code)


@dataclass(frozen=True, slots=True)
class PixelDtypeCatalog:
    """Authoritative Bio-Formats pixel-type to NumPy dtype mapping."""

    specs_by_key: dict[int, PixelDtypeSpec]

    @classmethod
    def from_format_tools(cls, format_tools: Any) -> "PixelDtypeCatalog":
        specs = (
            PixelDtypeSpec(int(format_tools.INT8), "i1", endian_sensitive=False),
            PixelDtypeSpec(int(format_tools.UINT8), "u1", endian_sensitive=False),
            PixelDtypeSpec(int(format_tools.INT16), "i2"),
            PixelDtypeSpec(int(format_tools.UINT16), "u2"),
            PixelDtypeSpec(int(format_tools.INT32), "i4"),
            PixelDtypeSpec(int(format_tools.UINT32), "u4"),
            PixelDtypeSpec(int(format_tools.FLOAT), "f4"),
            PixelDtypeSpec(int(format_tools.DOUBLE), "f8"),
        )
        return cls({spec.key: spec for spec in specs})

    def dtype(self, *, pixel_type: int, little_endian: bool) -> np.dtype:
        try:
            return self.specs_by_key[pixel_type].dtype(little_endian=little_endian)
        except KeyError as exc:
            raise ValueError(f"Unsupported Bio-Formats pixel type: {pixel_type}") from exc

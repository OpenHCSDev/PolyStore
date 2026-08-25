"""
File format definitions for polystore.

This module defines the supported file formats and their extensions.
"""

import importlib
from enum import Enum
from types import ModuleType


class FileFormatDependency(Enum):
    """Optional runtime required by a declared file format."""

    TORCH = "torch"
    JAX = "jax"
    CUPY = "cupy"
    TENSORFLOW = "tensorflow"
    ZARR = "zarr"
    SCIPY_IO = "scipy.io"
    TIFFFILE = "tifffile"
    IMAGEIO = "imageio.v3"

    def load(self) -> ModuleType:
        """Import this runtime only when a format operation requires it."""
        try:
            return importlib.import_module(self.value)
        except ModuleNotFoundError as exc:
            dependency_root = self.value.partition(".")[0]
            if exc.name != dependency_root:
                raise
            raise ImportError(f"The {self.value!r} package is not installed.") from exc


class FileFormat(Enum):
    """Enumeration of supported file formats."""

    def __new__(
        cls,
        value: str,
        extensions: tuple[str, ...],
        is_pixel_payload: bool,
        is_raster_source: bool = False,
        dependency: FileFormatDependency | None = None,
    ):
        member = object.__new__(cls)
        member._value_ = value
        member.extensions = extensions
        member.is_pixel_payload = is_pixel_payload
        member.is_raster_source = is_raster_source
        member.dependency = dependency
        return member

    def load_dependency(self) -> ModuleType:
        """Load this format's optional runtime at the actual I/O boundary."""
        if self.dependency is None:
            raise TypeError(f"{self.value!r} does not declare an optional dependency")
        return self.dependency.load()

    # Array formats
    NUMPY = ("numpy", (".npy", ".npz"), True)
    TORCH = ("torch", (".pt", ".pth"), True, False, FileFormatDependency.TORCH)
    JAX = ("jax", (".jax",), True, False, FileFormatDependency.JAX)
    CUPY = ("cupy", (".cupy",), True, False, FileFormatDependency.CUPY)
    TENSORFLOW = (
        "tensorflow",
        (".tf",),
        True,
        False,
        FileFormatDependency.TENSORFLOW,
    )
    ZARR = ("zarr", (".zarr",), True, False, FileFormatDependency.ZARR)
    MATLAB = ("matlab", (".mat",), True, False, FileFormatDependency.SCIPY_IO)

    # Image formats
    TIFF = ("tiff", (".tif", ".tiff"), True, True, FileFormatDependency.TIFFFILE)
    PNG = ("png", (".png",), True, True, FileFormatDependency.IMAGEIO)
    RASTER_IMAGE = (
        "raster_image",
        (".bmp", ".gif", ".jpeg", ".jpg"),
        True,
        True,
        FileFormatDependency.IMAGEIO,
    )

    # Data formats
    CSV = ("csv", (".csv",), False)
    JSON = ("json", (".json",), False)
    TEXT = ("text", (".txt",), False)

    # ROI format
    ROI = ("roi", (".roi.zip",), False)


# Default image extensions
DEFAULT_IMAGE_EXTENSIONS = {
    extension
    for file_format in FileFormat
    if file_format.is_raster_source
    for extension in file_format.extensions
}

PIXEL_PAYLOAD_EXTENSIONS = frozenset(
    extension
    for file_format in FileFormat
    if file_format.is_pixel_payload
    for extension in file_format.extensions
)


def get_format_from_extension(ext: str) -> FileFormat:
    """
    Get file format from extension.

    Args:
        ext: File extension (with or without leading dot)

    Returns:
        FileFormat enum value

    Raises:
        ValueError: If extension is not recognized
    """
    if not ext.startswith("."):
        ext = f".{ext}"

    ext = ext.lower()

    for file_format in FileFormat:
        if ext in file_format.extensions:
            return file_format

    raise ValueError(f"Unknown file extension: {ext}")

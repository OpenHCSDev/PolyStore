"""
File format definitions for polystore.

This module defines the supported file formats and their extensions.
"""

from enum import Enum


class FileFormat(Enum):
    """Enumeration of supported file formats."""

    def __new__(
        cls,
        value: str,
        extensions: tuple[str, ...],
        is_pixel_payload: bool,
        is_raster_source: bool = False,
    ):
        member = object.__new__(cls)
        member._value_ = value
        member.extensions = extensions
        member.is_pixel_payload = is_pixel_payload
        member.is_raster_source = is_raster_source
        return member

    # Array formats
    NUMPY = ("numpy", (".npy", ".npz"), True)
    TORCH = ("torch", (".pt", ".pth"), True)
    JAX = ("jax", (".jax",), True)
    CUPY = ("cupy", (".cupy",), True)
    TENSORFLOW = ("tensorflow", (".tf",), True)
    ZARR = ("zarr", (".zarr",), True)
    MATLAB = ("matlab", (".mat",), True)

    # Image formats
    TIFF = ("tiff", (".tif", ".tiff"), True, True)
    PNG = ("png", (".png",), True, True)
    RASTER_IMAGE = (
        "raster_image",
        (".bmp", ".gif", ".jpeg", ".jpg"),
        True,
        True,
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

"""
File format definitions for polystore.

This module defines the supported file formats and their extensions.
"""

from enum import Enum


class FileFormat(Enum):
    """Enumeration of supported file formats."""

    # Array formats
    NUMPY = "numpy"
    TORCH = "torch"
    JAX = "jax"
    CUPY = "cupy"
    TENSORFLOW = "tensorflow"
    ZARR = "zarr"

    # Image formats
    TIFF = "tiff"

    # Data formats
    CSV = "csv"
    JSON = "json"
    TEXT = "text"

    # ROI format
    ROI = "roi"

    @property
    def extensions(self):
        """Get file extensions for this format."""
        return FILE_FORMAT_EXTENSIONS.get(self, [])


# Mapping of file formats to their extensions
FILE_FORMAT_EXTENSIONS = {
    FileFormat.NUMPY: [".npy", ".npz"],
    FileFormat.TORCH: [".pt", ".pth"],
    FileFormat.JAX: [".jax"],
    FileFormat.CUPY: [".cupy"],
    FileFormat.TENSORFLOW: [".tf"],
    FileFormat.ZARR: [".zarr"],
    FileFormat.TIFF: [".tif", ".tiff"],
    FileFormat.CSV: [".csv"],
    FileFormat.JSON: [".json"],
    FileFormat.TEXT: [".txt"],
    FileFormat.ROI: [".roi.zip"],
}

# Default image extensions
DEFAULT_IMAGE_EXTENSIONS = {".tif", ".tiff", ".png", ".jpg", ".jpeg"}


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

    for fmt, extensions in FILE_FORMAT_EXTENSIONS.items():
        if ext in extensions:
            return fmt

    raise ValueError(f"Unknown file extension: {ext}")

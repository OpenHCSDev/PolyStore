"""
Exceptions for polystore.

This module defines exceptions for storage operations and backend management.
"""


class StorageError(Exception):
    """Base exception for storage operations."""
    pass


class StorageResolutionError(StorageError):
    """Raised when a storage key cannot be resolved to a valid path."""
    pass


class BackendNotFoundError(StorageError):
    """Raised when a backend is not found in the registry."""
    pass


class UnsupportedFormatError(StorageError):
    """Raised when a file format is not supported."""
    pass


class ImageLoadError(RuntimeError):
    """Raised when image loading fails."""
    pass


class ImageSaveError(RuntimeError):
    """Raised when image saving fails."""
    pass


class StorageWriteError(RuntimeError):
    """Raised when writing to storage fails."""
    pass


class MetadataNotFoundError(ValueError):
    """Raised when required metadata files cannot be found."""
    pass


class PathMismatchError(ValueError):
    """Raised when a path scheme doesn't match the expected scheme for a backend."""
    pass


class VFSTypeError(TypeError):
    """Raised when a type error occurs in the VFS boundary."""
    pass
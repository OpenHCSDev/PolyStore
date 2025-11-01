"""
Polystore - Framework-agnostic multi-backend storage abstraction.

Provides pluggable storage backends with multi-framework I/O support for
NumPy, PyTorch, JAX, TensorFlow, CuPy, and Zarr.
"""

__version__ = "0.1.0"

# Core abstractions
from .base import (
    DataSink,
    DataSource,
    StorageBackend,
    VirtualBackend,
    ReadOnlyBackend,
)

# Concrete backends
from .memory import MemoryBackend
from .disk import DiskBackend

# Optional backends
try:
    from .zarr import ZarrBackend
except ImportError:
    ZarrBackend = None

# File manager
from .filemanager import FileManager

# Registry
from .backend_registry import BackendRegistry, create_storage_registry

# Atomic operations
from .atomic import atomic_write, atomic_write_json

# Exceptions
from .exceptions import (
    StorageError,
    StorageResolutionError,
    BackendNotFoundError,
    UnsupportedFormatError,
)

# Streaming (optional)
try:
    from .streaming import StreamingBackend
except ImportError:
    StreamingBackend = None

__all__ = [
    # Version
    "__version__",
    # Core abstractions
    "DataSink",
    "DataSource",
    "StorageBackend",
    "VirtualBackend",
    "ReadOnlyBackend",
    # Backends
    "MemoryBackend",
    "DiskBackend",
    "ZarrBackend",
    # File manager
    "FileManager",
    # Registry
    "BackendRegistry",
    # Atomic operations
    "atomic_write",
    "atomic_write_json",
    # Exceptions
    "StorageError",
    "StorageResolutionError",
    "BackendNotFoundError",
    "UnsupportedFormatError",
    # Streaming
    "StreamingBackend",
]


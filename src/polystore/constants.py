"""
Polystore constants.

Minimal, self-contained enum and constant definitions used by polystore backends.
No external dependencies or framework-specific imports.
"""

from enum import Enum


class Backend(Enum):
    """Storage backend type identifiers."""
    AUTO = "auto"
    DISK = "disk"
    MEMORY = "memory"
    ZARR = "zarr"
    OME_ZARR = "ome_zarr"
    STREAMING = "streaming"
    NAPARI_STREAM = "napari_stream"
    FIJI_STREAM = "fiji_stream"
    OMERO_LOCAL = "omero_local"
    VIRTUAL_WORKSPACE = "virtual_workspace"
    BIOFORMATS = "bioformats"


class TransportMode(Enum):
    """ZeroMQ transport mode (IPC vs TCP)."""
    IPC = "ipc"
    TCP = "tcp"


class MemoryType(Enum):
    """Supported in-memory array types."""
    NUMPY = "numpy"
    CUPY = "cupy"
    TORCH = "torch"
    TENSORFLOW = "tensorflow"
    JAX = "jax"
    PYCLESPERANTO = "pyclesperanto"


# Default backend for operations
DEFAULT_BACKEND = Backend.MEMORY

# Memory type convenience sets
CPU_MEMORY_TYPES = {MemoryType.NUMPY}
GPU_MEMORY_TYPES = {
    MemoryType.CUPY,
    MemoryType.TORCH,
    MemoryType.TENSORFLOW,
    MemoryType.JAX,
    MemoryType.PYCLESPERANTO,
}
SUPPORTED_MEMORY_TYPES = CPU_MEMORY_TYPES | GPU_MEMORY_TYPES

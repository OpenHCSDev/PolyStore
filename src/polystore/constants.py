"""
PolyStore constants and compatibility exports.
"""

from enum import Enum

from arraybridge.types import (
    CPU_MEMORY_TYPES as CPU_MEMORY_TYPES,
)
from arraybridge.types import (
    GPU_MEMORY_TYPES as GPU_MEMORY_TYPES,
)
from arraybridge.types import (
    SUPPORTED_MEMORY_TYPES as SUPPORTED_MEMORY_TYPES,
)
from arraybridge.types import (
    MemoryType as MemoryType,
)


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


# Default backend for operations
DEFAULT_BACKEND = Backend.MEMORY

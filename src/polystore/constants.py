"""
Polystore constants extracted from OpenHCS.

Minimal, self-contained enum and constant definitions used by polystore backends.
No external dependencies or openhcs imports.
"""

from enum import Enum


class Backend(Enum):
    """Storage backend type identifiers."""
    DISK = "disk"
    MEMORY = "memory"
    ZARR = "zarr"
    STREAMING = "streaming"


class TransportMode(Enum):
    """ZeroMQ transport mode (IPC vs TCP)."""
    IPC = "ipc"
    TCP = "tcp"


# Default backend for operations
DEFAULT_BACKEND = Backend.MEMORY

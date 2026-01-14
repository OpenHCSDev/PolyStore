"""
Polystore package exports.
"""

import os

from .atomic import file_lock, atomic_write_json, atomic_update_json, FileLockError, FileLockTimeoutError
from .backend_registry import (
    get_backend_instance,
    cleanup_backend_connections,
    cleanup_all_backends,
    register_cleanup_callback,
    STORAGE_BACKENDS,
)
from .base import (
    BackendBase,
    DataSink,
    DataSource,
    ReadOnlyBackend,
    StorageBackend,
    storage_registry,
    reset_memory_backend,
    ensure_storage_registry,
    get_backend,
)
from .constants import Backend, MemoryType, TransportMode
from .disk import DiskStorageBackend
from .filemanager import FileManager
from .formats import FileFormat, DEFAULT_IMAGE_EXTENSIONS
from .memory import MemoryStorageBackend
from .metadata_writer import (
    AtomicMetadataWriter,
    MetadataWriteError,
    METADATA_CONFIG,
    get_metadata_path,
    get_subdirectory_name,
    resolve_subdirectory_path,
)
from .metadata_migration import detect_legacy_format, migrate_legacy_metadata, migrate_plate_metadata
from .roi import (
    ROI,
    PolygonShape,
    PolylineShape,
    MaskShape,
    PointShape,
    EllipseShape,
    extract_rois_from_labeled_mask,
    load_rois_from_json,
    load_rois_from_zip,
    materialize_rois,
)
from .streaming import StreamingBackend
from .streaming_constants import StreamingDataType, NapariShapeType

__all__ = [
    "Backend",
    "MemoryType",
    "TransportMode",
    "FileFormat",
    "DEFAULT_IMAGE_EXTENSIONS",
    "BackendBase",
    "DataSink",
    "DataSource",
    "ReadOnlyBackend",
    "StorageBackend",
    "StreamingBackend",
    "storage_registry",
    "reset_memory_backend",
    "ensure_storage_registry",
    "get_backend",
    "get_backend_instance",
    "cleanup_backend_connections",
    "cleanup_all_backends",
    "register_cleanup_callback",
    "STORAGE_BACKENDS",
    "DiskStorageBackend",
    "MemoryStorageBackend",
    "FileManager",
    "file_lock",
    "atomic_write_json",
    "atomic_update_json",
    "FileLockError",
    "FileLockTimeoutError",
    "AtomicMetadataWriter",
    "MetadataWriteError",
    "METADATA_CONFIG",
    "get_metadata_path",
    "get_subdirectory_name",
    "resolve_subdirectory_path",
    "detect_legacy_format",
    "migrate_legacy_metadata",
    "migrate_plate_metadata",
    "ROI",
    "PolygonShape",
    "PolylineShape",
    "MaskShape",
    "PointShape",
    "EllipseShape",
    "extract_rois_from_labeled_mask",
    "load_rois_from_json",
    "load_rois_from_zip",
    "materialize_rois",
    "StreamingDataType",
    "NapariShapeType",
    "NapariStreamingBackend",
    "FijiStreamingBackend",
    "ZarrStorageBackend",
    "OMEROLocalBackend",
    "OMEROFileFormatRegistry",
]

_LAZY_BACKEND_REGISTRY = {
    "NapariStreamingBackend": ("polystore.napari_stream", "NapariStreamingBackend"),
    "FijiStreamingBackend": ("polystore.fiji_stream", "FijiStreamingBackend"),
    "ZarrStorageBackend": ("polystore.zarr", "ZarrStorageBackend"),
    "OMEROLocalBackend": ("polystore.omero_local", "OMEROLocalBackend"),
    "OMEROFileFormatRegistry": ("polystore.omero_local", "OMEROFileFormatRegistry"),
}


def __getattr__(name):
    """Lazy import of optional/extra backend classes."""
    if name not in _LAZY_BACKEND_REGISTRY:
        raise AttributeError(f"module '{__name__}' has no attribute '{name}'")

    if os.getenv("POLYSTORE_SUBPROCESS_NO_GPU") == "1":
        class PlaceholderBackend:
            pass
        PlaceholderBackend.__name__ = name
        PlaceholderBackend.__qualname__ = name
        return PlaceholderBackend

    module_path, class_name = _LAZY_BACKEND_REGISTRY[name]
    import importlib
    module = importlib.import_module(module_path)
    return getattr(module, class_name)

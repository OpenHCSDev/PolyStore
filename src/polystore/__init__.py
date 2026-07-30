"""
Polystore package exports.
"""

__version__ = "0.1.25"

from .atomic import (
    FileLockError,
    FileLockTimeoutError,
    atomic_update_json,
    atomic_write_json,
    file_lock,
)
from .backend_registry import (
    STORAGE_BACKENDS,
    cleanup_all_backends,
    cleanup_backend_connections,
    get_backend_instance,
    register_cleanup_callback,
)
from .base import (
    BackendBase,
    DataSink,
    DataSource,
    ImageSamplingRequest,
    ImageSamplingResult,
    ImageSamplingStatisticsScope,
    ReadOnlyBackend,
    StorageBackend,
    ensure_storage_registry,
    get_backend,
    reset_memory_backend,
    storage_registry,
)
from .constants import Backend, MemoryType
from .disk import DiskBackend, DiskStorageBackend
from .filemanager import FileManager
from .formats import DEFAULT_IMAGE_EXTENSIONS, FileFormat
from .memory import MemoryBackend, MemoryStorageBackend
from .metadata_migration import (
    detect_legacy_format,
    migrate_legacy_metadata,
    migrate_plate_metadata,
)
from .metadata_writer import (
    METADATA_CONFIG,
    AtomicMetadataWriter,
    MetadataWriteError,
    get_metadata_path,
    get_subdirectory_name,
    resolve_subdirectory_path,
)
from .roi import (
    ROI,
    EllipseShape,
    MaskShape,
    PointShape,
    PolygonShape,
    PolylineShape,
    extract_rois_from_labeled_mask,
    load_rois_from_json,
    load_rois_from_zip,
    materialize_rois,
)
from .streaming import StreamingBackend
from .streaming_constants import NapariShapeType, StreamingDataType
from .virtual_workspace import SourcePixelRef

__all__ = [
    "Backend",
    "MemoryType",
    "FileFormat",
    "DEFAULT_IMAGE_EXTENSIONS",
    "BackendBase",
    "DataSink",
    "DataSource",
    "ImageSamplingRequest",
    "ImageSamplingResult",
    "ImageSamplingStatisticsScope",
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
    "DiskBackend",
    "MemoryStorageBackend",
    "MemoryBackend",
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
    "SourcePixelRef",
]

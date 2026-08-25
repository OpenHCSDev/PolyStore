"""
Polystore package exports.
"""

from importlib.metadata import version as _distribution_version
from typing import TYPE_CHECKING

__version__ = _distribution_version("polystore")

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
from .filemanager import FileManager
from .formats import DEFAULT_IMAGE_EXTENSIONS, FileFormat
from .imagej_distribution import (
    FIJI_IMAGEJ_DISTRIBUTION,
    FijiArchiveDistribution,
    FijiBundleAsset,
    ImageJArchiveDownloadPolicy,
    ImageJDistributionABC,
    ImageJDistributionUnavailableError,
    ImageJRuntimeArchive,
    ImageJRuntimeLaunch,
    ImageJRuntimeOverlay,
)
from .imagej_runtime import (
    FIJI_IMAGEJ_RUNTIME,
    ImageJRuntimePolicy,
    ImageJRuntimeUnavailableError,
)
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
from .omero_address import (
    OMEROPlaneAddress,
    OMEROPlaneAxis,
    OMEROPlaneCoordinates,
    OMEROWellAddress,
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

if TYPE_CHECKING:
    from .disk import DiskBackend, DiskStorageBackend


def __getattr__(name: str):
    """Load disk implementations only when their public exports are requested."""

    if name not in {"DiskBackend", "DiskStorageBackend"}:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    from .disk import DiskBackend, DiskStorageBackend

    globals().update(
        DiskBackend=DiskBackend,
        DiskStorageBackend=DiskStorageBackend,
    )
    return globals()[name]


__all__ = [
    "Backend",
    "MemoryType",
    "FileFormat",
    "ImageJRuntimePolicy",
    "ImageJRuntimeUnavailableError",
    "FIJI_IMAGEJ_RUNTIME",
    "ImageJArchiveDownloadPolicy",
    "ImageJDistributionABC",
    "ImageJDistributionUnavailableError",
    "ImageJRuntimeArchive",
    "ImageJRuntimeLaunch",
    "ImageJRuntimeOverlay",
    "FijiArchiveDistribution",
    "FijiBundleAsset",
    "FIJI_IMAGEJ_DISTRIBUTION",
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
    "OMEROPlaneAddress",
    "OMEROPlaneAxis",
    "OMEROPlaneCoordinates",
    "OMEROWellAddress",
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

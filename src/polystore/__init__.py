"""
Polystore package exports.

Package-root imports stay lightweight: submodule exports load on first
attribute access instead of at import time. Declaration-only consumers
(configuration modules, DTO layers) therefore do not pay the heavy
backend/streaming/ROI import cost.
"""

from importlib.metadata import version as _distribution_version

__version__ = _distribution_version("polystore")

_LAZY_EXPORTS: dict[str, str] = {
    "FileLockError": ".atomic",
    "FileLockTimeoutError": ".atomic",
    "atomic_update_json": ".atomic",
    "atomic_write_json": ".atomic",
    "file_lock": ".atomic",
    "STORAGE_BACKENDS": ".backend_registry",
    "cleanup_all_backends": ".backend_registry",
    "cleanup_backend_connections": ".backend_registry",
    "get_backend_instance": ".backend_registry",
    "register_cleanup_callback": ".backend_registry",
    "BackendBase": ".base",
    "DataSink": ".base",
    "DataSource": ".base",
    "ImageSamplingRequest": ".base",
    "ImageSamplingResult": ".base",
    "ImageSamplingStatisticsScope": ".base",
    "ReadOnlyBackend": ".base",
    "StorageBackend": ".base",
    "ensure_storage_registry": ".base",
    "get_backend": ".base",
    "reset_memory_backend": ".base",
    "storage_registry": ".base",
    "Backend": ".constants",
    "MemoryType": ".constants",
    "FileManager": ".filemanager",
    "DEFAULT_IMAGE_EXTENSIONS": ".formats",
    "FileFormat": ".formats",
    "FIJI_IMAGEJ_DISTRIBUTION": ".imagej_distribution",
    "FijiArchiveDistribution": ".imagej_distribution",
    "FijiBundleAsset": ".imagej_distribution",
    "ImageJArchiveDownloadPolicy": ".imagej_distribution",
    "ImageJDistributionABC": ".imagej_distribution",
    "ImageJDistributionUnavailableError": ".imagej_distribution",
    "ImageJRuntimeArchive": ".imagej_distribution",
    "ImageJRuntimeLaunch": ".imagej_distribution",
    "ImageJRuntimeOverlay": ".imagej_distribution",
    "FIJI_IMAGEJ_RUNTIME": ".imagej_runtime",
    "ImageJRuntimePolicy": ".imagej_runtime",
    "ImageJRuntimeUnavailableError": ".imagej_runtime",
    "MemoryBackend": ".memory",
    "MemoryStorageBackend": ".memory",
    "detect_legacy_format": ".metadata_migration",
    "migrate_legacy_metadata": ".metadata_migration",
    "migrate_plate_metadata": ".metadata_migration",
    "METADATA_CONFIG": ".metadata_writer",
    "AtomicMetadataWriter": ".metadata_writer",
    "MetadataWriteError": ".metadata_writer",
    "get_metadata_path": ".metadata_writer",
    "get_subdirectory_name": ".metadata_writer",
    "resolve_subdirectory_path": ".metadata_writer",
    "OMEROAddressComponent": ".omero_address",
    "OMEROPlaneAddress": ".omero_address",
    "OMEROPlaneFilenameTemplate": ".omero_address",
    "OMEROWellAddress": ".omero_address",
    "ROI": ".roi",
    "EllipseShape": ".roi",
    "MaskShape": ".roi",
    "PointShape": ".roi",
    "PolygonShape": ".roi",
    "PolylineShape": ".roi",
    "extract_rois_from_labeled_mask": ".roi",
    "load_rois_from_json": ".roi",
    "load_rois_from_zip": ".roi",
    "materialize_rois": ".roi",
    "StreamingBackend": ".streaming",
    "NapariShapeType": ".streaming_constants",
    "StreamingDataType": ".streaming_constants",
    "SourcePixelRef": ".virtual_workspace",
    "DiskBackend": ".disk",
    "DiskStorageBackend": ".disk",
}

__all__ = list(_LAZY_EXPORTS)


def __getattr__(name: str):
    module_name = _LAZY_EXPORTS.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    import importlib

    value = getattr(importlib.import_module(module_name, __name__), name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(_LAZY_EXPORTS))

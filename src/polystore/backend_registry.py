"""
Storage backend metaclass registration system.

Eliminates hardcoded backend registration by using metaclass auto-registration.
Backends are automatically discovered and registered when their classes are defined.
"""

import logging
from collections.abc import Callable

from .base import BackendBase, DataSink

logger = logging.getLogger(__name__)

_backend_instances: dict[str, DataSink] = {}
_cleanup_callbacks: list[Callable[[], None]] = []

# Registry auto-created by AutoRegisterMeta on BackendBase
# Includes both StorageBackend (read-write) and ReadOnlyBackend (read-only) subclasses
STORAGE_BACKENDS = BackendBase.__registry__


def get_backend_instance(backend_type: str) -> DataSink:
    """
    Get backend instance by type with lazy instantiation.

    Args:
        backend_type: Backend type identifier (e.g., 'disk', 'memory')

    Returns:
        Backend instance

    Raises:
        KeyError: If backend type not registered
        RuntimeError: If backend instantiation fails
    """
    if hasattr(backend_type, "value"):
        backend_type = backend_type.value
    backend_type = str(backend_type).lower()

    # Return cached instance if available
    if backend_type in _backend_instances:
        return _backend_instances[backend_type]

    # Get backend class from registry
    if backend_type not in STORAGE_BACKENDS:
        raise KeyError(
            f"Backend type '{backend_type}' not registered. "
            f"Available backends: {list(STORAGE_BACKENDS.keys())}"
        )

    backend_class = STORAGE_BACKENDS[backend_type]

    try:
        # Create and cache instance
        instance = backend_class()
        _backend_instances[backend_type] = instance
        logger.debug(f"Created instance for backend '{backend_type}'")
        return instance
    except Exception as e:
        raise RuntimeError(f"Failed to instantiate backend '{backend_type}': {e}") from e


def create_storage_registry() -> dict[str, DataSink]:
    """
    Create storage registry with all registered backends.

    Returns:
        Dictionary mapping backend types to instances
    """
    # Trigger discovery of all backends in polystore package
    # This imports all backend modules (disk, memory, zarr, napari_stream, fiji_stream, etc.)
    # and registers them via metaclass
    STORAGE_BACKENDS._discover()
    logger.debug("Triggered backend discovery via LazyDiscoveryDict")

    # Backends that require context-specific initialization (e.g., plate_root)
    # These are registered lazily when needed, not at startup
    SKIP_BACKENDS = {"virtual_workspace", "omero_local", "bioformats"}

    registry = {}
    for backend_type in STORAGE_BACKENDS.keys():
        # Skip backends that need context-specific initialization
        if backend_type in SKIP_BACKENDS:
            logger.debug(
                f"Skipping backend '{backend_type}' - requires context-specific initialization"
            )
            continue

        try:
            registry[backend_type] = get_backend_instance(backend_type)
        except Exception as e:
            logger.warning(f"Failed to create instance for backend '{backend_type}': {e}")
            continue

    logger.info(f"Created storage registry with {len(registry)} backends: {list(registry.keys())}")
    return registry


def register_cleanup_callback(callback: Callable[[], None]) -> None:
    """Register one process-resource cleanup callback exactly once."""

    if callback not in _cleanup_callbacks:
        _cleanup_callbacks.append(callback)


def _cleanup_registered_process_resources() -> None:
    """Run every declared process-resource cleanup callback."""

    failures: list[Exception] = []
    for callback in tuple(_cleanup_callbacks):
        try:
            callback()
        except Exception as exc:
            logger.warning("Process-resource cleanup callback failed: %s", exc)
            failures.append(exc)
    if failures:
        raise ExceptionGroup("Process-resource cleanup failed", failures)


def cleanup_backend_connections(*, include_process_resources: bool = False) -> None:
    """
    Clean up backend connections without affecting persistent resources.

    For napari streaming backend, this cleans up ZeroMQ connections but
    leaves the napari window open for future use unless process-resource
    cleanup is explicitly requested.
    """
    for backend_type, instance in _backend_instances.items():
        # Use targeted cleanup for napari streaming to preserve window
        if hasattr(instance, "cleanup_connections"):
            try:
                instance.cleanup_connections()
                logger.debug(f"Cleaned up connections for backend '{backend_type}'")
            except Exception as e:
                logger.warning(f"Failed to cleanup connections for backend '{backend_type}': {e}")
        elif hasattr(instance, "cleanup") and backend_type != "napari_stream":
            try:
                instance.cleanup()
                logger.debug(f"Cleaned up backend '{backend_type}'")
            except Exception as e:
                logger.warning(f"Failed to cleanup backend '{backend_type}': {e}")

    if include_process_resources:
        _cleanup_registered_process_resources()

    cleanup_scope = (
        "including process resources" if include_process_resources else "viewer windows preserved"
    )
    logger.info("Backend connections cleaned up (%s)", cleanup_scope)


def cleanup_all_backends() -> None:
    """
    Clean up all cached backend instances completely.

    This is for full shutdown - clears instance cache and calls full cleanup.
    Use cleanup_backend_connections() for test cleanup to preserve napari window.
    """
    for backend_type, instance in _backend_instances.items():
        if hasattr(instance, "cleanup"):
            try:
                instance.cleanup()
                logger.debug(f"Cleaned up backend '{backend_type}'")
            except Exception as e:
                logger.warning(f"Failed to cleanup backend '{backend_type}': {e}")

    try:
        _cleanup_registered_process_resources()
    finally:
        _backend_instances.clear()
    logger.info("All backend instances and process resources cleaned up")

"""
Storage backend metaclass registration system.

Backends are automatically discovered and registered when their classes are defined.
"""

import logging
from typing import Dict
from .base import BackendBase, DataSink

logger = logging.getLogger(__name__)

_backend_instances: Dict[str, DataSink] = {}


def _get_storage_backends() -> Dict:
    """Get the storage backends registry, ensuring it's initialized."""
    # Import backends to trigger registration
    from . import memory, disk
    try:
        from . import zarr
    except ImportError:
        pass  # Zarr not available
    
    # Registry auto-created by AutoRegisterMeta on BackendBase
    return BackendBase.__registry__


# Lazy access to registry
STORAGE_BACKENDS = None


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
    backend_type = backend_type.lower()

    # Return cached instance if available
    if backend_type in _backend_instances:
        return _backend_instances[backend_type]

    # Get backend class from registry
    storage_backends = _get_storage_backends()
    if backend_type not in storage_backends:
        raise KeyError(f"Backend type '{backend_type}' not registered. "
                      f"Available backends: {list(storage_backends.keys())}")

    backend_class = storage_backends[backend_type]

    try:
        # Create and cache instance
        instance = backend_class()
        _backend_instances[backend_type] = instance
        logger.debug(f"Created instance for backend '{backend_type}'")
        return instance
    except Exception as e:
        raise RuntimeError(f"Failed to instantiate backend '{backend_type}': {e}") from e


def create_storage_registry() -> Dict[str, DataSink]:
    """
    Create storage registry with all registered backends.

    Returns:
        Dictionary mapping backend types to instances
    """
    # Get backends registry (triggers import and registration)
    storage_backends = _get_storage_backends()

    # Backends that require context-specific initialization (e.g., plate_root)
    # These are registered lazily when needed, not at startup
    SKIP_BACKENDS = {'virtual_workspace'}

    registry = {}
    for backend_type in storage_backends.keys():
        # Skip backends that need context-specific initialization
        if backend_type in SKIP_BACKENDS:
            logger.debug(f"Skipping backend '{backend_type}' - requires context-specific initialization")
            continue

        try:
            registry[backend_type] = get_backend_instance(backend_type)
        except Exception as e:
            logger.warning(f"Failed to create instance for backend '{backend_type}': {e}")
            continue

    logger.info(f"Created storage registry with {len(registry)} backends: {list(registry.keys())}")
    return registry


def cleanup_backend_connections() -> None:
    """
    Clean up backend connections without affecting persistent resources.

    For napari streaming backend, this cleans up ZeroMQ connections but
    leaves the napari window open for future use.
    """
    import os

    # Check if we're running in test mode
    is_test_mode = (
        'pytest' in os.environ.get('_', '') or
        'PYTEST_CURRENT_TEST' in os.environ or
        any('pytest' in arg for arg in __import__('sys').argv)
    )

    for backend_type, instance in _backend_instances.items():
        # Use targeted cleanup for napari streaming to preserve window
        if hasattr(instance, 'cleanup_connections'):
            try:
                instance.cleanup_connections()
                logger.debug(f"Cleaned up connections for backend '{backend_type}'")
            except Exception as e:
                logger.warning(f"Failed to cleanup connections for backend '{backend_type}': {e}")
        elif hasattr(instance, 'cleanup') and backend_type != 'napari_stream':
            try:
                instance.cleanup()
                logger.debug(f"Cleaned up backend '{backend_type}'")
            except Exception as e:
                logger.warning(f"Failed to cleanup backend '{backend_type}': {e}")

    # In test mode, also stop viewer processes to allow pytest to exit
    if is_test_mode:
        try:
            from openhcs.runtime.napari_stream_visualizer import _cleanup_global_viewer
            _cleanup_global_viewer()
            logger.debug("Cleaned up napari viewer for test mode")
        except ImportError:
            pass  # napari not available
        except Exception as e:
            logger.warning(f"Failed to cleanup napari viewer: {e}")


class BackendRegistry(dict):
    """
    Registry for storage backends.

    This is a dictionary that automatically populates with available backends
    when first accessed.
    """

    def __init__(self):
        """Initialize the backend registry."""
        super().__init__()
        # Populate with available backends
        self.update(create_storage_registry())


def cleanup_all_backends() -> None:
    """
    Clean up all cached backend instances completely.

    This is for full shutdown - clears instance cache and calls full cleanup.
    Use cleanup_backend_connections() for test cleanup to preserve napari window.
    """
    for backend_type, instance in _backend_instances.items():
        if hasattr(instance, 'cleanup'):
            try:
                instance.cleanup()
                logger.debug(f"Cleaned up backend '{backend_type}'")
            except Exception as e:
                logger.warning(f"Failed to cleanup backend '{backend_type}': {e}")

    _backend_instances.clear()
    logger.info("All backend instances cleaned up")




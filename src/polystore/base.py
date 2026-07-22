# polystore/base.py
"""
Abstract base classes for storage backends.

This module defines the fundamental interfaces for storage backends,
independent of specific implementations. It establishes the contract
that all storage backends must fulfill.
"""

import logging
import threading
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Set, Union

import numpy as np
from .constants import Backend
from .exceptions import StorageResolutionError
from .registry import AutoRegisterMeta

logger = logging.getLogger(__name__)


class ImageSamplingStatisticsScope(str, Enum):
    """Pixels over which one image-sampling result computed statistics."""

    SOURCE_RESOLUTION = "source_resolution"
    BOUNDED_SAMPLE = "bounded_sample"


@dataclass(frozen=True, slots=True)
class ImageSamplingRequest:
    """Decoder-neutral request for one bounded native-resolution image region."""

    origin_yx: tuple[int, int] = (0, 0)
    shape_yx: tuple[int, int] = (32, 32)
    resolution_index: int | None = None
    max_auto_resolution_size: int = 1024

    def __post_init__(self) -> None:
        origin_yx = tuple(self.origin_yx)
        shape_yx = tuple(self.shape_yx)
        if len(origin_yx) != 2 or any(
            not isinstance(value, int) or isinstance(value, bool) or value < 0
            for value in origin_yx
        ):
            raise ValueError(
                "ImageSamplingRequest.origin_yx must contain two nonnegative integers."
            )
        if len(shape_yx) != 2 or any(
            not isinstance(value, int) or isinstance(value, bool) or value <= 0
            for value in shape_yx
        ):
            raise ValueError(
                "ImageSamplingRequest.shape_yx must contain two positive integers."
            )
        resolution_index = self.resolution_index
        if resolution_index is not None and (
            not isinstance(resolution_index, int)
            or isinstance(resolution_index, bool)
            or resolution_index < 0
        ):
            raise ValueError(
                "ImageSamplingRequest.resolution_index must be a nonnegative integer "
                "or None."
            )
        if (
            not isinstance(self.max_auto_resolution_size, int)
            or isinstance(self.max_auto_resolution_size, bool)
            or self.max_auto_resolution_size <= 0
        ):
            raise ValueError(
                "ImageSamplingRequest.max_auto_resolution_size must be positive."
            )
        object.__setattr__(self, "origin_yx", origin_yx)
        object.__setattr__(self, "shape_yx", shape_yx)


@dataclass(frozen=True, slots=True)
class ImageSamplingResult:
    """Bounded pixels plus native-resolution provenance from one data source."""

    data: Any
    statistics_data: Any
    source_shape: tuple[int, ...]
    resolution_shape: tuple[int, ...]
    sample_origin_yx: tuple[int, int]
    selected_resolution_index: int = 0
    resolution_count: int = 1
    downsample_yx: tuple[float, float] = (1.0, 1.0)
    statistics_scope: ImageSamplingStatisticsScope = (
        ImageSamplingStatisticsScope.SOURCE_RESOLUTION
    )


class PicklableBackend(ABC):
    """
    Abstract base class for storage backends that support pickling with connection parameters.

    Backends that maintain network connections or other unpicklable resources must
    explicitly inherit from this ABC and implement the required methods to be safely
    pickled and unpickled in multiprocessing workers.

    This is particularly important for backends that maintain:
    - Network connections (e.g., OMERO, remote databases, S3)
    - File handles that can't cross process boundaries
    - Authentication sessions

    The pattern is:
    1. Main process: Backend stores connection params via get_connection_params()
    2. Pickling: FileManager preserves these params
    3. Worker process: Backend recreates connection using set_connection_params()

    This uses nominal typing (ABC) not structural typing (Protocol), so
    explicit inheritance is required for isinstance() checks to work.
    """

    @classmethod
    def from_connection_params(
        cls,
        params: Optional[Dict[str, Any]],
    ) -> "PicklableBackend":
        """
        Recreate a backend instance from worker-safe connection parameters.

        The default contract is a no-argument constructor followed by
        set_connection_params(). Backends with required constructor arguments
        must override this method.
        """
        backend = cls()
        backend.set_connection_params(params)
        return backend

    @abstractmethod
    def get_connection_params(self) -> Optional[Dict[str, Any]]:
        """
        Return connection parameters for worker process reconnection.

        Returns:
            Dictionary of connection parameters (host, port, username, etc.)
            or None if no connection parameters are available.

        Note:
            Passwords should NOT be included in connection params.
            They should be retrieved from environment variables in the worker.
        """
        pass

    @abstractmethod
    def set_connection_params(self, params: Optional[Dict[str, Any]]) -> None:
        """
        Set connection parameters (used during unpickling).

        Args:
            params: Dictionary of connection parameters or None
        """
        pass


class BackendBase(metaclass=AutoRegisterMeta):
    """
    Base class for all storage backends (read-only and read-write).

    Defines the registry and common interface for backend discovery.
    Concrete backends should inherit from StorageBackend, ReadOnlyBackend, or DataSink.
    """
    __registry_key__ = '_backend_type'

    # Enable automatic discovery of backends in polystore package
    from metaclass_registry import RegistryConfig, LazyDiscoveryDict
    __registry_config__ = RegistryConfig(
        registry_dict=LazyDiscoveryDict(),
        key_attribute='_backend_type',
        skip_if_no_key=True,
        registry_name='backend',
        discovery_package='polystore',
        discovery_recursive=False,  # All backends are in polystore/*.py (flat structure)
    )

    @property
    @abstractmethod
    def requires_filesystem_validation(self) -> bool:
        """
        Whether this backend requires filesystem validation.

        Returns:
            True for local filesystem backends, False for virtual/remote/streaming
        """
        pass

    # Class attribute: can be accessed without instantiation
    supports_arbitrary_files: bool = True
    """
    Whether this backend can save arbitrary file formats (e.g., .tif, .csv, .roi.zip).
    
    True for backends that handle files directly (disk, streaming viewers)
    False for backends that only handle array data (zarr, HDF5)
    
    Override this in specific backends that cannot handle arbitrary files.
    Default is True for backwards compatibility.
    """

    def supports_file_path(self, path: Union[str, Path]) -> bool:
        """Return whether this backend can save the requested file path."""
        del path
        return self.supports_arbitrary_files

    def bind_registry(self, registry: Mapping[str, "BackendBase"]) -> None:
        """Bind an execution-local backend registry when this backend needs it."""
        del registry

    def resolve_listed_address(
        self,
        listed_address: Union[str, Path],
        *,
        directory: Union[str, Path],
    ) -> Union[str, Path]:
        """Return the same-backend address represented by one listing entry."""

        del directory
        return listed_address

    def physical_source_path(
        self,
        backend_address: Union[str, Path],
        *,
        base_path: Path,
    ) -> Union[str, Path, None]:
        """Return a physical source path when this backend declares one."""

        del backend_address, base_path
        return None


class DataSink(BackendBase):
    """
    Abstract base class for data destinations.

    Defines the minimal interface for sending data to any destination,
    whether storage, streaming, or other data handling systems.

    This interface follows core design principles:
    - Fail-loud: No defensive programming, explicit error handling
    - Minimal: Only essential operations both storage and streaming need
    - Generic: Enables any type of data destination backend

    Inherits from BackendBase for automatic registration.
    """

    def contextual_save_kwargs(
        self,
        *,
        images_dir: str | None,
    ) -> Mapping[str, Any]:
        """Return backend-owned save context for artifact materialization."""

        del images_dir
        return {}

    @abstractmethod
    def save(self, data: Any, identifier: Union[str, Path], **kwargs) -> None:
        """
        Send data to the destination.

        Args:
            data: The data to send
            identifier: Unique identifier for the data (path-like for compatibility)
            **kwargs: Backend-specific arguments

        Raises:
            TypeError: If identifier is not a valid type
            ValueError: If data cannot be sent to destination
        """
        pass

    @abstractmethod
    def save_batch(self, data_list: List[Any], identifiers: List[Union[str, Path]], **kwargs) -> None:
        """
        Send multiple data objects to the destination in a single operation.

        Args:
            data_list: List of data objects to send
            identifiers: List of unique identifiers (must match length of data_list)
            **kwargs: Backend-specific arguments

        Raises:
            ValueError: If data_list and identifiers have different lengths
            TypeError: If any identifier is not a valid type
            ValueError: If any data cannot be sent to destination
        """
        pass


class DataSource(BackendBase):
    """
    Abstract base class for read-only data sources.

    Defines the minimal interface for loading data from any source,
    whether filesystem, virtual workspace, remote storage, or databases.

    This is the read-only counterpart to DataSink.
    """

    def resolve_address(
        self,
        backend_address: Union[str, Path],
        *,
        base_path: Path,
    ) -> Union[str, Path]:
        """Resolve one backend-owned address for loading."""
        del base_path
        return backend_address

    def source_path(
        self,
        backend_address: Union[str, Path],
        *,
        base_path: Path,
    ) -> Union[str, Path]:
        """Return the physical source path represented by an opaque address."""

        return self.resolve_address(backend_address, base_path=base_path)

    def physical_source_path(
        self,
        backend_address: Union[str, Path],
        *,
        base_path: Path,
    ) -> Union[str, Path]:
        """Compose the optional root projection from this source contract."""

        return self.source_path(backend_address, base_path=base_path)

    def sample(
        self,
        file_path: Union[str, Path],
        request: ImageSamplingRequest,
    ) -> ImageSamplingResult:
        """Load and bound one ordinary image through the generic source contract.

        Decoder leaves with native region or pyramid support override this template
        method. The default preserves ordinary backend loading behavior and applies
        the requested spatial bound after loading.
        """

        if request.resolution_index not in (None, 0):
            raise ValueError(
                "This data source exposes one resolution; resolution_index must "
                "be 0 or None."
            )
        array = np.asarray(self.load(file_path))
        if array.ndim < 2:
            raise ValueError(
                f"Sampled image must have at least two dimensions, got {array.shape!r}."
            )
        y, x = request.origin_yx
        height, width = request.shape_yx
        y_stop = min(int(array.shape[-2]), y + height)
        x_stop = min(int(array.shape[-1]), x + width)
        sample = array[..., y:y_stop, x:x_stop]
        source_shape = tuple(int(value) for value in array.shape)
        return ImageSamplingResult(
            data=sample,
            statistics_data=array,
            source_shape=source_shape,
            resolution_shape=source_shape,
            sample_origin_yx=request.origin_yx,
        )

    @abstractmethod
    def load(self, file_path: Union[str, Path], **kwargs) -> Any:
        """
        Load data from a file path.

        Args:
            file_path: Path to the file to load
            **kwargs: Backend-specific arguments

        Raises:
            FileNotFoundError: If the file does not exist
            TypeError: If file_path is not a valid type
            ValueError: If the data cannot be loaded
        """
        pass

    @abstractmethod
    def load_batch(self, file_paths: List[Union[str, Path]], **kwargs) -> List[Any]:
        """
        Load multiple files in a single batch operation.

        Args:
            file_paths: List of file paths to load
            **kwargs: Backend-specific arguments

        Raises:
            FileNotFoundError: If any file does not exist
            TypeError: If any file_path is not a valid type
            ValueError: If any data cannot be loaded
        """
        pass

    @abstractmethod
    def list_files(self, directory: Union[str, Path], pattern: Optional[str] = None,
                  extensions: Optional[Set[str]] = None, recursive: bool = False,
                  **kwargs) -> List[str]:
        """
        List files in a directory.

        Args:
            directory: Directory to list files from
            pattern: Optional glob pattern to filter files
            extensions: Optional set of file extensions to filter (e.g., {'.tif', '.png'})
            recursive: Whether to search recursively
            **kwargs: Backend-specific arguments

        Returns:
            List of file paths (absolute or relative depending on backend)
        """
        pass

    @abstractmethod
    def exists(self, path: Union[str, Path]) -> bool:
        """Check if a path exists."""
        pass

    @abstractmethod
    def is_file(self, path: Union[str, Path]) -> bool:
        """Check if a path is a file."""
        pass

    @abstractmethod
    def is_dir(self, path: Union[str, Path]) -> bool:
        """Check if a path is a directory."""
        pass

    @abstractmethod
    def list_dir(self, path: Union[str, Path]) -> List[str]:
        """List immediate entries in a directory (names only)."""
        pass


class VirtualBackend(DataSink):
    """
    Abstract base for backends that provide virtual filesystem semantics.

    Virtual backends generate file listings on-demand without real filesystem operations.
    Examples: OMERO (generates filenames from plate structure), S3 (lists objects), HTTP APIs.

    Virtual backends may require additional context via kwargs.
    Backends MUST validate required kwargs and raise TypeError if missing.
    """

    @abstractmethod
    def load(self, file_path: Union[str, Path], **kwargs) -> Any:
        """
        Load data from virtual path.

        Args:
            file_path: Virtual path to load
            **kwargs: Backend-specific context (e.g., plate_id for OMERO)

        Returns:
            The loaded data

        Raises:
            FileNotFoundError: If the virtual path does not exist
            TypeError: If required kwargs are missing
            ValueError: If the data cannot be loaded
        """
        pass

    @abstractmethod
    def load_batch(self, file_paths: List[Union[str, Path]], **kwargs) -> List[Any]:
        """
        Load multiple virtual paths in a single batch operation.

        Args:
            file_paths: List of virtual paths to load
            **kwargs: Backend-specific context

        Returns:
            List of loaded data objects in the same order as file_paths

        Raises:
            FileNotFoundError: If any virtual path does not exist
            TypeError: If required kwargs are missing
            ValueError: If any data cannot be loaded
        """
        pass

    @abstractmethod
    def list_files(self, directory: Union[str, Path], pattern: Optional[str] = None,
                  extensions: Optional[Set[str]] = None, recursive: bool = False,
                  **kwargs) -> List[str]:
        """
        Generate virtual file listing.

        Args:
            directory: Virtual directory path
            pattern: Optional file pattern filter
            extensions: Optional set of file extensions to filter
            recursive: Whether to list recursively
            **kwargs: Backend-specific context (e.g., plate_id for OMERO)

        Returns:
            List of virtual filenames

        Raises:
            TypeError: If required kwargs are missing
            ValueError: If directory is invalid
        """
        pass

    @property
    def requires_filesystem_validation(self) -> bool:
        """
        Whether this backend requires filesystem validation.

        Virtual backends return False - they don't have real filesystem paths.
        Real backends return True - they need path validation.

        Returns:
            False for virtual backends
        """
        return False


class ReadOnlyBackend(DataSource):
    """
    Abstract base class for read-only storage backends with auto-registration.

    Use this for backends that only need to read data (virtual workspaces,
    read-only mounts, archive viewers, etc.).

    Inherits from DataSource (which inherits from BackendBase for registration).
    No write operations - clean separation of concerns.

    Concrete implementations are automatically registered via AutoRegisterMeta.
    """

    @property
    def requires_filesystem_validation(self) -> bool:
        """
        Whether this backend requires filesystem validation.

        Returns:
            False for virtual/remote backends, True for local filesystem
        """
        return False

    # Inherits all abstract methods from DataSource:
    # - load(), load_batch()
    # - list_files(), list_dir()
    # - exists(), is_file(), is_dir()


class StorageBackend(DataSource, DataSink):
    """
    Abstract base class for read-write storage backends.

    Extends DataSource (read) and DataSink (write) with file system operations
    for backends that provide persistent storage with file-like semantics.

    Concrete implementations are automatically registered via AutoRegisterMeta.
    """
    # Inherits load(), load_batch(), list_files(), etc. from DataSource
    # Inherits save() and save_batch() from DataSink

    @property
    def requires_filesystem_validation(self) -> bool:
        """
        Whether this backend requires filesystem validation.

        Returns:
            True for real filesystem backends (default for StorageBackend)
        """
        return True

    def exists(self, path: Union[str, Path]) -> bool:
        """
        Declarative truth test: does the path resolve to a valid object?

        A path only 'exists' if:
        - it is a valid file or directory
        - or it is a symlink that resolves to a valid file or directory

        Returns:
            bool: True if path structurally resolves to a real object
        """
        try:
            return self.is_file(path)
        except (FileNotFoundError, NotADirectoryError, StorageResolutionError):
            pass
        except IsADirectoryError:
            # Path exists but is a directory, so check if it's a valid directory
            try:
                return self.is_dir(path)
            except (FileNotFoundError, NotADirectoryError, StorageResolutionError):
                return False

        # If is_file failed for other reasons, try is_dir
        try:
            return self.is_dir(path)
        except (FileNotFoundError, NotADirectoryError, StorageResolutionError):
            return False


def _create_storage_registry() -> Dict[str, DataSink]:
    """
    Create a new storage registry using metaclass-based discovery.

    This function creates a dictionary mapping backend names to their respective
    storage backend instances using automatic discovery and registration.

    Now returns Dict[str, DataSink] to support both StorageBackend and StreamingBackend.

    Returns:
        A dictionary mapping backend names to DataSink instances (polymorphic)

    Note:
        This function now uses the metaclass-based registry system for automatic
        backend discovery, eliminating hardcoded imports.
    """
    # Import the metaclass-based registry system
    from .backend_registry import create_storage_registry

    return create_storage_registry()


class _LazyStorageRegistry(dict):
    """
    Storage registry that auto-initializes on first access.

    This maintains backward compatibility with existing code that
    directly accesses storage_registry without calling ensure_storage_registry().
    All read operations trigger lazy initialization, while write operations
    (like OMERO backend registration) work without initialization.
    """

    def __getitem__(self, key):
        ensure_storage_registry()
        return super().__getitem__(key)

    def __setitem__(self, key, value):
        # Allow setting without initialization (for OMERO backend registration)
        return super().__setitem__(key, value)

    def __contains__(self, key):
        ensure_storage_registry()
        return super().__contains__(key)

    def get(self, key, default=None):
        ensure_storage_registry()
        return super().get(key, default)

    def keys(self):
        ensure_storage_registry()
        return super().keys()

    def values(self):
        ensure_storage_registry()
        return super().values()

    def items(self):
        ensure_storage_registry()
        return super().items()


# Global singleton storage registry - created lazily on first access
# This is the shared registry instance that all components should use
storage_registry: Dict[str, DataSink] = _LazyStorageRegistry()
_registry_initialized = False
# Use RLock (reentrant lock) to allow same thread to acquire lock multiple times
# This prevents deadlocks when gc.collect() triggers __del__ methods that access storage_registry
_registry_lock = threading.RLock()


def ensure_storage_registry() -> None:
    """
    Ensure storage registry is initialized.

    Lazily creates the registry on first access to avoid importing
    GPU-heavy backends during module import. This provides instant
    imports while maintaining backward compatibility.

    Thread-safe: Multiple threads can call this simultaneously.
    """
    global _registry_initialized

    # Double-checked locking pattern for thread safety
    if not _registry_initialized:
        with _registry_lock:
            if not _registry_initialized:
                storage_registry.update(_create_storage_registry())
                _registry_initialized = True
                logger.info("Lazily initialized storage registry")


def get_backend(backend_type: str) -> DataSink:
    """
    Get a backend by type, ensuring registry is initialized.

    Args:
        backend_type: Backend type (e.g., 'disk', 'memory', 'zarr')

    Returns:
        Backend instance

    Raises:
        KeyError: If backend type not found
    """
    ensure_storage_registry()

    if isinstance(backend_type, Enum):
        backend_type = backend_type.value
    backend_key = str(backend_type).lower()
    if backend_key not in storage_registry:
        raise KeyError(f"Backend '{backend_type}' not found. "
                      f"Available: {list(storage_registry.keys())}")

    return storage_registry[backend_key]


def reset_memory_backend() -> None:
    """
    Clear files from the memory backend while preserving directory structure.

    This function clears all file entries from the existing memory backend but preserves
    directory entries (None values). This prevents key collisions between plate executions
    while maintaining the directory structure needed for subsequent operations.

    Benefits over full reset:
    - Preserves directory structure created by path planner
    - Prevents "Parent path does not exist" errors on subsequent runs
    - Avoids key collisions for special inputs/outputs
    - Maintains performance by not recreating directory hierarchy

    Note:
        This only affects the memory backend. Other backends (disk, zarr) are not modified.
        Caller is responsible for calling gc.collect() and GPU cleanup after this function.
    """

    # Clear files from existing memory backend while preserving directories
    memory_backend = storage_registry[Backend.MEMORY.value]

    existing_keys = list(memory_backend._memory_store.keys())
    logger.debug("Memory backend has %s entries before clear", len(existing_keys))
    logger.debug("First memory backend keys before clear: %s", existing_keys[:10])

    memory_backend.clear_files_only()

    remaining_keys = list(memory_backend._memory_store.keys())
    logger.debug(
        "Memory backend has %s entries after clear (directories only)",
        len(remaining_keys),
    )
    logger.debug("First memory backend keys after clear: %s", remaining_keys[:10])
    logger.info("Memory backend reset - files cleared, directories preserved")

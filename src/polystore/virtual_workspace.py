"""Virtual Workspace Backend - Symlink-free workspace using metadata mapping."""

import json
import logging
from pathlib import Path
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, ClassVar, Dict, Hashable, List, Mapping, Optional, Set, Union
from fnmatch import fnmatch

import numpy as np

from .disk import DiskStorageBackend
from .metadata_writer import get_metadata_path
from .exceptions import StorageResolutionError
from .base import PicklableBackend, ReadOnlyBackend
from .constants import Backend
from .registry import AutoRegisterMeta

logger = logging.getLogger(__name__)


class VirtualWorkspaceSourceRefResolver(ABC, metaclass=AutoRegisterMeta):
    """Nominal loader family for virtual-workspace source references."""

    __registry_key__ = "resolver_key"
    __skip_if_no_key__ = True
    resolver_key: ClassVar[str | None] = None
    priority: ClassVar[int]

    @classmethod
    def for_ref(cls, source_ref: Any) -> "VirtualWorkspaceSourceRefResolver":
        for resolver_type in sorted(
            cls.__registry__.values(),
            key=lambda registered_type: registered_type.priority,
        ):
            resolver = resolver_type()
            if resolver.accepts(source_ref):
                return resolver
        raise StorageResolutionError(
            f"Unsupported virtual workspace source reference: {source_ref!r}"
        )

    @abstractmethod
    def accepts(self, source_ref: Any) -> bool:
        """Return whether this resolver owns the reference shape."""

    @abstractmethod
    def source_path(self, plate_root: Path, source_ref: Any) -> Path:
        """Return the concrete source path for existence and diagnostics."""

    @abstractmethod
    def load(
        self,
        disk_backend: DiskStorageBackend,
        plate_root: Path,
        source_ref: Any,
        **kwargs: Any,
    ) -> Any:
        """Load the payload addressed by this source reference."""

    def batch_key(self, plate_root: Path, source_ref: Any) -> Hashable:
        """Return the physical source identity shared by batch-compatible refs."""
        return self.source_path(plate_root, source_ref)

    def load_batch(
        self,
        disk_backend: DiskStorageBackend,
        plate_root: Path,
        source_refs: tuple[Any, ...],
        **kwargs: Any,
    ) -> tuple[Any, ...]:
        """Load a batch of references owned by this resolver."""
        return tuple(
            self.load(disk_backend, plate_root, source_ref, **kwargs)
            for source_ref in source_refs
        )


class PathSourceRefResolver(VirtualWorkspaceSourceRefResolver):
    """Resolve legacy string path mappings."""

    resolver_key = "path"
    priority = 100

    def accepts(self, source_ref: Any) -> bool:
        return isinstance(source_ref, (str, Path))

    def source_path(self, plate_root: Path, source_ref: Any) -> Path:
        path = Path(source_ref)
        return path if path.is_absolute() else plate_root / path

    def load(
        self,
        disk_backend: DiskStorageBackend,
        plate_root: Path,
        source_ref: Any,
        **kwargs: Any,
    ) -> Any:
        return disk_backend.load(self.source_path(plate_root, source_ref), **kwargs)


class DiskSourceRefResolver(VirtualWorkspaceSourceRefResolver):
    """Resolve structured disk refs, including single-plane TIFF pages."""

    resolver_key = "disk"
    priority = 10

    def accepts(self, source_ref: Any) -> bool:
        return (
            isinstance(source_ref, Mapping)
            and source_ref.get("backend", Backend.DISK.value) == Backend.DISK.value
            and isinstance(source_ref.get("source_path"), (str, Path))
        )

    def source_path(self, plate_root: Path, source_ref: Any) -> Path:
        path = Path(source_ref["source_path"])
        return path if path.is_absolute() else plate_root / path

    def load(
        self,
        disk_backend: DiskStorageBackend,
        plate_root: Path,
        source_ref: Any,
        **kwargs: Any,
    ) -> Any:
        payload = disk_backend.load(self.source_path(plate_root, source_ref), **kwargs)
        plane_index = source_ref.get("plane_index")
        if plane_index is None:
            return payload
        return _payload_plane(payload, int(plane_index), source_ref)

    def load_batch(
        self,
        disk_backend: DiskStorageBackend,
        plate_root: Path,
        source_refs: tuple[Any, ...],
        **kwargs: Any,
    ) -> tuple[Any, ...]:
        if not source_refs:
            return ()
        source_paths = tuple(self.source_path(plate_root, ref) for ref in source_refs)
        unique_source_paths = tuple(dict.fromkeys(source_paths))
        if len(unique_source_paths) != 1:
            raise StorageResolutionError(
                f"{type(self).__name__}.load_batch requires one physical source path, "
                f"got {len(unique_source_paths)}."
            )
        payload = disk_backend.load(unique_source_paths[0], **kwargs)
        return tuple(
            payload
            if source_ref.get("plane_index") is None
            else _payload_plane(payload, int(source_ref["plane_index"]), source_ref)
            for source_ref in source_refs
        )


def _payload_plane(payload: Any, plane_index: int, source_ref: Mapping[str, Any]) -> Any:
    array = np.asarray(payload)
    if array.ndim < 3:
        raise StorageResolutionError(
            f"Source ref {source_ref!r} requested plane {plane_index}, but loaded "
            f"payload shape {array.shape!r} is not a stack."
        )
    if plane_index < 0 or plane_index >= array.shape[0]:
        raise StorageResolutionError(
            f"Source ref {source_ref!r} requested plane {plane_index}, but loaded "
            f"payload shape is {array.shape!r}."
        )
    return array[plane_index]


@dataclass(frozen=True, slots=True)
class VirtualWorkspaceResolvedRef:
    """Resolved source reference for one virtual workspace request."""

    output_index: int
    source_ref: Any
    resolver: VirtualWorkspaceSourceRefResolver

    def batch_key(self, plate_root: Path) -> tuple[type[VirtualWorkspaceSourceRefResolver], Hashable]:
        return (type(self.resolver), self.resolver.batch_key(plate_root, self.source_ref))


_UNSET_BATCH_OUTPUT = object()


class VirtualWorkspaceBackend(ReadOnlyBackend, PicklableBackend):
    """
    Read-only path translation layer for virtual workspace.

    Maps virtual filenames to real plate files using workspace_mapping from
    metadata file (plate-relative paths), then delegates I/O to DiskStorageBackend.

    This is NOT a storage backend - it's a path resolver. It does not support save operations.

    Follows OMERO backend pattern:
    - Explicit initialization with plate_root
    - Fail-loud path resolution
    - No path inspection or 'workspace' searching

    Uses PLATE-RELATIVE paths (no workspace directory):
    - Mapping: {"Images/r01c01f05.tif": "Images/r01c01f01.tif"}
    - Resolution: plate_root / "Images/r01c01f05.tif" → plate_root / "Images/r01c01f01.tif"

    Example:
        backend = VirtualWorkspaceBackend(plate_root=Path("/data/plate"))
        # Input: plate_root / "Images/r01c01f05.tif" (doesn't exist)
        # Resolves to: plate_root / "Images/r01c01f01.tif" (exists)
    """
    
    _backend_type = 'virtual_workspace'  # Auto-registers via metaclass
    
    def __init__(self, plate_root: Path):
        """
        Initialize with explicit plate root.

        Args:
            plate_root: Path to plate directory containing the metadata file

        Raises:
            FileNotFoundError: If metadata file doesn't exist
            ValueError: If no workspace_mapping in metadata
        """
        self.plate_root = Path(plate_root)
        self.disk_backend = DiskStorageBackend()
        self._mapping_cache: Optional[Dict[str, Any]] = None
        self._cache_mtime: Optional[float] = None

        # Load mapping eagerly - fail loud if metadata missing
        self._load_mapping()

    @classmethod
    def from_connection_params(
        cls,
        params: Optional[Dict[str, Any]],
    ) -> "VirtualWorkspaceBackend":
        if not params:
            raise ValueError("VirtualWorkspaceBackend requires plate_root.")
        return cls(plate_root=Path(params["plate_root"]))

    def get_connection_params(self) -> Optional[Dict[str, Any]]:
        return {"plate_root": str(self.plate_root)}

    def set_connection_params(self, params: Optional[Dict[str, Any]]) -> None:
        if not params:
            raise ValueError("VirtualWorkspaceBackend requires plate_root.")
        self.plate_root = Path(params["plate_root"])
        self.disk_backend = DiskStorageBackend()
        self._mapping_cache = None
        self._cache_mtime = None
        self._load_mapping()

    @staticmethod
    def _normalize_relative_path(path_str: str) -> str:
        """
        Normalize relative path for internal mapping lookups.

        Converts Windows backslashes to forward slashes and normalizes
        '.' (current directory) to empty string for root directory.

        Args:
            path_str: Relative path string to normalize

        Returns:
            Normalized path string with forward slashes, empty string for root
        """
        normalized = path_str.replace('\\', '/')
        return '' if normalized == '.' else normalized
    
    def _load_mapping(self) -> Dict[str, Any]:
        """
        Load workspace_mapping from metadata with mtime-based caching.
        
        Returns:
            Combined mapping from all subdirectories
            
        Raises:
            FileNotFoundError: If metadata file doesn't exist
            ValueError: If no workspace_mapping in metadata
        """
        metadata_path = get_metadata_path(self.plate_root)
        if not metadata_path.exists():
            raise FileNotFoundError(
                f"Metadata not found: {metadata_path}\n"
                f"Plate root: {self.plate_root}"
            )
        
        # Check cache with mtime invalidation
        current_mtime = metadata_path.stat().st_mtime
        if self._mapping_cache is not None and self._cache_mtime == current_mtime:
            return self._mapping_cache
        
        # Load and combine mappings from all subdirectories
        with open(metadata_path, 'r') as f:
            metadata = json.load(f)
        
        combined_mapping = {}
        for subdir_data in metadata.get('subdirectories', {}).values():
            workspace_mapping = subdir_data.get('workspace_mapping', {})
            combined_mapping.update(workspace_mapping)
        
        if not combined_mapping:
            raise ValueError(
                f"No workspace_mapping in {metadata_path}\n"
                f"Plate root: {self.plate_root}\n"
                f"This is not a virtual workspace."
            )
        
        # Cache it
        self._mapping_cache = combined_mapping
        self._cache_mtime = current_mtime
        
        logger.info(f"Loaded {len(combined_mapping)} mappings for {self.plate_root}")
        return combined_mapping
    
    def _resolve_ref(self, path: Union[str, Path]) -> Any:
        """
        Resolve virtual path to real plate path using plate-relative mapping.

        Pure mapping-based resolution - no physical path fallbacks.
        Follows OMERO backend pattern: all paths go through mapping.

        Args:
            path: Absolute or relative path (e.g., "/data/plate/Images/r01c01f05.tif" or "Images/r01c01f05.tif")

        Returns:
            Real absolute path: e.g., "/data/plate/Images/r01c01f01.tif"

        Raises:
            StorageResolutionError: If path not in mapping
        """
        path_obj = Path(path)

        # Convert to plate-relative path
        try:
            relative_path = path_obj.relative_to(self.plate_root)
        except ValueError:
            # Already relative or different root
            relative_path = path_obj

        # Normalize Windows backslashes to forward slashes
        relative_str = str(relative_path).replace('\\', '/')

        # Load mapping if not cached
        if self._mapping_cache is None:
            self._load_mapping()

        # Resolve via mapping - fail loud if not in mapping
        if relative_str not in self._mapping_cache:
            raise StorageResolutionError(
                f"Path not in virtual workspace mapping: {relative_str}\n"
                f"Plate root: {self.plate_root}\n"
                f"Available virtual paths: {len(self._mapping_cache)}\n"
                f"This path must be accessed through the virtual workspace mapping."
            )

        source_ref = self._mapping_cache[relative_str]
        logger.debug("Resolved virtual source ref: %s -> %r", relative_str, source_ref)
        return source_ref

    def _resolve_path(self, path: Union[str, Path]) -> str:
        """Resolve a virtual path to the concrete source path for diagnostics."""
        source_ref = self._resolve_ref(path)
        resolver = VirtualWorkspaceSourceRefResolver.for_ref(source_ref)
        return str(resolver.source_path(self.plate_root, source_ref))
    
    def load(self, file_path: Union[str, Path], **kwargs) -> Any:
        """Load file from virtual workspace."""
        source_ref = self._resolve_ref(file_path)
        resolver = VirtualWorkspaceSourceRefResolver.for_ref(source_ref)
        return resolver.load(
            self.disk_backend,
            self.plate_root,
            source_ref,
            **kwargs,
        )
    
    def load_batch(self, file_paths: List[Union[str, Path]], **kwargs) -> List[Any]:
        """Load multiple files from virtual workspace."""
        resolved_refs = tuple(
            self._resolved_ref(index, file_path)
            for index, file_path in enumerate(file_paths)
        )
        grouped_refs: dict[
            tuple[type[VirtualWorkspaceSourceRefResolver], Hashable],
            list[VirtualWorkspaceResolvedRef],
        ] = {}
        for resolved_ref in resolved_refs:
            grouped_refs.setdefault(
                resolved_ref.batch_key(self.plate_root),
                [],
            ).append(resolved_ref)

        ordered_outputs: list[Any] = [_UNSET_BATCH_OUTPUT] * len(file_paths)
        for group in grouped_refs.values():
            resolver = group[0].resolver
            source_refs = tuple(ref.source_ref for ref in group)
            outputs = resolver.load_batch(
                self.disk_backend,
                self.plate_root,
                source_refs,
                **kwargs,
            )
            if len(outputs) != len(group):
                raise StorageResolutionError(
                    f"{type(resolver).__name__}.load_batch returned {len(outputs)} "
                    f"outputs for {len(group)} virtual workspace refs."
                )
            for resolved_ref, output in zip(group, outputs, strict=True):
                ordered_outputs[resolved_ref.output_index] = output

        if any(output is _UNSET_BATCH_OUTPUT for output in ordered_outputs):
            raise StorageResolutionError(
                "Virtual workspace batch load did not populate every requested path."
            )
        return ordered_outputs

    def _resolved_ref(
        self,
        output_index: int,
        file_path: Union[str, Path],
    ) -> VirtualWorkspaceResolvedRef:
        source_ref = self._resolve_ref(file_path)
        return VirtualWorkspaceResolvedRef(
            output_index=output_index,
            source_ref=source_ref,
            resolver=VirtualWorkspaceSourceRefResolver.for_ref(source_ref),
        )
    
    def list_files(self, directory: Union[str, Path], pattern: Optional[str] = None,
                  extensions: Optional[Set[str]] = None, recursive: bool = False,
                  **kwargs) -> List[str]:
        """
        List files in directory (returns absolute paths of virtual files).

        Returns absolute virtual paths from mapping that match the directory.

        Raises:
            ValueError: If mapping not loaded
        """
        dir_path = Path(directory)

        # Convert to plate-relative
        try:
            relative_dir = dir_path.relative_to(self.plate_root)
        except ValueError:
            # Already relative
            relative_dir = dir_path

        # Normalize to forward slashes for comparison with JSON mapping
        relative_dir_str = self._normalize_relative_path(str(relative_dir))

        # Load mapping - fail loud if missing
        if self._mapping_cache is None:
            self._load_mapping()

        logger.debug(
            "VirtualWorkspace.list_files directory=%s recursive=%s pattern=%s extensions=%s",
            directory,
            recursive,
            pattern,
            extensions,
        )
        logger.debug("  plate_root=%s", self.plate_root)
        logger.debug("  relative_dir_str=%r", relative_dir_str)
        logger.debug("  mapping has %s entries", len(self._mapping_cache))

        lowercase_extensions = (
            None if extensions is None else {ext.lower() for ext in extensions}
        )

        # Filter paths in this directory
        results = []
        for virtual_relative in self._mapping_cache.keys():
            # Check directory match using string comparison with forward slashes
            if recursive:
                # For recursive, check if virtual_relative starts with directory prefix
                if relative_dir_str:
                    if not virtual_relative.startswith(relative_dir_str + '/') and virtual_relative != relative_dir_str:
                        continue
                # else: relative_dir_str is empty (root), include all files
            else:
                # For non-recursive, check if parent directory matches
                vpath_parent = self._normalize_relative_path(str(Path(virtual_relative).parent))
                if vpath_parent != relative_dir_str:
                    continue

            # Apply filters
            vpath = Path(virtual_relative)
            if pattern and not fnmatch(vpath.name, pattern):
                continue
            if lowercase_extensions and vpath.suffix.lower() not in lowercase_extensions:
                continue

            # Return absolute path
            results.append(str(self.plate_root / virtual_relative))

        logger.debug("  VirtualWorkspace.list_files returning %s files", len(results))
        if len(results) == 0 and len(self._mapping_cache) > 0:
            # Log first few mapping keys to help debug
            sample_keys = list(self._mapping_cache.keys())[:3]
            logger.debug("  Sample mapping keys: %s", sample_keys)
            if not recursive and relative_dir_str == '':
                sample_parents = [str(Path(k).parent).replace('\\', '/') for k in sample_keys]
                logger.debug("  Sample parent dirs: %s", sample_parents)
                logger.info(f"  Expected parent to match: '{relative_dir_str}'")

        return sorted(results)

    def list_dir(self, path: Union[str, Path]) -> List[str]:
        """
        List directory entries (names only, not full paths).

        For virtual workspace, this returns the unique directory names
        that exist in the mapping under the given path.
        """
        path = Path(path)

        # Convert to plate-relative path
        if path.is_absolute():
            try:
                relative_path = path.relative_to(self.plate_root)
            except ValueError:
                # Path is not under plate_root
                raise FileNotFoundError(f"Path not under plate root: {path}")
        else:
            relative_path = path

        # Normalize to string with forward slashes
        relative_str = self._normalize_relative_path(str(relative_path))

        # Collect all unique directory/file names under this path
        entries = set()
        for virtual_relative in self._mapping_cache.keys():
            # Check if this virtual path is under the requested directory
            if relative_str:
                # Looking for children of a subdirectory
                if not virtual_relative.startswith(relative_str + '/'):
                    continue
                # Get the part after the directory prefix
                remainder = virtual_relative[len(relative_str) + 1:]
            else:
                # Looking for top-level entries
                remainder = virtual_relative

            # Get the first component (immediate child)
            first_component = remainder.split('/')[0] if '/' in remainder else remainder
            if first_component:
                entries.add(first_component)

        return sorted(entries)

    def exists(self, path: Union[str, Path]) -> bool:
        """Check if virtual path exists (file in mapping or directory containing files)."""
        if self._mapping_cache is None:
            self._load_mapping()

        try:
            relative_str = str(Path(path).relative_to(self.plate_root))
        except ValueError:
            relative_str = str(path)

        # Normalize Windows backslashes to forward slashes and '.' to ''
        relative_str = self._normalize_relative_path(relative_str)

        # File in mapping or directory prefix
        # For root directory (empty string), check if mapping has any files
        if relative_str == '':
            return len(self._mapping_cache) > 0

        return (relative_str in self._mapping_cache or
                any(vp.startswith(relative_str + '/') for vp in self._mapping_cache))
    
    def is_file(self, path: Union[str, Path]) -> bool:
        """Check if virtual path is a file (exists in mapping directly)."""
        if self._mapping_cache is None:
            self._load_mapping()

        try:
            relative_str = str(Path(path).relative_to(self.plate_root))
        except ValueError:
            relative_str = str(path)

        # Normalize Windows backslashes to forward slashes
        relative_str = relative_str.replace('\\', '/')

        # File if it's directly in the mapping
        return relative_str in self._mapping_cache

    def is_dir(self, path: Union[str, Path]) -> bool:
        """Check if virtual path is a directory (has files under it)."""
        if self._mapping_cache is None:
            self._load_mapping()

        try:
            relative_str = str(Path(path).relative_to(self.plate_root))
        except ValueError:
            relative_str = str(path)

        # Normalize to string with forward slashes and '.' to ''
        relative_str = self._normalize_relative_path(relative_str)

        # Directory if any virtual path starts with this prefix
        if relative_str:
            return any(vp.startswith(relative_str + '/') for vp in self._mapping_cache)
        else:
            # Root is always a directory if mapping exists
            return len(self._mapping_cache) > 0

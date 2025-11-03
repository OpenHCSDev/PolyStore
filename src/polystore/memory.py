# polystore/memory.py
"""
Memory storage backend module.

This module provides an in-memory implementation of the StorageBackend interface.
It stores data in memory and supports directory operations.
"""

import logging
import copy as py_copy
from pathlib import Path, PurePosixPath
from typing import Any, Dict, List, Optional, Set, Union

from .base import StorageBackend
from .exceptions import StorageResolutionError

logger = logging.getLogger(__name__)


class MemoryBackend(StorageBackend):
    """Memory storage backend with automatic registration."""
    _backend_type = "memory"
    def __init__(self, shared_dict: Optional[Dict[str, Any]] = None):
        """
        Initializes the memory storage.

        Args:
            shared_dict: If provided, uses this dictionary as the storage backend.
                         This is useful for sharing memory between processes with a
                         multiprocessing.Manager.dict. If None, a new local
                         dictionary is created.
        """
        self._memory_store = shared_dict if shared_dict is not None else {}
        self._prefixes = set()  # Declared directory-like namespaces

    def _normalize(self, path: Union[str, Path],bypass_normalization=False) -> str:
        """
        Normalize paths for memory backend storage.

        Memory backend uses relative paths internally to avoid conflicts
        between absolute paths from different systems. This method converts
        absolute paths to relative paths by removing the root component.

        Args:
            path: Path to normalize (absolute or relative)

        Returns:
            Normalized relative path string
        """
        path_obj = Path(path)

        if bypass_normalization:
            return path_obj.as_posix()

        # Store paths as-is - no forced relative conversion
        # This preserves absolute paths which are needed for cross-backend operations
        return path_obj.as_posix()

    def load(self, file_path: Union[str, Path], **kwargs) -> Any:
        key = self._normalize(file_path)

        if key not in self._memory_store:
            raise FileNotFoundError(f"Memory path not found: {file_path}")

        value = self._memory_store[key]
        if value is None:
            raise IsADirectoryError(f"Path is a directory: {file_path}")

        return value

    def save(self, data: Any, output_path: Union[str, Path], **kwargs) -> None:
        key = self._normalize(output_path)

        # Check if parent directory exists (simple flat structure)
        parent_path = self._normalize(Path(key).parent)
        if parent_path != '.' and parent_path not in self._memory_store:
            raise FileNotFoundError(f"Parent path does not exist: {output_path}")

        # Check if file already exists
        if key in self._memory_store:
            raise FileExistsError(f"Path already exists: {output_path}")
        self._memory_store[key] = data

        # Save the file

    def load_batch(self, file_paths: List[Union[str, Path]]) -> List[Any]:
        """
        Load multiple files sequentially using existing load method.

        Args:
            file_paths: List of file paths to load
            **kwargs: Additional arguments passed to load method

        Returns:
            List of loaded data objects in the same order as file_paths
        """
        results = []
        for file_path in file_paths:
            result = self.load(file_path)
            results.append(result)
        return results

    def save_batch(self, data_list: List[Any], output_paths: List[Union[str, Path]]) -> None:
        """
        Save multiple files sequentially using existing save method.

        Args:
            data_list: List of data objects to save
            output_paths: List of destination paths (must match length of data_list)
            **kwargs: Additional arguments passed to save method

        Raises:
            ValueError: If data_list and output_paths have different lengths
        """
        if len(data_list) != len(output_paths):
            raise ValueError(f"data_list length ({len(data_list)}) must match output_paths length ({len(output_paths)})")

        for data, output_path in zip(data_list, output_paths):
            self.save(data, output_path)

    def list_files(
        self,
        directory: Union[str, Path],
        pattern: str = "*",
        extensions: Optional[Set[str]] = None,
        recursive: bool = False
    ) -> List[Path]:
        from fnmatch import fnmatch

        dir_key = self._normalize(directory)

        # Check if directory exists and is a directory
        if dir_key not in self._memory_store:
            raise FileNotFoundError(f"Directory not found: {directory}")
        if self._memory_store[dir_key] is not None:
            raise NotADirectoryError(f"Path is not a directory: {directory}")

        result = []
        dir_prefix = dir_key + "/" if not dir_key.endswith("/") else dir_key

        for path, value in list(self._memory_store.items()):
            # Skip if not under this directory
            if not path.startswith(dir_prefix):
                continue

            # Get relative path from directory
            rel_path = path[len(dir_prefix):]

            # Skip if recursive=False and path has subdirectories
            if not recursive and "/" in rel_path:
                continue

            # Only include files (value is not None)
            if value is not None:
                filename = Path(rel_path).name
                # If pattern is None, match all files
                if pattern is None or fnmatch(filename, pattern):
                    if not extensions or Path(filename).suffix in extensions:
                        # Calculate depth for breadth-first sorting
                        depth = rel_path.count('/')
                        result.append((Path(path), depth))

        # Sort by depth first (breadth-first), then by path for consistency
        result.sort(key=lambda x: (x[1], str(x[0])))

        # Return just the paths
        return [path for path, _ in result]

    def list_dir(self, path: Union[str, Path]) -> List[str]:
        dir_key = self._normalize(path)

        # Check if directory exists and is a directory
        if dir_key not in self._memory_store:
            raise FileNotFoundError(f"Directory not found: {path}")
        if self._memory_store[dir_key] is not None:
            raise NotADirectoryError(f"Path is not a directory: {path}")

        # Find all direct children of this directory
        result = set()
        dir_prefix = dir_key + "/" if not dir_key.endswith("/") else dir_key

        for stored_path in list(self._memory_store.keys()):
            if stored_path.startswith(dir_prefix):
                rel_path = stored_path[len(dir_prefix):]
                # Only direct children (no subdirectories)
                if "/" not in rel_path:
                    result.add(rel_path)
                else:
                    # Add the first directory component
                    first_dir = rel_path.split("/")[0]
                    result.add(first_dir)

        return list(result)

    
    def delete(self, path: Union[str, Path]) -> None:
        """
        Delete a file or empty directory from the in-memory store.

        This method does not support recursive deletion.

        Args:
            path: Virtual path to delete

        Raises:
            FileNotFoundError: If the path does not exist
            IsADirectoryError: If path is a non-empty directory
            StorageResolutionError: For unexpected internal failures
        """
        key = self._normalize(path)

        if key not in self._memory_store:
            raise FileNotFoundError(f"Path not found: {path}")

        # If it's a directory, check if it's empty
        if self._memory_store[key] is None:
            # Check if directory has any children
            dir_prefix = key + "/" if not key.endswith("/") else key
            for stored_path in list(self._memory_store.keys()):
                if stored_path.startswith(dir_prefix):
                    raise IsADirectoryError(f"Cannot delete non-empty directory: {path}")

        try:
            del self._memory_store[key]
        except Exception as e:
            raise StorageResolutionError(f"Failed to delete path from memory store: {path}") from e
    
    def delete_all(self, path: Union[str, Path]) -> None:
        """
        Recursively delete a file, empty directory, or a nested directory tree
        from the in-memory store.

        This method is the only allowed way to recursively delete in memory backend.

        Args:
            path: Virtual path to delete

        Raises:
            FileNotFoundError: If the path does not exist
            StorageResolutionError: If internal deletion fails
        """
        key = self._normalize(path)

        if key not in self._memory_store:
            raise FileNotFoundError(f"Path not found: {path}")

        try:
            # Delete the path itself
            del self._memory_store[key]

            # If it was a directory, delete all children
            dir_prefix = key + "/" if not key.endswith("/") else key
            keys_to_delete = [k for k in list(self._memory_store.keys()) if k.startswith(dir_prefix)]
            for k in keys_to_delete:
                del self._memory_store[k]

        except Exception as e:
            raise StorageResolutionError(f"Failed to recursively delete path: {path}") from e

    def ensure_directory(self, directory: Union[str, Path]) -> PurePosixPath:
        key = self._normalize(directory)
        self._prefixes.add(key if key.endswith("/") else key + "/")

        # Create the entire directory hierarchy
        path_obj = Path(key)
        parts = path_obj.parts

        # Create each parent directory in the hierarchy
        for i in range(1, len(parts) + 1):
            partial_path = self._normalize(Path(*parts[:i]))
            if partial_path not in self._memory_store:
                self._memory_store[partial_path] = None  # Directory = None value

        # Return a POSIX-style path object so string conversion preserves
        # forward slashes across platforms (important for Windows CI/tests)
        return PurePosixPath(key)


    def create_symlink(self, source: Union[str, Path], link_name: Union[str, Path], overwrite: bool = False):
        src_key = self._normalize(source)
        link_key = self._normalize(link_name)
        
        # Check source exists
        if src_key not in self._memory_store:
            raise FileNotFoundError(f"Symlink source not found: {source}")
        
        # Check destination parent exists
        link_parent = self._normalize(Path(link_key).parent)
        if link_parent != '.' and link_parent not in self._memory_store:
            raise FileNotFoundError(f"Destination parent path does not exist: {link_name}")
        
        # Check if destination already exists
        if link_key in self._memory_store:
            if not overwrite:
                raise FileExistsError(f"Symlink destination already exists: {link_name}")
            # Remove existing entry if overwrite=True
            del self._memory_store[link_key]
        
        self._memory_store[link_key] = MemorySymlink(target=str(source))

    def is_symlink(self, path: Union[str, Path]) -> bool:
        key = self._normalize(path)
        return isinstance(self._memory_store.get(key), MemorySymlink)

    def exists(self, path: Union[str, Path]) -> bool:
        """
        Check if a path exists in memory storage.

        Args:
            path: Path to check

        Returns:
            bool: True if path exists (as file or directory), False otherwise
        """
        key = self._normalize(path)
        return key in self._memory_store

    def is_file(self, path: Union[str, Path]) -> bool:
        """
        Check if a memory path points to a file.

        Returns:
            bool: True if path exists and is a file, False otherwise
        """
        key = self._normalize(path)

        if key not in self._memory_store:
            return False

        value = self._memory_store[key]
        # File if value is not None (directories have None value)
        return value is not None
    
    def is_dir(self, path: Union[str, Path]) -> bool:
        """
        Check if a memory path points to a directory.

        Args:
            path: Path to check

        Returns:
            bool: True if path exists and is a directory, False otherwise
        """
        key = self._normalize(path)

        if key not in self._memory_store:
            return False

        value = self._memory_store[key]
        # Directory if value is None
        return value is None
    
    def _resolve_path(self, path: Union[str, Path]) -> Optional[Any]:
        """
        Resolves a memory-style virtual path into an in-memory object (file or directory).

        This performs a pure dictionary traversal. It never coerces types or guesses structure.
        If any intermediate path component is missing or not a dict, resolution fails.

        Args:
            path: Memory-style path, e.g., 'root/dir1/file.txt'

        Returns:
            The object at that path (could be dict or content object), or None if not found
        """
        components = str(path).strip("/").split("/")
        current = self._memory_store  # root dict, e.g., {"root": {"file.txt": "data"}}

        for comp in components:
            if not isinstance(current, dict):
                return None  # hit a file too early
            if comp not in current:
                return None
            current = current[comp]

        return current

    def move(self, src: Union[str, Path], dst: Union[str, Path]) -> None:
        """
        Move a file or directory within the memory store. Symlinks are preserved as objects.

        Raises:
            FileNotFoundError: If src path or dst parent path does not exist
            FileExistsError: If destination already exists
            StorageResolutionError: On structure violations
        """
        src_key = self._normalize(src)
        dst_key = self._normalize(dst)
        
        # Check source exists
        if src_key not in self._memory_store:
            raise FileNotFoundError(f"Source not found: {src}")
        
        # Check destination parent exists
        dst_parent = self._normalize(Path(dst_key).parent)
        if dst_parent != '.' and dst_parent not in self._memory_store:
            raise FileNotFoundError(f"Destination parent path does not exist: {dst}")
        
        # Check destination doesn't exist
        if dst_key in self._memory_store:
            raise FileExistsError(f"Destination already exists: {dst}")
        
        # Move the item (works for files and directories)
        self._memory_store[dst_key] = self._memory_store.pop(src_key)
        
        # If moving a directory, also move all files/subdirs under it
        if self._memory_store[dst_key] is None:  # It's a directory
            src_prefix = src_key if src_key.endswith("/") else src_key + "/"
            dst_prefix = dst_key if dst_key.endswith("/") else dst_key + "/"
            
            # Find all items under source directory and move them
            keys_to_move = [k for k in self._memory_store.keys() if k.startswith(src_prefix)]
            for key in keys_to_move:
                rel_path = key[len(src_prefix):]
                new_key = dst_prefix + rel_path
                self._memory_store[new_key] = self._memory_store.pop(key)

    def copy(self, src: Union[str, Path], dst: Union[str, Path]) -> None:
        """
        Copy a file, directory, or symlink within the memory store.
    
        - Respects structural separation (no fallback)
        - Will not overwrite destination
        - Will not create missing parent directories
        - Symlinks are copied as objects
    
        Raises:
            FileNotFoundError: If src does not exist or dst parent is missing
            FileExistsError: If dst already exists
            StorageResolutionError: On invalid structure
        """
        src_key = self._normalize(src)
        dst_key = self._normalize(dst)
        
        # Check source exists
        if src_key not in self._memory_store:
            raise FileNotFoundError(f"Source not found: {src}")
        
        # Check destination parent exists
        dst_parent = self._normalize(Path(dst_key).parent)
        if dst_parent != '.' and dst_parent not in self._memory_store:
            raise FileNotFoundError(f"Destination parent path does not exist: {dst}")
        
        # Check destination doesn't exist
        if dst_key in self._memory_store:
            raise FileExistsError(f"Destination already exists: {dst}")
        
        # Copy the item (deep copy to avoid aliasing)
        self._memory_store[dst_key] = py_copy.deepcopy(self._memory_store[src_key])
        
        # If copying a directory, also copy all files/subdirs under it
        if self._memory_store[dst_key] is None:  # It's a directory
            src_prefix = src_key if src_key.endswith("/") else src_key + "/"
            dst_prefix = dst_key if dst_key.endswith("/") else dst_key + "/"
            
            # Find all items under source directory and copy them
            keys_to_copy = [k for k in self._memory_store.keys() if k.startswith(src_prefix)]
            for key in keys_to_copy:
                rel_path = key[len(src_prefix):]
                new_key = dst_prefix + rel_path
                self._memory_store[new_key] = py_copy.deepcopy(self._memory_store[key])
    
    def stat(self, path: Union[str, Path]) -> Dict[str, Any]:
        """
        Return structural metadata about a memory-backed path.

        Returns:
            dict with keys:
            - 'type': 'file', 'directory', 'symlink', or 'missing'
            - 'path': str(path)
            - 'target': symlink target if applicable
            - 'exists': bool

        Raises:
            StorageResolutionError: On resolution failure
        """
        key = self._normalize(path)
        
        try:
            # Check if path exists in store
            if key not in self._memory_store:
                return {
                    "type": "missing",
                    "path": str(path),
                    "exists": False
                }
            
            obj = self._memory_store[key]
            
            # Check if it's a symlink
            if isinstance(obj, MemorySymlink):
                # Check if symlink target exists
                target_exists = self._resolve_path(obj.target) is not None
                return {
                    "type": "symlink",
                    "path": str(path),
                    "target": obj.target,
                    "exists": target_exists
                }
            
            # Check if it's a directory (None value)
            if obj is None:
                return {
                    "type": "directory",
                    "path": str(path),
                    "exists": True
                }
            
            # Otherwise it's a file
            return {
                "type": "file",
                "path": str(path),
                "exists": True
            }

        except Exception as e:
            raise StorageResolutionError(f"Failed to stat memory path: {path}") from e

    def clear_files_only(self) -> None:
        """
        Clear all files from the memory store while preserving directory structure.

        This method removes all file entries (non-None values) but keeps directory
        entries (None values) intact. This prevents key collisions when reusing
        the same processing context while maintaining the directory structure
        needed for subsequent operations.

        Note:
            - Directories (entries with None values) are preserved
            - Files (entries with non-None values) are deleted
            - Symlinks are also deleted as they are considered file-like objects
            - GPU objects are explicitly deleted before removal
            - Caller is responsible for calling gc.collect() and GPU cleanup after this method
        """
        try:
            # Collect keys and objects to delete (preserve directories)
            files_to_delete = []
            gpu_objects_found = 0

            for key, value in list(self._memory_store.items()):
                # Delete files (non-None values) and symlinks, but keep directories (None values)
                if value is not None:
                    files_to_delete.append(key)

                    # Check if this is a GPU object that needs explicit cleanup
                    if self._is_gpu_object(value):
                        gpu_objects_found += 1
                        self._explicit_gpu_delete(value, key)

            # Delete all file entries from memory store
            for key in files_to_delete:
                del self._memory_store[key]

            logger.debug(f"Cleared {len(files_to_delete)} files from memory backend (including {gpu_objects_found} GPU objects), "
                        f"preserved {len(self._memory_store)} directories")

        except Exception as e:
            raise StorageResolutionError("Failed to clear files from memory store") from e

    def _is_gpu_object(self, obj: Any) -> bool:
        """
        Check if an object is a GPU tensor/array that needs explicit cleanup.

        Args:
            obj: Object to check

        Returns:
            True if object is a GPU tensor/array
        """
        try:
            # Check for PyTorch tensors on GPU
            if hasattr(obj, 'device') and hasattr(obj, 'is_cuda'):
                if obj.is_cuda:
                    return True

            # Check for CuPy arrays
            if hasattr(obj, '__class__') and 'cupy' in str(type(obj)):
                return True

            # Check for other GPU arrays by device attribute
            if hasattr(obj, 'device') and hasattr(obj.device, 'type'):
                if 'cuda' in str(obj.device.type).lower() or 'gpu' in str(obj.device.type).lower():
                    return True

            return False
        except Exception:
            # If we can't determine, assume it's not a GPU object
            return False

    def _explicit_gpu_delete(self, obj: Any, key: str) -> None:
        """
        Explicitly delete a GPU object and clear its memory.

        Args:
            obj: GPU object to delete
            key: Memory backend key for logging
        """
        try:
            # For PyTorch tensors
            if hasattr(obj, 'device') and hasattr(obj, 'is_cuda') and obj.is_cuda:
                device_id = obj.device.index if obj.device.index is not None else 0
                # Move to CPU first to free GPU memory, then delete
                obj_cpu = obj.cpu()
                del obj_cpu
                logger.debug(f"🔥 EXPLICIT GPU DELETE: PyTorch tensor {key} on device {device_id}")
                return

            # For CuPy arrays
            if hasattr(obj, '__class__') and 'cupy' in str(type(obj)):
                # CuPy arrays are automatically freed when deleted
                logger.debug(f"🔥 EXPLICIT GPU DELETE: CuPy array {key}")
                return

            # For other GPU objects
            if hasattr(obj, 'device'):
                logger.debug(f"🔥 EXPLICIT GPU DELETE: GPU object {key} on device {obj.device}")

        except Exception as e:
            logger.warning(f"Failed to explicitly delete GPU object {key}: {e}")

class MemorySymlink:
    def __init__(self, target: str):
        self.target = target  # Must be a normalized key path

    def __repr__(self):
        return f"<MemorySymlink → {self.target}>"
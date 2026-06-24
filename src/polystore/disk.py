# polystore/disk.py
"""
Disk-based storage backend implementation.

This module provides a concrete implementation of the storage backend interfaces
for local disk storage. It strictly enforces VFS boundaries and doctrinal clauses.
"""

import logging
import os
import shutil
import importlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Union

import numpy as np

from .constants import Backend, MemoryType
from .formats import FileFormat
from .base import StorageBackend

logger = logging.getLogger(__name__)


def optional_import(module_name):
    try:
        return importlib.import_module(module_name)
    except ImportError:
        return None

# Optional dependencies at module level (not instance level to avoid pickle issues)
# Skip GPU libraries in subprocess runner mode
if os.getenv("POLYSTORE_SUBPROCESS_NO_GPU") == "1":
    torch = None
    jax = None
    jnp = None
    cupy = None
    tf = None
    logger.info("Subprocess runner mode - skipping GPU library imports in disk backend")
else:
    from .lazy_imports import get_torch, get_jax, get_jnp, get_cupy, get_tf
    torch = get_torch()
    jax = get_jax()
    jnp = get_jnp()
    cupy = get_cupy()
    tf = get_tf()
tifffile = optional_import("tifffile")
imageio = optional_import("imageio.v3")

# Optional arraybridge integration for memory conversion
try:
    from arraybridge import convert_memory as _convert_memory, detect_memory_type as _detect_memory_type
except Exception:
    _convert_memory = None
    _detect_memory_type = None

class FileFormatRegistry:
    def __init__(self):
        self._writers: Dict[str, Callable[[Path, Any], None]] = {}
        self._readers: Dict[str, Callable[[Path], Any]] = {}

    def register(self, ext: str, writer: Callable, reader: Callable):
        ext = ext.lower()
        self._writers[ext] = writer
        self._readers[ext] = reader

    def get_writer(self, ext: str) -> Callable:
        return self._writers[ext.lower()]

    def get_reader(self, ext: str) -> Callable:
        return self._readers[ext.lower()]

    def is_registered(self, ext: str) -> bool:
        return ext.lower() in self._writers and ext.lower() in self._readers


@dataclass(frozen=True, slots=True)
class DiskFileFormatRegistration:
    """One disk file format registration with explicit dependency availability."""

    file_format: FileFormat
    writer: Callable
    reader: Callable


@dataclass(frozen=True, slots=True)
class DiskGlobPattern:
    """Nominal glob pattern used by disk file listing."""

    value: str

    @classmethod
    def from_optional(cls, pattern: Optional[str]) -> "DiskGlobPattern":
        if pattern is None:
            return cls("*")
        if pattern == "":
            raise ValueError("Disk file listing pattern cannot be empty.")
        return cls(pattern)


class DiskStorageBackend(StorageBackend):
    """Disk storage backend with automatic registration."""
    _backend_type = Backend.DISK.value
    def __init__(self):
        self.format_registry = FileFormatRegistry()
        self._register_formats()

    def _register_formats(self):
        """
        Register all file format handlers.

        Uses enum-driven registration to eliminate boilerplate.
        Complex formats (CSV, JSON, TIFF, ROI.ZIP, TEXT) use custom handlers.
        Simple formats (NumPy, Torch, CuPy, JAX, TensorFlow) use library save/load directly.
        """
        format_handlers = self._available_format_registrations()

        # Register all available formats
        for registration in format_handlers:
            # Register all extensions for this format
            for ext in registration.file_format.extensions:
                self.format_registry.register(
                    ext.lower(),
                    registration.writer,
                    registration.reader,
                )

    def _available_format_registrations(self) -> List[DiskFileFormatRegistration]:
        registrations = [
            DiskFileFormatRegistration(FileFormat.NUMPY, np.save, np.load),
            DiskFileFormatRegistration(FileFormat.TEXT, self._text_writer, self._text_reader),
            DiskFileFormatRegistration(FileFormat.JSON, self._json_writer, self._json_reader),
            DiskFileFormatRegistration(FileFormat.CSV, self._csv_writer, self._csv_reader),
            DiskFileFormatRegistration(FileFormat.ROI, self._roi_zip_writer, self._roi_zip_reader),
        ]
        if torch is not None:
            registrations.append(
                DiskFileFormatRegistration(FileFormat.TORCH, torch.save, torch.load)
            )
        if jax is not None and jnp is not None:
            registrations.append(
                DiskFileFormatRegistration(FileFormat.JAX, self._jax_writer, self._jax_reader)
            )
        if cupy is not None:
            registrations.append(
                DiskFileFormatRegistration(FileFormat.CUPY, self._cupy_writer, self._cupy_reader)
            )
        if tf is not None:
            registrations.append(
                DiskFileFormatRegistration(
                    FileFormat.TENSORFLOW,
                    self._tensorflow_writer,
                    self._tensorflow_reader,
                )
            )
        if tifffile is not None:
            registrations.append(
                DiskFileFormatRegistration(FileFormat.TIFF, self._tiff_writer, self._tiff_reader)
            )
        if imageio is not None:
            registrations.append(
                DiskFileFormatRegistration(
                    FileFormat.RASTER_IMAGE,
                    self._image_writer,
                    self._image_reader,
                )
            )
        return registrations

    # Format-specific writer/reader functions (pickleable)
    # Only needed for formats that require special handling beyond library save/load

    def _jax_writer(self, path, data, **kwargs):
        """JAX arrays must be moved to CPU before saving."""
        np.save(path, jax.device_get(data))

    def _jax_reader(self, path):
        """Load NumPy array and convert to JAX."""
        return jnp.array(np.load(path))

    def _cupy_writer(self, path, data, **kwargs):
        """CuPy has its own save format."""
        cupy.save(path, data)

    def _cupy_reader(self, path):
        """Load CuPy array from disk."""
        return cupy.load(path)

    def _tensorflow_writer(self, path, data, **kwargs):
        """TensorFlow uses tensor serialization."""
        tf.io.write_file(path.as_posix(), tf.io.serialize_tensor(data))

    def _tensorflow_reader(self, path):
        """Load and deserialize TensorFlow tensor."""
        return tf.io.parse_tensor(tf.io.read_file(path.as_posix()), out_type=tf.dtypes.float32)

    def _tiff_writer(self, path, data, **kwargs):
        tifffile.imwrite(path, data)

    def _tiff_reader(self, path):
        # For symlinks, try multiple approaches to handle filesystem issues
        path_obj = Path(path)

        if path_obj.is_symlink():
            # First try reading the symlink directly (let OS handle it)
            try:
                return tifffile.imread(str(path))
            except FileNotFoundError:
                # If that fails, try the target path
                try:
                    target_path = path_obj.readlink()
                    return tifffile.imread(str(target_path))
                except FileNotFoundError:
                    # If target doesn't exist, try resolving the symlink
                    resolved_path = path_obj.resolve()
                    return tifffile.imread(str(resolved_path))
        else:
            return tifffile.imread(str(path))

    def _image_writer(self, path, data, **kwargs):
        """Write standard raster images using imageio."""
        imageio.imwrite(path, np.asarray(data))

    def _image_reader(self, path):
        """Read standard raster images using imageio."""
        return imageio.imread(path)

    def _text_writer(self, path, data, **kwargs):
        """Write text data to file. Accepts and ignores extra kwargs for compatibility."""
        path.write_text(str(data))

    def _text_reader(self, path):
        return path.read_text()

    def _json_writer(self, path, data, **kwargs):
        import json
        # Ensure parent directory exists
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(data, indent=2))

    def _json_reader(self, path):
        import json
        return json.loads(path.read_text())

    def _csv_writer(self, path, data, **kwargs):
        import csv
        # Handle pre-formatted CSV strings (from pandas to_csv)
        if isinstance(data, str):
            path.write_text(data)
            return

        # Assume data is a list of rows or a dict
        with path.open('w', newline='') as f:
            if isinstance(data, dict):
                # Write dict as CSV with headers
                writer = csv.DictWriter(f, fieldnames=data.keys())
                writer.writeheader()
                writer.writerow(data)
            elif isinstance(data, list) and len(data) > 0:
                if isinstance(data[0], dict):
                    # List of dicts
                    writer = csv.DictWriter(f, fieldnames=data[0].keys())
                    writer.writeheader()
                    writer.writerows(data)
                else:
                    # List of lists/tuples
                    writer = csv.writer(f)
                    writer.writerows(data)
            else:
                # Fallback: write as single row
                writer = csv.writer(f)
                writer.writerow([data])

    def _roi_zip_writer(self, path, data, **kwargs):
        """Write ROIs to .roi.zip archive. Wrapper for _save_rois."""
        # data should be a list of ROI objects
        self._save_rois(data, path, **kwargs)

    def _roi_zip_reader(self, path, **kwargs):
        """Read ROIs from .roi.zip archive."""
        from .roi import load_rois_from_zip
        return load_rois_from_zip(path)

    def _csv_reader(self, path):
        import csv
        with path.open('r', newline='') as f:
            reader = csv.DictReader(f)
            return list(reader)


    def load(self, file_path: Union[str, Path], **kwargs) -> Any:
        """
        Load data from disk based on explicit content type.

        Args:
            file_path: Path to the file to load
            **kwargs: Additional arguments for the load operation, must include 'content_type'
                      to explicitly specify the type of content to load

        Returns:
            The loaded data

        Raises:
            TypeError: If file_path is not a valid path type or content_type is not specified
            FileNotFoundError: If the file does not exist
            ValueError: If the file cannot be loaded
        """

        disk_path = Path(file_path)

        # Handle double extensions (e.g., .roi.zip, .csv.zip)
        # Check if file has double extension by looking at suffixes
        ext = None
        if len(disk_path.suffixes) >= 2:
            # Try double extension first (e.g., '.roi.zip')
            double_ext = ''.join(disk_path.suffixes[-2:]).lower()
            if self.format_registry.is_registered(double_ext):
                ext = double_ext

        # Fall back to single extension if double extension not registered
        if ext is None:
            ext = disk_path.suffix.lower()

        if not self.format_registry.is_registered(ext):
            raise ValueError(f"No reader registered for extension '{ext}'")

        try:
            reader = self.format_registry.get_reader(ext)
            return reader(disk_path, **kwargs)
        except Exception as e:
            raise ValueError(f"Error loading data from {disk_path}: {e}") from e

    def save(self, data: Any, output_path: Union[str, Path], **kwargs) -> None:
        """
        Save data to disk based on explicit content type.

        Args:
            data: The data to save
            output_path: Path where the data should be saved
            **kwargs: Additional arguments for the save operation, must include 'content_type'
                      to explicitly specify the type of content to save

        Raises:
            TypeError: If output_path is not a valid path type or content_type is not specified
            ValueError: If the data cannot be saved
        """
        from .roi import ROI

        disk_output_path = Path(output_path)

        # Explicit type dispatch for ROI data
        if isinstance(data, list) and len(data) > 0 and isinstance(data[0], ROI):
            # ROI data - save as JSON
            images_dir = kwargs.pop('images_dir', None)
            self._save_rois(data, disk_output_path, images_dir=images_dir, **kwargs)
            return

        ext = disk_output_path.suffix.lower()
        if not self.format_registry.is_registered(ext):
            raise ValueError(f"No writer registered for extension '{ext}'")

        try:
            writer = self.format_registry.get_writer(ext)
            return writer(disk_output_path, data, **kwargs )
        except Exception as e:
            raise ValueError(f"Error saving data to {disk_output_path}: {e}") from e

    def load_batch(self, file_paths: List[Union[str, Path]], **kwargs) -> List[Any]:
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
            result = self.load(file_path, **kwargs)
            results.append(result)
        return results

    def save_batch(self, data_list: List[Any], output_paths: List[Union[str, Path]], **kwargs) -> None:
        """
        Save multiple files sequentially using existing save method.

        Converts GPU arrays to CPU numpy arrays before saving using the optional conversion hooks.

        Args:
            data_list: List of data objects to save
            output_paths: List of destination paths (must match length of data_list)
            **kwargs: Additional arguments passed to save method

        Raises:
            ValueError: If data_list and output_paths have different lengths
        """
        if len(data_list) != len(output_paths):
            raise ValueError(f"data_list length ({len(data_list)}) must match output_paths length ({len(output_paths)})")

        # Convert GPU arrays to CPU numpy arrays using conversion hooks
        cpu_data_list = []
        for data in data_list:
            if _detect_memory_type:
                source_type = _detect_memory_type(data)
                if source_type == MemoryType.NUMPY.value:
                    cpu_data_list.append(data)
                elif _convert_memory:
                    numpy_data = _convert_memory(
                        data=data,
                        source_type=source_type,
                        target_type=MemoryType.NUMPY.value,
                        gpu_id=0,
                    )
                    cpu_data_list.append(numpy_data)
                else:
                    cpu_data_list.append(np.asarray(data))
            else:
                cpu_data_list.append(np.asarray(data))

        # Save converted data using existing save method
        for cpu_data, output_path in zip(cpu_data_list, output_paths):
            self.save(cpu_data, output_path, **kwargs)

    def list_files(self, directory: Union[str, Path], pattern: Optional[str] = None,
                  extensions: Optional[Set[str]] = None, recursive: bool = False) -> List[Union[str,Path]]:
        """
        List files on disk, optionally filtering by pattern and extensions.

        Args:
            directory: Directory to search.
            pattern: Optional glob pattern to match filenames.
            extensions: Optional set of file extensions to filter by (e.g., {'.tif', '.png'}).
                        Extensions should include the dot and are case-insensitive.
            recursive: Whether to search recursively.

        Returns:
            List of paths to matching files.

        Raises:
            TypeError: If directory is not a valid path type
            FileNotFoundError: If the directory does not exist
        """
        disk_directory = Path(directory)

        if not disk_directory.is_dir():
            raise ValueError(f"Path is not a directory: {disk_directory}")

        # Use appropriate search strategy based on recursion
        if recursive:
            # Use breadth-first traversal to prioritize shallower files
            files = self._list_files_breadth_first(disk_directory, pattern)
        else:
            glob_pattern = DiskGlobPattern.from_optional(pattern).value
            # Include both regular files and symlinks (even broken ones)
            files = [p for p in disk_directory.glob(glob_pattern) if p.is_file() or p.is_symlink()]

        # Filter out macOS metadata files (._* files) that interfere with parsing
        files = [f for f in files if not f.name.startswith('._')]

        # Filter by extensions if provided
        if extensions:
            # Convert extensions to lowercase for case-insensitive comparison
            lowercase_extensions = {ext.lower() for ext in extensions}
            files = [f for f in files if f.suffix.lower() in lowercase_extensions]

        # Return paths as strings
        return [str(f) for f in files]

    def _list_files_breadth_first(self, directory: Path, pattern: Optional[str] = None) -> List[Path]:
        """
        List files using breadth-first traversal to prioritize shallower files.

        This ensures that files in the root directory are found before files
        in subdirectories, which is important for metadata detection.

        Args:
            directory: Root directory to search
            pattern: Optional glob pattern to match filenames

        Returns:
            List of file paths sorted by depth (shallower first)
        """
        from collections import deque

        files = []
        # Use deque for breadth-first traversal
        dirs_to_search = deque([(directory, 0)])  # (path, depth)

        while dirs_to_search:
            current_dir, depth = dirs_to_search.popleft()

            try:
                # Get all entries in current directory
                for entry in current_dir.iterdir():
                    if entry.is_file():
                        # Filter out macOS metadata files (._* files) that interfere with parsing
                        if entry.name.startswith('._'):
                            continue
                        # Check if file matches pattern
                        if pattern is None or entry.match(pattern):
                            files.append((entry, depth))
                    elif entry.is_dir():
                        # Add subdirectory to queue for later processing
                        dirs_to_search.append((entry, depth + 1))
            except (PermissionError, OSError):
                # Skip directories we can't read
                continue

        # Sort by depth first, then by path for consistent ordering
        files.sort(key=lambda x: (x[1], str(x[0])))

        # Return just the paths
        return [file_path for file_path, _ in files]

    def list_dir(self, path: Union[str, Path]) -> List[str]:
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"Path does not exist: {path}")
        if not path.is_dir():
            raise NotADirectoryError(f"Not a directory: {path}")
        return [entry.name for entry in path.iterdir()]

        
    def delete(self, path: Union[str, Path]) -> None:
        """
        Delete a file or empty directory at the given disk path.

        Args:
            path: Path to delete

        Raises:
            FileNotFoundError: If path does not exist
            IsADirectoryError: If path is a directory and not empty
            StorageResolutionError: If deletion fails for unknown reasons
        """
        path = Path(path)

        if not path.exists():
            raise FileNotFoundError(f"Cannot delete: path does not exist: {path}")

        try:
            if path.is_dir():
                # Do not allow recursive deletion
                path.rmdir()  # will raise OSError if directory is not empty
            else:
                path.unlink()
        except IsADirectoryError:
            raise
        except OSError as e:
            raise IsADirectoryError(f"Cannot delete non-empty directory: {path}") from e
        except Exception as e:
            raise StorageResolutionError(f"Failed to delete {path}") from e
    
    def delete_all(self, path: Union[str, Path]) -> None:
        """
        Recursively delete a file or directory and all its contents from disk.

        Args:
            path: Filesystem path to delete

        Raises:
            FileNotFoundError: If the path does not exist
            StorageResolutionError: If deletion fails for any reason
        """
        path = Path(path)

        if not path.exists():
            raise FileNotFoundError(f"Path does not exist: {path}")

        try:
            if path.is_file():
                path.unlink()
            else:
                # Safe, recursive removal of directories
                import shutil
                shutil.rmtree(path)
        except Exception as e:
            raise StorageResolutionError(f"Failed to recursively delete: {path}") from e


    def ensure_directory(self, directory: Union[str, Path]) -> Union[str, Path]:
        """
        Ensure a directory exists on disk.

        Args:
            directory: Path to the directory to ensure exists

        Returns:
            Path to the directory

        Raises:
            TypeError: If directory is not a valid path type
            ValueError: If there is an error creating the directory
        """
        try:
            disk_directory = Path(directory)
            disk_directory.mkdir(parents=True, exist_ok=True)
            return directory
        except OSError as e:
            # Propagate the error with additional context
            raise ValueError(f"Error creating directory {disk_directory}: {e}") from e

    def exists(self, path: Union[str, Path]) -> bool:
        return Path(path).exists()

    def create_symlink(self, source: Union[str, Path], link_name: Union[str, Path], overwrite: bool = False):
        source = Path(source).resolve()
        link_name = Path(link_name)  # Don't resolve link_name - we want the actual symlink path

        if not source.exists():
            raise FileNotFoundError(f"Source path does not exist: {source}")

        # Check if target exists and handle overwrite policy
        if link_name.exists() or link_name.is_symlink():
            if not overwrite:
                raise FileExistsError(f"Target already exists: {link_name}")
            link_name.unlink()  # Remove existing file/symlink only if overwrite=True

        link_name.parent.mkdir(parents=True, exist_ok=True)
        # On Windows, symlink_to() requires target_is_directory to be set correctly
        # On Unix, this parameter is ignored, so it's safe to always specify it
        link_name.symlink_to(source, target_is_directory=source.is_dir())


    def is_symlink(self, path: Union[str, Path]) -> bool:
        return Path(path).is_symlink()


    def is_file(self, path: Union[str, Path]) -> bool:
        path = Path(path)

        if not path.exists():
            raise FileNotFoundError(f"Path does not exist: {path}")

        # Resolve symlinks and return True only if final target is a file
        resolved = path.resolve(strict=True)

        if resolved.is_dir():
            raise IsADirectoryError(f"Path is a directory: {path}")

        return resolved.is_file()

    def is_dir(self, path: Union[str, Path]) -> bool:
        """
        Check if a given disk path is a directory.

        Follows filesystem symlinks to determine the actual resolved structure.

        Args:
            path: Filesystem path (absolute or relative)

        Returns:
            bool: True if path resolves to a directory

        Raises:
            FileNotFoundError: If the path or symlink target does not exist
            NotADirectoryError: If the resolved target is not a directory
        """
        path = Path(path)

        if not path.exists():
            raise FileNotFoundError(f"Path does not exist: {path}")

        # Follow symlinks to final real target
        resolved = path.resolve(strict=True)

        if not resolved.is_dir():
            raise NotADirectoryError(f"Path is not a directory: {path}")

        return True

    def move(self, src: Union[str, Path], dst: Union[str, Path]) -> None:
        """
        Move a file or directory on disk. Follows symlinks and performs overwrite-safe move.

        Raises:
            FileNotFoundError: If source does not exist
            FileExistsError: If destination already exists
            StorageResolutionError: On failure to move
        """
        import shutil
        from pathlib import Path

        src = Path(src)
        dst = Path(dst)

        if not src.exists():
            raise FileNotFoundError(f"Source path does not exist: {src}")
        if dst.exists():
            raise FileExistsError(f"Destination already exists: {dst}")

        try:
            shutil.move(str(src), str(dst))
        except Exception as e:
            raise StorageResolutionError(f"Failed to move {src} to {dst}") from e
    
    def stat(self, path: Union[str, Path]) -> Dict[str, Any]:
        """
        Return structural metadata about a disk-backed path.

        Returns:
            dict with keys:
            - 'type': 'file', 'directory', 'symlink', or 'missing'
            - 'path': str(path)
            - 'target': resolved target if symlink
            - 'exists': bool

        Raises:
            StorageResolutionError: On access or resolution failure
        """
        path_str = str(path)
        try:
            if not os.path.lexists(path_str):  # includes broken symlinks
                return {
                    "type": "missing",
                    "path": path_str,
                    "exists": False
                }

            if os.path.islink(path_str):
                try:
                    resolved = os.readlink(path_str)
                    target_exists = os.path.exists(path_str)
                except OSError as e:
                    raise StorageResolutionError(f"Failed to resolve symlink: {path}") from e

                return {
                    "type": "symlink",
                    "path": path_str,
                    "target": resolved,
                    "exists": target_exists
                }

            if os.path.isdir(path_str):
                return {
                    "type": "directory",
                    "path": path_str,
                    "exists": True
                }

            if os.path.isfile(path_str):
                return {
                    "type": "file",
                    "path": path_str,
                    "exists": True
                }

            raise StorageResolutionError(f"Unknown filesystem object at: {path_str}")

        except Exception as e:
            raise StorageResolutionError(f"Failed to stat disk path: {path}") from e

    def copy(self, src: Union[str, Path], dst: Union[str, Path]) -> None:
        """
        Copy a file or directory to a new location.
    
        - Does not overwrite destination.
        - Will raise if destination exists.
        - Supports file-to-file and dir-to-dir copies.
    
        Raises:
            FileExistsError: If destination already exists
            FileNotFoundError: If source is missing
            StorageResolutionError: On structural failure
        """
        src = Path(src)
        dst = Path(dst)
    
        if not src.exists():
            raise FileNotFoundError(f"Source does not exist: {src}")
        if dst.exists():
            raise FileExistsError(f"Destination already exists: {dst}")
    
        try:
            if src.is_dir():
                shutil.copytree(src, dst)
            else:
                shutil.copy2(src, dst)
        except Exception as e:
            raise StorageResolutionError(f"Failed to copy {src} → {dst}") from e

    def _save_rois(self, rois: List, output_path: Path, images_dir: str = None, **kwargs) -> str:
        """Save ROIs as .roi.zip archive (ImageJ standard format).

        Args:
            rois: List of ROI objects
            output_path: Output path (e.g., /disk/plate_001/step_7_results/A01_rois_step7.roi.zip)
            images_dir: Images directory path (unused for disk backend)

        Returns:
            Path where ROIs were saved
        """
        import zipfile
        from .roi import (
            ROI_ZIP_METADATA_MEMBER,
            ROIArchivePath,
            roi_zip_metadata_payload,
        )
        from .roi_converters import FijiROIConverter

        output_path = ROIArchivePath.from_output_path(output_path).path

        # Ensure output directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Create .roi.zip archive
        roi_members = FijiROIConverter.rois_to_imagej_members(rois)
        metadata_by_filename = {}

        with zipfile.ZipFile(output_path, 'w', zipfile.ZIP_DEFLATED) as zf:
            for roi_count, member in enumerate(roi_members, start=1):
                roi_filename = f"{roi_count:04d}.roi"
                imagej_roi = member.imagej_roi
                imagej_roi.name = f"ROI_{roi_count}"
                metadata_by_filename[roi_filename] = member.metadata
                zf.writestr(roi_filename, imagej_roi.tobytes())
            if metadata_by_filename:
                zf.writestr(
                    ROI_ZIP_METADATA_MEMBER,
                    roi_zip_metadata_payload(metadata_by_filename),
                )

        logger.info(f"Saved {len(roi_members)} ROIs to .roi.zip archive: {output_path}")
        return str(output_path)


DiskBackend = DiskStorageBackend

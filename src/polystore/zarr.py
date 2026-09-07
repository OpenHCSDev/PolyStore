# polystore/zarr.py
"""
Zarr storage backend module for polystore.

This module provides a Zarr-backed implementation of the MicroscopyStorageBackend interface.
It stores data in a Zarr store on disk and supports overlay operations
for materializing data to disk when needed.
"""

import fnmatch
import logging
import os
import shutil
from collections.abc import Iterator, Mapping, Sequence
from pathlib import Path
from typing import Any, Optional

import numpy as np
import zarr
from numcodecs.abc import Codec
from ome_zarr.format import FormatV04
from zarr.storage import LocalStore

from .array_payload import storage_numpy_array
from .atomic import file_lock
from .base import PicklableBackend, StorageBackend
from .config import ZarrConfig
from .constants import Backend
from .exceptions import StorageResolutionError
from .ome_zarr_metadata import OmeZarrLocation
from .zarr_batch import (
    ATTR_DIMENSIONS,
    ATTR_FILENAME_MAP,
    ATTR_IMAGE_COORDINATE,
    ATTR_OUTPUT_PATHS,
    ZarrBatchLayout,
)

logger = logging.getLogger(__name__)

DEFAULT_PLATE_NAME = os.getenv("POLYSTORE_PLATE_NAME", "Polystore_Plate")
DISK_PASSTHROUGH_EXTENSIONS = (".json", ".csv", ".txt", ".roi.zip", ".zip")


def _get_attr(attrs: Any, key: str):
    if key in attrs:
        return attrs[key]
    return None


def _declared_well_image_paths(well_group: zarr.Group) -> tuple[str, ...]:
    """Return declared HCS images, with the historical group-zero fallback."""

    location = OmeZarrLocation(well_group.store.root / well_group.path)
    well_metadata = _get_attr(location.root_attrs, "well")
    if well_metadata is None:
        return ("0",) if "0" in well_group.group_keys() else ()
    if not isinstance(well_metadata, Mapping):
        raise StorageResolutionError("OME-Zarr well metadata must be a mapping")
    images = well_metadata.get("images")
    if not isinstance(images, Sequence) or isinstance(images, (str, bytes)):
        raise StorageResolutionError("OME-Zarr well images must be a sequence")

    image_paths: list[str] = []
    for image in images:
        if not isinstance(image, Mapping) or not isinstance(image.get("path"), str):
            raise StorageResolutionError("Each OME-Zarr well image must declare a string path")
        image_paths.append(image["path"])
    if len(set(image_paths)) != len(image_paths):
        raise StorageResolutionError("OME-Zarr well image paths must be unique")

    missing_paths = tuple(
        path
        for path in image_paths
        if path not in well_group or not isinstance(well_group[path], zarr.Group)
    )
    if missing_paths:
        raise StorageResolutionError(
            f"OME-Zarr well declares missing image groups: {missing_paths!r}"
        )
    return tuple(image_paths)


def _plate_image_arrays(root: zarr.Group) -> Iterator[zarr.Array]:
    """Project the plate's declared wells and image resolutions once."""
    location = OmeZarrLocation(root.store.root / root.path)
    for well in location.root_attrs["plate"]["wells"]:
        well_group = root[well["path"]]
        for image_path in _declared_well_image_paths(well_group):
            yield OmeZarrLocation(root.store.root / well_group.path / image_path).base_array


class ZarrStorageBackend(StorageBackend, PicklableBackend):
    """Zarr storage backend with automatic registration."""

    _backend_type = Backend.ZARR.value
    output_format = FormatV04()
    supports_arbitrary_files = False  # Class attribute: zarr only handles array data
    """
    Zarr storage backend implementation with configurable compression.

    This class provides a concrete implementation of the storage backend interfaces
    for Zarr storage. It stores data in a Zarr store on disk with configurable
    compression algorithms and settings.

    Features:
    - Single-chunk batch operations for 40x performance improvement
    - Configurable compression (Blosc, Zlib, LZ4, Zstd, or none)
    - Configurable compression levels
    - Full path mapping for batch operations

    Limitations:
    - Only supports array data (numpy arrays)
    - Cannot save arbitrary file formats (CSV, ROI.ZIP, etc.)
    """

    def __init__(self, zarr_config: Optional["ZarrConfig"] = None):
        """
        Initialize Zarr backend with ZarrConfig.

        Args:
            zarr_config: ZarrConfig dataclass with all zarr settings (uses defaults if None)
        """
        if zarr_config is None:
            zarr_config = ZarrConfig()

        self._configure(zarr_config)

    def _configure(self, zarr_config: "ZarrConfig") -> None:
        self.config = zarr_config

    @property
    def compressor(self) -> Codec | None:
        """Derive the output codec from the current configuration owner."""
        return self.config.compressor_factory.create(
            self.config.compression_level,
            shuffle=True,
        )

    def get_connection_params(self) -> dict[str, Any] | None:
        return {"zarr_config": self.config}

    def set_connection_params(self, params: dict[str, Any] | None) -> None:
        if params is None:
            self._configure(ZarrConfig())
            return
        self._configure(params["zarr_config"])

    @staticmethod
    def _as_cpu_array(data: Any) -> Any:
        return storage_numpy_array(data)

    def _calculate_chunks(self, data_shape: tuple[int, ...]) -> tuple[int, ...]:
        """
        Calculate chunk shape based on configured strategy.

        Args:
            data_shape: Array shape with arbitrary leading semantic axes and
                trailing ``y, x`` pixel axes.

        Returns:
            Chunk shape tuple
        """
        from .config import ZarrChunkStrategy

        match self.config.chunk_strategy:
            case ZarrChunkStrategy.WELL:
                # Single chunk for entire well (current behavior, optimal for batch I/O)
                return data_shape
            case ZarrChunkStrategy.FILE:
                # Each original plane is compressed separately regardless of
                # the number of declared leading axes.
                return (1,) * (len(data_shape) - 2) + data_shape[-2:]

    def _split_store_and_key(self, path: str | Path) -> tuple[LocalStore, str]:
        """
        Split path into zarr store and key.

        The zarr store is always the directory containing the image files, regardless of backend.
        For example:
        - "/path/to/plate_outputs/images/A01.tif" → Store: "/path/to/plate_outputs/images", Key: "A01.tif"
        - "/path/to/plate.zarr/images/A01.tif" → Store: "/path/to/plate.zarr/images", Key: "A01.tif"

        The images directory itself becomes the zarr store - zarr files are added within it.
        A zarr store doesn't need to have a folder name ending in .zarr.

        Chunk encoding is owned by the declared output format, not the store.
        """
        path = Path(path)

        # If path has a file extension (like .tif), the parent directory is the zarr store
        if path.suffix:
            # File path - parent directory (e.g., "images") is the zarr store
            store_path = path.parent
            relative_key = path.name
        else:
            # Directory path - treat as zarr store
            store_path = path
            relative_key = ""

        store = LocalStore(store_path)
        return store, relative_key

    def save(self, data: Any, output_path: str | Path, **kwargs):
        """
        Save data to Zarr at the given output_path.

        Will only write if the key does not already exist.
        Will NOT overwrite or delete existing data.

        Raises:
            FileExistsError: If destination key already exists
            StorageResolutionError: If creation fails
        """
        output_path_text = str(output_path)
        if output_path_text.endswith(DISK_PASSTHROUGH_EXTENSIONS):
            from .backend_registry import get_backend_instance

            disk_backend = get_backend_instance(Backend.DISK.value)
            disk_backend.ensure_directory(Path(output_path_text).parent)
            disk_backend.save(data, output_path, **kwargs)
            return

        store, key = self._split_store_and_key(output_path)
        group = zarr.open_group(store=store, mode="a", zarr_format=self.output_format.zarr_format)

        if key in group:
            raise FileExistsError(f"Zarr key already exists: {output_path}")

        cpu_data = self._as_cpu_array(data)
        chunks = kwargs.get("chunks")
        if chunks is None:
            chunks = self._auto_chunks(
                cpu_data,
                chunk_divisor=kwargs.get("chunk_divisor", 1),
            )

        try:
            # Create array with correct shape and dtype, then assign data
            array = group.create_array(
                name=key,
                shape=cpu_data.shape,
                dtype=cpu_data.dtype,
                chunks=chunks,
                compressor=kwargs.get("compressor", self.compressor),
                chunk_key_encoding=self.output_format.chunk_key_encoding,
                overwrite=False,  # 🔒 Must be False by doctrine
            )
            array[:] = cpu_data
        except Exception as e:
            raise StorageResolutionError(f"Failed to save to Zarr: {output_path}") from e

    def load_batch(self, file_paths: list[str | Path], **kwargs) -> list[Any]:
        """
        Load from zarr array using filename mapping.

        Args:
            file_paths: List of file paths to load
            **kwargs: Additional arguments (zarr_config not needed)

        Returns:
            List of loaded data objects in same order as file_paths

        Raises:
            FileNotFoundError: If expected zarr store not found
            KeyError: If filename not found in filename_map
        """
        if not file_paths:
            return []

        # Use _split_store_and_key to get store path from first file path
        store, _ = self._split_store_and_key(file_paths[0])
        store_path = store.root

        # FAIL LOUD: Store must exist
        if not store_path.exists():
            raise FileNotFoundError(f"Expected zarr store not found: {store_path}")
        root = zarr.open_group(store=store, mode="r")

        results = [None] * len(file_paths)
        for field_array in _plate_image_arrays(root):
            filename_map = _get_attr(field_array.attrs, ATTR_FILENAME_MAP)
            if filename_map is None:
                continue
            matches = tuple(
                (index, filename_map[Path(path).name])
                for index, path in enumerate(file_paths)
                if Path(path).name in filename_map
            )
            if not matches:
                continue
            all_well_data = field_array[:]
            for file_pos, coordinates in matches:
                results[file_pos] = all_well_data[
                    *(int(index) for index in coordinates),
                    slice(None),
                    slice(None),
                ]

        missing_paths = [
            str(path) for path, result in zip(file_paths, results, strict=True) if result is None
        ]
        if missing_paths:
            raise KeyError(
                f"Zarr filename mapping does not contain requested paths: {missing_paths!r}"
            )

        logger.debug(
            "Loaded %d images from zarr store at %s",
            len(file_paths),
            store_path,
        )
        return results

    def save_batch(self, data_list: list[Any], output_paths: list[str | Path], **kwargs) -> None:
        """Save multiple images using ome-zarr-py for proper OME-ZARR compliance with multi-dimensional support.

        Args:
            data_list: List of image data to save
            output_paths: List of output file paths
            **kwargs: Must include ``chunk_name``, ``batch_layout``, ``row``,
                and ``col``.
        """

        # Keep Dask and writer imports on the storage execution boundary.
        from ome_zarr.writer import write_multiscales_metadata, write_well_metadata

        # Extract required parameters from kwargs
        chunk_name = kwargs.get("chunk_name")
        batch_layout = kwargs.get("batch_layout")
        row = kwargs.get("row")
        col = kwargs.get("col")

        # Validate required parameters
        if chunk_name is None:
            raise ValueError("chunk_name must be provided")
        if not isinstance(batch_layout, ZarrBatchLayout):
            raise TypeError("batch_layout must be a ZarrBatchLayout")
        if row is None:
            raise ValueError("row must be provided")
        if col is None:
            raise ValueError("col must be provided")

        if not data_list:
            logger.warning(f"Empty data list for chunk {chunk_name}")
            return

        if len(data_list) != len(output_paths):
            raise ValueError(
                "Zarr batch data and output paths must have equal lengths: "
                f"got {len(data_list)} and {len(output_paths)}"
            )
        if len(data_list) != len(batch_layout.item_coordinates):
            raise ValueError(
                "Zarr batch data must match the declared item coordinates: "
                f"got {len(data_list)} item(s) and "
                f"{len(batch_layout.item_coordinates)} coordinate(s)"
            )

        # Use _split_store_and_key to get store path from first output path
        store, _ = self._split_store_and_key(output_paths[0])
        store_path = store.root

        logger.debug(
            f"Saving batch for chunk {chunk_name} with {len(data_list)} images to row={row}, col={col}"
        )

        # Convert GPU arrays to CPU arrays before saving
        cpu_data_list = [self._as_cpu_array(data) for data in data_list]

        sample_image = cpu_data_list[0]
        if sample_image.ndim != 2:
            raise ValueError(
                "Zarr batch items must be two-dimensional image planes; "
                f"got shape {sample_image.shape!r}"
            )
        mismatched_shapes = [
            array.shape for array in cpu_data_list if array.shape != sample_image.shape
        ]
        if mismatched_shapes:
            raise ValueError(
                "Zarr batch items must share one image-plane shape; "
                f"expected {sample_image.shape!r}, got {mismatched_shapes!r}"
            )
        height, width = sample_image.shape

        # Ensure parent directory exists
        store_path.parent.mkdir(parents=True, exist_ok=True)

        root = zarr.open_group(store=store, mode="a", zarr_format=self.output_format.zarr_format)

        # Write plate metadata with locking to prevent concurrent corruption
        # Always enabled for OME-ZARR HCS compliance
        self._ensure_plate_metadata_with_lock(
            root,
            row,
            col,
            store_path,
            batch_layout.image_count,
        )

        # Create HCS-compliant structure: plate/row/col/field/resolution
        # Create row group if it doesn't exist
        if row not in root:
            row_group = root.create_group(row)
        else:
            row_group = root[row]

        # Create well group (remove existing if present to allow overwrite)
        if col in row_group:
            del row_group[col]
        well_group = row_group.create_group(col)

        # Add HCS well metadata
        image_names = tuple(str(index) for index in range(batch_layout.image_count))
        axes = list(batch_layout.ngff_axes)

        target_shape = (*batch_layout.array_shape, height, width)
        axes_names = [ax["name"] for ax in axes]
        logger.info("Dimensions: shape=%s, axes=%s", target_shape, axes_names)

        write_well_metadata(well_group, list(image_names), fmt=self.output_format)
        for image_index, image_name in enumerate(image_names):
            field_group = well_group.require_group(image_name)
            image_items = tuple(
                (data, path, coordinate)
                for data, path, coordinate in zip(
                    cpu_data_list,
                    output_paths,
                    batch_layout.item_coordinates,
                    strict=True,
                )
                if batch_layout.image_index(coordinate) == image_index
            )
            reshaped_data = np.empty(target_shape, dtype=sample_image.dtype)
            for data, _path, coordinate in image_items:
                reshaped_data[*batch_layout.array_coordinate(coordinate), :, :] = data

            field_array = field_group.create_array(
                "0",
                shape=reshaped_data.shape,
                dtype=reshaped_data.dtype,
                chunks=self._calculate_chunks(reshaped_data.shape),
                compressor=self.compressor,
                chunk_key_encoding=self.output_format.chunk_key_encoding,
            )
            field_array[:] = reshaped_data
            transformations = self.output_format.generate_coordinate_transformations(
                [field_array.shape]
            )
            write_multiscales_metadata(
                field_group,
                datasets=[
                    {
                        "path": field_array.basename,
                        "coordinateTransformations": transformations[0],
                    }
                ],
                fmt=self.output_format,
                axes=axes,
            )
            field_array.attrs[ATTR_FILENAME_MAP] = {
                Path(path).name: batch_layout.array_coordinate(coordinate)
                for _data, path, coordinate in image_items
            }
            field_array.attrs[ATTR_OUTPUT_PATHS] = [
                str(path) for _data, path, _coordinate in image_items
            ]
            field_array.attrs[ATTR_DIMENSIONS] = batch_layout.dimensions_attribute()
            field_array.attrs[ATTR_IMAGE_COORDINATE] = batch_layout.image_coordinate_attribute(
                image_items[0][2]
            )

        logger.debug(f"Successfully saved batch for chunk {chunk_name}")

        # Aggressive memory cleanup
        del cpu_data_list
        import gc

        gc.collect()

    def _ensure_plate_metadata_with_lock(
        self,
        root: zarr.Group,
        row: str,
        col: str,
        store_path: Path,
        field_count: int,
    ) -> None:
        """Ensure plate-level metadata includes ALL existing wells with file locking."""
        lock_path = store_path.with_suffix(".metadata.lock")

        try:
            with file_lock(lock_path):
                self._ensure_plate_metadata(root, row, col, field_count)
        except Exception as e:
            logger.error(f"Failed to update plate metadata with lock: {e}")
            raise

    def _ensure_plate_metadata(
        self,
        root: zarr.Group,
        row: str,
        col: str,
        field_count: int,
    ) -> None:
        """Ensure plate-level metadata includes ALL existing wells in the store."""

        from ome_zarr.writer import write_plate_metadata

        # Scan the store for all existing wells
        all_rows = set()
        all_cols = set()
        all_wells = []
        maximum_field_count = field_count

        for row_name in root.group_keys():
            if isinstance(root[row_name], zarr.Group):  # Ensure it's a row group
                row_group = root[row_name]
                all_rows.add(row_name)

                for col_name in row_group.group_keys():
                    if isinstance(row_group[col_name], zarr.Group):  # Ensure it's a well group
                        all_cols.add(col_name)
                        well_path = f"{row_name}/{col_name}"
                        all_wells.append(well_path)
                        maximum_field_count = max(
                            maximum_field_count,
                            len(tuple(row_group[col_name].group_keys())),
                        )

        # Include the current well being added (might not exist yet)
        all_rows.add(row)
        all_cols.add(col)
        current_well_path = f"{row}/{col}"
        if current_well_path not in all_wells:
            all_wells.append(current_well_path)

        # Sort for consistent ordering
        sorted_rows = sorted(all_rows)
        sorted_cols = sorted(all_cols)
        sorted_wells = sorted(all_wells)

        # Build wells metadata with proper indices
        wells_metadata = []
        for well_path in sorted_wells:
            well_row, well_col = well_path.split("/")
            row_index = sorted_rows.index(well_row)
            col_index = sorted_cols.index(well_col)
            wells_metadata.append(
                {"path": well_path, "rowIndex": row_index, "columnIndex": col_index}
            )

        # Add acquisition metadata for HCS compliance
        acquisitions = [
            {"id": 0, "name": "default_acquisition", "maximumfieldcount": maximum_field_count}
        ]

        # Write complete HCS plate metadata
        write_plate_metadata(
            root,
            sorted_rows,
            sorted_cols,
            wells_metadata,
            acquisitions=acquisitions,
            field_count=maximum_field_count,
            name=DEFAULT_PLATE_NAME,
            fmt=self.output_format,
        )

    def load(self, file_path: str | Path, **kwargs) -> Any:
        """
        Load a single file from zarr store.

        For OME-ZARR structure with filename mapping, delegates to load_batch.
        For legacy flat structure or direct keys, uses direct key lookup.

        Args:
            file_path: Path to file to load
            **kwargs: Additional arguments

        Returns:
            Loaded array data

        Raises:
            FileNotFoundError: If file not found in zarr store
        """
        store, key = self._split_store_and_key(file_path)
        location = OmeZarrLocation(store, mode="r")
        group = location.group

        # Check if this is OME-ZARR structure with filename mapping
        if "plate" in location.root_attrs:
            # OME-ZARR structure: use load_batch which understands filename mapping
            result = self.load_batch([file_path], **kwargs)
            if not result:
                raise FileNotFoundError(f"File not found in OME-ZARR store: {file_path}")
            return result[0]

        # Legacy flat structure: direct key lookup with symlink resolution
        _, array = self._resolve_symlink(group, key)
        return array[:]

    def list_files(
        self,
        directory: str | Path,
        pattern: str | None = None,
        extensions: set[str] | None = None,
        recursive: bool = False,
    ) -> list[Path]:
        """
        List all file-like entries (i.e. arrays) in a Zarr store, optionally filtered.
        Returns filenames from array attributes (output_paths) if available.
        """

        store, relative_key = self._split_store_and_key(directory)
        result: list[Path] = []

        def _matches_filters(name: str) -> bool:
            if pattern and not fnmatch.fnmatch(name, pattern):
                return False
            if extensions:
                return any(name.lower().endswith(ext.lower()) for ext in extensions)
            return True

        try:
            # Open zarr group and traverse OME-ZARR structure
            location = OmeZarrLocation(store, mode="r")
            group = location.group

            # Check if this is OME-ZARR structure (has plate metadata)
            if "plate" in location.root_attrs:
                arrays = _plate_image_arrays(group)
            else:
                arrays = (array for _name, array in group.arrays())
            for array in arrays:
                output_paths = _get_attr(array.attrs, ATTR_OUTPUT_PATHS)
                if output_paths is not None:
                    result.extend(
                        Path(filename)
                        for filename in output_paths
                        if _matches_filters(Path(filename).name)
                    )

        except Exception as e:
            raise StorageResolutionError(f"Failed to list zarr arrays: {e}") from e

        return result

    def list_dir(self, path: str | Path) -> list[str]:
        node = self._resolved_node(path)
        if not isinstance(node, zarr.Group):
            raise NotADirectoryError(f"Zarr path is not a directory: {path}")
        return sorted(node.keys())

    def delete(self, path: str | Path) -> None:
        """
        Delete a Zarr array (file) or empty group (directory) at the given path.

        Args:
            path: Zarr path or URI

        Raises:
            FileNotFoundError: If path does not exist
            IsADirectoryError: If path is a non-empty group
            StorageResolutionError: For unexpected failures
        """
        # Passthrough to disk backend for text files (JSON, CSV, TXT)
        path_str = str(path)
        if path_str.endswith((".json", ".csv", ".txt")):
            from .backend_registry import get_backend_instance

            disk_backend = get_backend_instance(Backend.DISK.value)
            return disk_backend.delete(path)

        path = str(path)

        if not os.path.exists(path):
            raise FileNotFoundError(f"Zarr path does not exist: {path}")

        try:
            zarr_obj = zarr.open(path, mode="r")
        except Exception as e:
            raise StorageResolutionError(f"Failed to open Zarr path: {path}") from e

        if isinstance(zarr_obj, zarr.Group) and tuple(zarr_obj.keys()):
            raise IsADirectoryError(f"Zarr group is not empty: {path}")
        shutil.rmtree(path)

    def delete_all(self, path: str | Path) -> None:
        """
        Recursively delete a Zarr array or group (file or directory).

        This is the only permitted recursive deletion method for the Zarr backend.

        Args:
            path: the path shared through all backnds

        Raises:
            FileNotFoundError: If the path does not exist
            StorageResolutionError: If deletion fails
        """
        path = str(path)

        if not os.path.exists(path):
            raise FileNotFoundError(f"Zarr path does not exist: {path}")

        try:
            shutil.rmtree(path)
        except Exception as e:
            raise StorageResolutionError(f"Failed to recursively delete Zarr path: {path}") from e

    def exists(self, path: str | Path) -> bool:
        if str(path).endswith(DISK_PASSTHROUGH_EXTENSIONS):
            from .backend_registry import get_backend_instance

            disk_backend = get_backend_instance(Backend.DISK.value)
            return disk_backend.exists(path)

        path = Path(path)

        # If path has no file extension, treat as directory existence check
        # This handles auto_detect_patterns asking "does this directory exist?"
        if not path.suffix:
            return path.exists()

        # Otherwise, check zarr key existence (for actual files)
        store, key = self._split_store_and_key(path)

        try:
            root_group = zarr.open_group(store=store, mode="r")
            return key in root_group or any(
                k.startswith(key.rstrip("/") + "/") for k in root_group.array_keys()
            )
        except FileNotFoundError:
            return False

    def ensure_directory(self, directory: str | Path) -> Path:
        """
        No-op for zarr backend - zarr stores handle their own structure.

        Zarr doesn't have filesystem directories that need to be "ensured".
        Store creation and group structure is handled by save operations.
        """
        return Path(directory)

    def create_symlink(self, source: str | Path, link_name: str | Path, overwrite: bool = False):
        store, src_key = self._split_store_and_key(source)
        store2, dst_key = self._split_store_and_key(link_name)

        if store.root != store2.root:
            raise ValueError("Symlinks must exist within the same .zarr store")

        group = zarr.open_group(store=store, mode="r+")
        if src_key not in group:
            raise FileNotFoundError(f"Source key '{src_key}' not found in Zarr store")

        if dst_key in group:
            if not overwrite:
                raise FileExistsError(f"Symlink target already exists at: {dst_key}")
            # Remove existing entry if overwrite=True
            del group[dst_key]

        # Create a new group at the symlink path
        link_group = group.require_group(dst_key)
        link_group.attrs["_symlink"] = src_key  # Store as declared string

    def is_symlink(self, path: str | Path) -> bool:
        """
        Check if the given Zarr path represents a logical symlink (based on attribute contract).

        Returns:
            bool: True if the key exists and has a declared symlink attribute
            False if the key doesn't exist or is not a symlink
        """
        store, key = self._split_store_and_key(path)
        try:
            group = zarr.open_group(store=store, mode="r")
            obj = group[key]
            return self._symlink_target(obj, path) is not None
        except (KeyError, FileNotFoundError):
            # Key doesn't exist, so it's not a symlink
            return False
        except Exception as e:
            raise StorageResolutionError(f"Failed to inspect Zarr symlink at: {path}") from e

    def _symlink_target(self, obj: zarr.Array | zarr.Group, path: str | Path) -> str | None:
        if "_symlink" not in obj.attrs:
            return None
        target = obj.attrs["_symlink"]
        if not isinstance(target, str):
            raise StorageResolutionError(f"Invalid symlink format in Zarr attrs at: {path}")
        return target

    def _resolve_symlink(self, group: zarr.Group, key: str) -> tuple[str, zarr.Array | zarr.Group]:
        seen_keys = set()
        while True:
            if key not in group:
                raise FileNotFoundError(f"Zarr key does not exist: {key}")
            obj = group[key]
            target = self._symlink_target(obj, key)
            if target is None:
                return key, obj
            if key in seen_keys:
                raise StorageResolutionError(f"Symlink cycle detected in Zarr at: {key}")
            seen_keys.add(key)
            key = target

    def _resolved_node(self, path: str | Path) -> zarr.Array | zarr.Group:
        """Resolve an existing local node without creating storage during reads."""
        store, key = self._split_store_and_key(path)
        group = zarr.open_group(store=store, mode="r")
        return self._resolve_symlink(group, key)[1]

    def _auto_chunks(self, data: Any, chunk_divisor: int = 1) -> tuple[int, ...]:
        shape = data.shape

        # Simple logic: 1/10th of each dim, with min 1
        return tuple(max(1, s // chunk_divisor) for s in shape)

    def is_file(self, path: str | Path) -> bool:
        """
        Check if a Zarr path points to a file (Zarr array), resolving both OS and Zarr-native symlinks.

        Args:
            path: Zarr store path (may point to key within store)

        Returns:
            bool: True if resolved path is a Zarr array

        Raises:
            FileNotFoundError: If path does not exist or broken symlink
            IsADirectoryError: If resolved object is a Zarr group
            StorageResolutionError: For other failures
        """
        if not isinstance(self._resolved_node(path), zarr.Array):
            raise IsADirectoryError(f"Zarr path is a group (directory): {path}")
        return True

    def is_dir(self, path: str | Path) -> bool:
        """
        Check if a Zarr path resolves to a directory (i.e., a Zarr group).

        Resolves both OS-level symlinks and Zarr-native symlinks via .attrs['_symlink'].

        Args:
            path: Zarr path or URI

        Returns:
            bool: True if path resolves to a Zarr group

        Raises:
            FileNotFoundError: If path or resolved target does not exist
            NotADirectoryError: If resolved target is not a group
            StorageResolutionError: For symlink cycles or other failures
        """
        if not isinstance(self._resolved_node(path), zarr.Group):
            raise NotADirectoryError(f"Zarr path is an array (file): {path}")
        return True

    def _transfer_paths(self, src: str | Path, dst: str | Path) -> tuple[Path, Path]:
        """Resolve local node transfer paths while preserving the source format."""
        obj = self._resolved_node(src)
        destination = Path(dst)
        if destination.exists():
            raise FileExistsError(f"Zarr destination already exists: {destination}")
        source = obj.store.root / obj.path
        if source == destination or source in destination.parents:
            raise ValueError("A Zarr node cannot be transferred into itself")
        zarr.open_group(destination.parent, mode="a", zarr_format=obj.metadata.zarr_format)
        return source, destination

    def move(self, src: str | Path, dst: str | Path) -> None:
        """
        Move a Zarr key or object (array/group) from one location to another, resolving symlinks.

        Supports:
        - Disk or memory stores
        - Zarr-native symlinks
        - Key renames within group
        - Full copy+delete across stores if needed

        Raises:
            FileNotFoundError: If src does not exist
            FileExistsError: If dst already exists
            StorageResolutionError: On failure
        """
        source, destination = self._transfer_paths(src, dst)
        shutil.move(source, destination)

    def copy(self, src: str | Path, dst: str | Path) -> None:
        """
        Copy a Zarr key or object (array/group) from one location to another.

        - Resolves Zarr-native symlinks before copying
        - Prevents overwrite unless explicitly allowed (future feature)
        - Works across memory or disk stores

        Raises:
            FileNotFoundError: If src does not exist
            FileExistsError: If dst already exists
            StorageResolutionError: On failure
        """
        source, destination = self._transfer_paths(src, dst)
        shutil.copytree(source, destination)

    def stat(self, path: str | Path) -> dict[str, Any]:
        """
        Return structural metadata about a Zarr path.

        Returns:
            dict with keys:
            - 'type': 'file', 'directory', 'symlink', or 'missing'
            - 'key': final resolved key
            - 'target': symlink target if applicable
            - 'store': repr(store)
            - 'exists': bool

        Raises:
            StorageResolutionError: On resolution failure
        """
        store, key = self._split_store_and_key(path)
        group = zarr.open_group(store=store, mode="r")

        try:
            if key in group:
                obj = group[key]
                target = self._symlink_target(obj, key)
                if target is not None:
                    return {
                        "type": "symlink",
                        "key": key,
                        "target": target,
                        "store": repr(store),
                        "exists": target in group,
                    }

                if isinstance(obj, zarr.Array):
                    return {"type": "file", "key": key, "store": repr(store), "exists": True}

                elif isinstance(obj, zarr.Group):
                    return {"type": "directory", "key": key, "store": repr(store), "exists": True}

                raise StorageResolutionError(f"Unknown object type at: {key}")
            else:
                return {"type": "missing", "key": key, "store": repr(store), "exists": False}

        except Exception as e:
            raise StorageResolutionError(f"Failed to stat Zarr key {key}") from e


class ZarrSymlink:
    """
    Represents a symbolic link in a Zarr store.

    This class is used to represent symbolic links in a Zarr store.
    It stores the target path of the symlink.
    """

    def __init__(self, target: str):
        self.target = target

    def __repr__(self):
        return f"<ZarrSymlink → {self.target}>"

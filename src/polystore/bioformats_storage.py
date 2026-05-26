"""Structured-reference backend for Bio-Formats-backed virtual workspaces."""

from __future__ import annotations

import json
from dataclasses import dataclass
from fnmatch import fnmatch
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Union

from .base import PicklableBackend, ReadOnlyBackend
from .constants import Backend
from .exceptions import StorageResolutionError
from .metadata_writer import get_metadata_path


@dataclass(frozen=True, slots=True)
class BioFormatsPlaneRef:
    """Serializable reference to one Bio-Formats image plane."""

    source_path: Path
    series_index: int
    plane_index: int
    c: int
    z: int
    t: int
    reader: str = "bioformats"

    @classmethod
    def from_mapping(
        cls,
        payload: Dict[str, Any],
        *,
        plate_root: Path,
    ) -> "BioFormatsPlaneRef":
        source_path = Path(payload["source_path"])
        if not source_path.is_absolute():
            source_path = plate_root / source_path
        return cls(
            source_path=source_path,
            series_index=int(payload.get("series_index", 0)),
            plane_index=int(payload["plane_index"]),
            c=int(payload["c"]),
            z=int(payload["z"]),
            t=int(payload["t"]),
            reader=str(payload.get("reader", "bioformats")),
        )


class BioFormatsStorageBackend(ReadOnlyBackend, PicklableBackend):
    """Load normalized virtual source keys from structured Bio-Formats refs."""

    _backend_type = Backend.BIOFORMATS.value

    def __init__(self, plate_root: Path | None = None):
        self.plate_root = None if plate_root is None else Path(plate_root)
        self._mapping_cache: Optional[Dict[str, Dict[str, Any]]] = None
        self._cache_mtime: Optional[float] = None

    def get_connection_params(self) -> Optional[Dict[str, Any]]:
        if self.plate_root is None:
            return None
        return {"plate_root": str(self.plate_root)}

    def set_connection_params(self, params: Optional[Dict[str, Any]]) -> None:
        if not params:
            self.plate_root = None
            self._mapping_cache = None
            self._cache_mtime = None
            return
        self.plate_root = Path(params["plate_root"])
        self._mapping_cache = None
        self._cache_mtime = None

    def load(self, file_path: Union[str, Path], **kwargs) -> Any:
        ref = self._resolve_ref(file_path)
        if ref.reader == "npy":
            return _load_npy_plane(ref)
        if ref.reader != "bioformats":
            raise BioFormatsReaderUnavailableError(
                f"Unsupported Bio-Formats reader {ref.reader!r}."
            )
        from .bioformats_java import load_bioformats_plane

        return load_bioformats_plane(
            source_path=ref.source_path,
            series_index=ref.series_index,
            plane_index=ref.plane_index,
        )

    def load_batch(self, file_paths: List[Union[str, Path]], **kwargs) -> List[Any]:
        return [self.load(file_path, **kwargs) for file_path in file_paths]

    def list_files(
        self,
        directory: Union[str, Path],
        pattern: Optional[str] = None,
        extensions: Optional[Set[str]] = None,
        recursive: bool = False,
        **kwargs,
    ) -> List[str]:
        plate_root = self._require_plate_root()
        relative_dir = self.relative_to_root(directory)
        normalized_dir = _normalize_relative_path(str(relative_dir))
        lowercase_extensions = (
            None if extensions is None else {extension.lower() for extension in extensions}
        )
        results = []
        for virtual_path in self._load_mapping().keys():
            if not _virtual_path_in_directory(
                virtual_path,
                normalized_dir=normalized_dir,
                recursive=recursive,
            ):
                continue
            path = Path(virtual_path)
            if lowercase_extensions is not None and path.suffix.lower() not in lowercase_extensions:
                continue
            if pattern is not None and not fnmatch(path.name, pattern):
                continue
            results.append(str(plate_root / virtual_path))
        return results

    def exists(self, path: Union[str, Path]) -> bool:
        try:
            relative = self.normalized_relative_path(path)
        except StorageResolutionError:
            return False
        if not relative:
            return True
        mapping = self._load_mapping()
        return relative in mapping or any(
            virtual_path.startswith(relative + "/")
            for virtual_path in mapping
        )

    def is_file(self, path: Union[str, Path]) -> bool:
        try:
            relative = self.normalized_relative_path(path)
        except StorageResolutionError:
            return False
        return relative in self._load_mapping()

    def is_dir(self, path: Union[str, Path]) -> bool:
        try:
            relative = self.normalized_relative_path(path)
        except StorageResolutionError:
            return False
        return not relative or any(
            virtual_path.startswith(relative + "/")
            for virtual_path in self._load_mapping()
        )

    def list_dir(self, path: Union[str, Path]) -> List[str]:
        relative = self.normalized_relative_path(path)
        prefix = "" if not relative else relative + "/"
        names = set()
        for virtual_path in self._load_mapping():
            if not virtual_path.startswith(prefix):
                continue
            remainder = virtual_path[len(prefix):]
            if remainder:
                names.add(remainder.split("/", 1)[0])
        return sorted(names)

    def _resolve_ref(self, path: Union[str, Path]) -> BioFormatsPlaneRef:
        plate_root = self._require_plate_root()
        relative_path = self.normalized_relative_path(path)
        mapping = self._load_mapping()
        try:
            payload = mapping[relative_path]
        except KeyError as exc:
            raise StorageResolutionError(
                f"Path not in Bio-Formats workspace mapping: {relative_path}"
            ) from exc
        if not isinstance(payload, dict):
            raise StorageResolutionError(
                f"Bio-Formats workspace mapping for {relative_path!r} is not structured."
            )
        return BioFormatsPlaneRef.from_mapping(payload, plate_root=plate_root)

    def _load_mapping(self) -> Dict[str, Dict[str, Any]]:
        plate_root = self._require_plate_root()
        metadata_path = get_metadata_path(plate_root)
        if not metadata_path.exists():
            raise FileNotFoundError(f"Metadata not found: {metadata_path}")
        current_mtime = metadata_path.stat().st_mtime
        if self._mapping_cache is not None and self._cache_mtime == current_mtime:
            return self._mapping_cache
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        combined_mapping: Dict[str, Dict[str, Any]] = {}
        for subdirectory in metadata.get("subdirectories", {}).values():
            if Backend.BIOFORMATS.value not in subdirectory.get("available_backends", {}):
                continue
            workspace_mapping = subdirectory.get("workspace_mapping", {})
            for virtual_path, ref_payload in workspace_mapping.items():
                if isinstance(ref_payload, dict):
                    combined_mapping[_normalize_relative_path(str(virtual_path))] = ref_payload
        if not combined_mapping:
            raise ValueError(f"No Bio-Formats workspace_mapping in {metadata_path}")
        self._mapping_cache = combined_mapping
        self._cache_mtime = current_mtime
        return combined_mapping

    def _require_plate_root(self) -> Path:
        if self.plate_root is None:
            raise StorageResolutionError("BioFormatsStorageBackend requires plate_root.")
        return self.plate_root

    def relative_to_root(self, path: Union[str, Path]) -> Path:
        plate_root = self._require_plate_root()
        path_obj = Path(path)
        if not path_obj.is_absolute():
            return path_obj
        try:
            return path_obj.relative_to(plate_root)
        except ValueError as exc:
            raise StorageResolutionError(
                f"Path {path_obj} is outside Bio-Formats plate root {plate_root}."
            ) from exc

    def normalized_relative_path(self, path: Union[str, Path]) -> str:
        return _normalize_relative_path(str(self.relative_to_root(path)))


class BioFormatsReaderUnavailableError(RuntimeError):
    """Raised when a production Bio-Formats reader has not been configured."""


def _load_npy_plane(ref: BioFormatsPlaneRef) -> Any:
    import numpy as np

    array = np.load(ref.source_path)
    if array.ndim == 2:
        return array
    if array.ndim == 5:
        return array[ref.t - 1, ref.z - 1, ref.c - 1]
    if array.ndim == 3:
        return array[ref.plane_index]
    raise ValueError(
        f"Unsupported npy Bio-Formats fixture shape {array.shape} for {ref.source_path}."
    )


def _normalize_relative_path(path: str) -> str:
    normalized = path.replace("\\", "/")
    return "" if normalized == "." else normalized


def _virtual_path_in_directory(
    virtual_path: str,
    *,
    normalized_dir: str,
    recursive: bool,
) -> bool:
    if recursive:
        return not normalized_dir or virtual_path.startswith(normalized_dir + "/")
    return _normalize_relative_path(str(Path(virtual_path).parent)) == normalized_dir

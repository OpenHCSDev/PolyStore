"""Virtual workspace loading through structured backend-owned source references."""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping
from dataclasses import dataclass, fields
from fnmatch import fnmatch
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Union

import numpy as np

from .base import (
    BackendBase,
    DataSource,
    PicklableBackend,
    ReadOnlyBackend,
)
from .constants import Backend
from .exceptions import StorageResolutionError
from .metadata_writer import get_metadata_path

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class SourcePixelRef:
    """Format-neutral reference to source pixels owned by one backend."""

    backend: str
    backend_address: str
    source_axis_indices: tuple[int, ...] = ()

    def __post_init__(self) -> None:
        backend = str(self.backend).strip().lower()
        backend_address = str(self.backend_address).strip()
        source_axis_indices = tuple(self.source_axis_indices)
        if not backend:
            raise ValueError("SourcePixelRef.backend cannot be empty.")
        if not backend_address:
            raise ValueError("SourcePixelRef.backend_address cannot be empty.")
        for index in source_axis_indices:
            if not isinstance(index, int) or isinstance(index, bool) or index < 0:
                raise TypeError(
                    "SourcePixelRef.source_axis_indices must contain "
                    "nonnegative integers."
                )
        object.__setattr__(self, "backend", backend)
        object.__setattr__(self, "backend_address", backend_address)
        object.__setattr__(self, "source_axis_indices", source_axis_indices)

    def to_workspace_mapping(self) -> dict[str, object]:
        """Return the sole structured virtual-workspace mapping shape."""
        return {
            field.name: (
                list(value)
                if isinstance(value := getattr(self, field.name), tuple)
                else value
            )
            for field in fields(self)
        }

    @classmethod
    def from_workspace_mapping(cls, payload: object) -> "SourcePixelRef":
        """Parse one exact structured workspace mapping."""
        if not isinstance(payload, Mapping):
            raise TypeError("SourcePixelRef workspace mapping must be structured.")
        required_fields = {field.name for field in fields(cls)}
        payload_fields = set(payload)
        if payload_fields != required_fields:
            missing = tuple(sorted(required_fields - payload_fields))
            extra = tuple(sorted(payload_fields - required_fields))
            raise ValueError(
                "SourcePixelRef workspace mapping fields are invalid: "
                f"missing={missing!r}, extra={extra!r}."
            )
        backend = payload["backend"]
        backend_address = payload["backend_address"]
        source_axis_indices = payload["source_axis_indices"]
        if not isinstance(backend, str):
            raise TypeError("SourcePixelRef.backend must be str.")
        if not isinstance(backend_address, str):
            raise TypeError("SourcePixelRef.backend_address must be str.")
        if not isinstance(source_axis_indices, list):
            raise TypeError("SourcePixelRef.source_axis_indices must be a JSON array.")
        return cls(
            backend=backend,
            backend_address=backend_address,
            source_axis_indices=tuple(source_axis_indices),
        )


class VirtualWorkspaceBackend(ReadOnlyBackend, PicklableBackend):
    """Read-only virtual paths dispatched through an execution-local registry."""

    _backend_type = Backend.VIRTUAL_WORKSPACE.value

    def __init__(self, plate_root: Path):
        self.plate_root = Path(plate_root)
        self._registry: Mapping[str, BackendBase] | None = None
        self._mapping_cache: Optional[Dict[str, SourcePixelRef]] = None
        self._cache_mtime: Optional[float] = None
        self._load_mapping()

    def bind_registry(self, registry: Mapping[str, BackendBase]) -> None:
        """Bind the FileManager's complete execution-local backend registry."""
        self._registry = registry

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
        self._mapping_cache = None
        self._cache_mtime = None
        self._load_mapping()

    @staticmethod
    def _normalize_relative_path(path_str: str) -> str:
        normalized = path_str.replace("\\", "/")
        return "" if normalized == "." else normalized

    def _load_mapping(self) -> Dict[str, SourcePixelRef]:
        metadata_path = get_metadata_path(self.plate_root)
        if not metadata_path.exists():
            raise FileNotFoundError(f"Metadata not found: {metadata_path}")
        current_mtime = metadata_path.stat().st_mtime
        if self._mapping_cache is not None and self._cache_mtime == current_mtime:
            return self._mapping_cache
        with metadata_path.open("r", encoding="utf-8") as stream:
            metadata = json.load(stream)
        combined_mapping: Dict[str, SourcePixelRef] = {}
        for subdirectory in metadata.get("subdirectories", {}).values():
            for virtual_path, source_ref in subdirectory.get(
                "workspace_mapping",
                {},
            ).items():
                combined_mapping[self._normalize_relative_path(str(virtual_path))] = (
                    SourcePixelRef.from_workspace_mapping(source_ref)
                )
        if not combined_mapping:
            raise ValueError(f"No workspace_mapping in {metadata_path}")
        self._mapping_cache = combined_mapping
        self._cache_mtime = current_mtime
        logger.info("Loaded %s mappings for %s", len(combined_mapping), self.plate_root)
        return combined_mapping

    def _resolve_ref(self, path: Union[str, Path]) -> SourcePixelRef:
        path_obj = Path(path)
        try:
            relative_path = path_obj.relative_to(self.plate_root)
        except ValueError:
            relative_path = path_obj
        relative_str = self._normalize_relative_path(str(relative_path))
        mapping = self._load_mapping()
        try:
            return mapping[relative_str]
        except KeyError as exc:
            raise StorageResolutionError(
                f"Path not in virtual workspace mapping: {relative_str}"
            ) from exc

    def _backend_for_ref(self, ref: SourcePixelRef) -> DataSource:
        if ref.backend == Backend.VIRTUAL_WORKSPACE.value:
            raise StorageResolutionError(
                "Virtual workspace source refs cannot target virtual_workspace."
            )
        if self._registry is None:
            raise StorageResolutionError(
                "VirtualWorkspaceBackend requires an execution-local FileManager registry."
            )
        try:
            backend = self._registry[ref.backend]
        except KeyError as exc:
            raise StorageResolutionError(
                f"Source backend {ref.backend!r} is not registered."
            ) from exc
        if not isinstance(backend, DataSource):
            raise StorageResolutionError(
                f"Source backend {ref.backend!r} is not a DataSource."
            )
        return backend

    def _load_ref(self, ref: SourcePixelRef, **kwargs: Any) -> Any:
        backend = self._backend_for_ref(ref)
        address = backend.resolve_address(
            ref.backend_address,
            base_path=self.plate_root,
        )
        payload = backend.load(address, **kwargs)
        return self._project_source_axes(payload, ref)

    @staticmethod
    def _project_source_axes(payload: Any, ref: SourcePixelRef) -> Any:
        projected = payload
        for index in ref.source_axis_indices:
            shape = np.shape(projected)
            if not shape or index >= shape[0]:
                raise StorageResolutionError(
                    f"Source ref {ref!r} cannot select leading index {index} "
                    f"from payload shape {shape!r}."
                )
            projected = projected[index]
        return projected

    def _resolve_path(self, path: Union[str, Path]) -> str:
        ref = self._resolve_ref(path)
        backend = self._backend_for_ref(ref)
        return str(
            backend.resolve_address(
                ref.backend_address,
                base_path=self.plate_root,
            )
        )

    def resolve_address(
        self,
        backend_address: Union[str, Path],
        *,
        base_path: Path,
    ) -> str:
        """Resolve a virtual path through its declared source backend."""
        del base_path
        return self._resolve_path(backend_address)

    def load(self, file_path: Union[str, Path], **kwargs: Any) -> Any:
        return self._load_ref(self._resolve_ref(file_path), **kwargs)

    def load_batch(
        self,
        file_paths: List[Union[str, Path]],
        **kwargs: Any,
    ) -> List[Any]:
        refs = tuple(self._resolve_ref(path) for path in file_paths)
        grouped_indices: dict[tuple[str, str], list[int]] = {}
        for output_index, ref in enumerate(refs):
            grouped_indices.setdefault(
                (ref.backend, ref.backend_address),
                [],
            ).append(output_index)
        outputs: list[Any] = [None] * len(refs)
        for output_indices in grouped_indices.values():
            first_ref = refs[output_indices[0]]
            backend = self._backend_for_ref(first_ref)
            address = backend.resolve_address(
                first_ref.backend_address,
                base_path=self.plate_root,
            )
            payload = backend.load(address, **kwargs)
            for output_index in output_indices:
                outputs[output_index] = self._project_source_axes(
                    payload,
                    refs[output_index],
                )
        return outputs

    def list_files(
        self,
        directory: Union[str, Path],
        pattern: Optional[str] = None,
        extensions: Optional[Set[str]] = None,
        recursive: bool = False,
        **kwargs: Any,
    ) -> List[str]:
        del kwargs
        directory_path = Path(directory)
        try:
            relative_directory = directory_path.relative_to(self.plate_root)
        except ValueError:
            relative_directory = directory_path
        relative_directory_text = self._normalize_relative_path(
            str(relative_directory)
        )
        lowercase_extensions = (
            None if extensions is None else {extension.lower() for extension in extensions}
        )
        results: list[str] = []
        for virtual_relative in self._load_mapping():
            path = Path(virtual_relative)
            parent = self._normalize_relative_path(str(path.parent))
            if recursive:
                if relative_directory_text and not (
                    virtual_relative == relative_directory_text
                    or virtual_relative.startswith(relative_directory_text + "/")
                ):
                    continue
            elif parent != relative_directory_text:
                continue
            if pattern is not None and not fnmatch(path.name, pattern):
                continue
            if (
                lowercase_extensions is not None
                and path.suffix.lower() not in lowercase_extensions
            ):
                continue
            results.append(str(self.plate_root / virtual_relative))
        return sorted(results)

    def list_dir(self, path: Union[str, Path]) -> List[str]:
        path_obj = Path(path)
        if path_obj.is_absolute():
            try:
                path_obj = path_obj.relative_to(self.plate_root)
            except ValueError as exc:
                raise FileNotFoundError(
                    f"Path not under plate root: {path_obj}"
                ) from exc
        prefix = self._normalize_relative_path(str(path_obj))
        prefix = "" if not prefix else prefix + "/"
        entries = {
            remainder.split("/", 1)[0]
            for virtual_path in self._load_mapping()
            if virtual_path.startswith(prefix)
            and (remainder := virtual_path[len(prefix):])
        }
        return sorted(entries)

    def _relative_mapping_path(self, path: Union[str, Path]) -> str | None:
        path_obj = Path(path)
        if path_obj.is_absolute():
            try:
                path_obj = path_obj.relative_to(self.plate_root)
            except ValueError:
                return None
        return self._normalize_relative_path(str(path_obj))

    def exists(self, path: Union[str, Path]) -> bool:
        relative_path = self._relative_mapping_path(path)
        if relative_path is None:
            return False
        mapping = self._load_mapping()
        return (
            not relative_path
            or relative_path in mapping
            or any(key.startswith(relative_path + "/") for key in mapping)
        )

    def is_file(self, path: Union[str, Path]) -> bool:
        relative_path = self._relative_mapping_path(path)
        return relative_path is not None and relative_path in self._load_mapping()

    def is_dir(self, path: Union[str, Path]) -> bool:
        relative_path = self._relative_mapping_path(path)
        if relative_path is None:
            return False
        return not relative_path or any(
            key.startswith(relative_path + "/") for key in self._load_mapping()
        )

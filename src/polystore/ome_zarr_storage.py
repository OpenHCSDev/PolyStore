"""Direct OME-Zarr array reader with opaque structured addresses."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Union

import zarr

from .base import PicklableBackend, ReadOnlyBackend
from .constants import Backend
from .exceptions import StorageResolutionError


@dataclass(frozen=True, slots=True)
class OmeZarrArrayRef:
    """Backend-owned address of one NGFF array."""

    store_path: Path
    array_path: str

    def __post_init__(self) -> None:
        store_path = Path(self.store_path)
        array_path = str(self.array_path).strip("/")
        if not str(store_path) or not array_path:
            raise ValueError("OME-Zarr store and array paths cannot be empty.")
        object.__setattr__(self, "store_path", store_path)
        object.__setattr__(self, "array_path", array_path)

    def to_backend_address(self) -> str:
        return json.dumps(
            {
                "array_path": self.array_path,
                "store_path": str(self.store_path),
            },
            sort_keys=True,
            separators=(",", ":"),
        )

    @classmethod
    def from_backend_address(cls, backend_address: str) -> "OmeZarrArrayRef":
        if not isinstance(backend_address, str):
            raise TypeError("OME-Zarr backend address must be str.")
        try:
            payload = json.loads(backend_address)
        except json.JSONDecodeError as exc:
            raise ValueError("Invalid OME-Zarr backend address JSON.") from exc
        if not isinstance(payload, dict):
            raise TypeError("OME-Zarr backend address must encode an object.")
        if set(payload) != {"array_path", "store_path"}:
            raise ValueError(
                "OME-Zarr backend address fields must be array_path and store_path."
            )
        if not isinstance(payload["array_path"], str) or not isinstance(
            payload["store_path"],
            str,
        ):
            raise TypeError("OME-Zarr backend address values must be strings.")
        return cls(
            store_path=Path(payload["store_path"]),
            array_path=payload["array_path"],
        )


class OmeZarrStorageBackend(ReadOnlyBackend, PicklableBackend):
    """Load arrays addressed directly by ``OmeZarrArrayRef`` values."""

    _backend_type = Backend.OME_ZARR.value

    def get_connection_params(self) -> Optional[Dict[str, Any]]:
        return None

    def source_path(
        self,
        backend_address: Union[str, Path],
        *,
        base_path: Path,
    ) -> Path:
        """Return the physical NGFF store owned by an opaque array address."""

        del base_path
        return OmeZarrArrayRef.from_backend_address(
            str(backend_address)
        ).store_path

    def set_connection_params(self, params: Optional[Dict[str, Any]]) -> None:
        if params is not None:
            raise ValueError("OmeZarrStorageBackend has no connection parameters.")

    def load(self, file_path: Union[str, Path], **kwargs: Any) -> Any:
        del kwargs
        ref = OmeZarrArrayRef.from_backend_address(str(file_path))
        if not ref.store_path.is_dir():
            raise FileNotFoundError(f"OME-Zarr store is absent: {ref.store_path}")
        try:
            root = zarr.open_group(str(ref.store_path), mode="r")
        except zarr.errors.GroupNotFoundError as exc:
            raise FileNotFoundError(
                f"OME-Zarr group is absent: {ref.store_path}"
            ) from exc
        if ref.array_path not in root:
            raise FileNotFoundError(
                f"OME-Zarr array {ref.array_path!r} is absent from {ref.store_path}."
            )
        return root[ref.array_path][:]

    def load_batch(
        self,
        file_paths: List[Union[str, Path]],
        **kwargs: Any,
    ) -> List[Any]:
        return [self.load(file_path, **kwargs) for file_path in file_paths]

    def list_files(
        self,
        directory: Union[str, Path],
        pattern: Optional[str] = None,
        extensions: Optional[Set[str]] = None,
        recursive: bool = False,
        **kwargs: Any,
    ) -> List[str]:
        del directory, pattern, extensions, recursive, kwargs
        raise StorageResolutionError(
            "OmeZarrStorageBackend is a direct array reader, not a workspace."
        )

    def exists(self, path: Union[str, Path]) -> bool:
        try:
            ref = OmeZarrArrayRef.from_backend_address(str(path))
            return ref.store_path.is_dir() and ref.array_path in zarr.open_group(
                str(ref.store_path),
                mode="r",
            )
        except (
            TypeError,
            ValueError,
            FileNotFoundError,
            zarr.errors.GroupNotFoundError,
        ):
            return False

    def is_file(self, path: Union[str, Path]) -> bool:
        return self.exists(path)

    def is_dir(self, path: Union[str, Path]) -> bool:
        del path
        return False

    def list_dir(self, path: Union[str, Path]) -> List[str]:
        del path
        raise StorageResolutionError(
            "OmeZarrStorageBackend is a direct array reader, not a directory."
        )

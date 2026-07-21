"""Direct Bio-Formats storage backend with opaque plane addresses."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Union

from .base import (
    ImageSamplingRequest,
    ImageSamplingResult,
    PicklableBackend,
    ReadOnlyBackend,
)
from .constants import Backend
from .exceptions import StorageResolutionError


@dataclass(frozen=True, slots=True)
class BioFormatsPlaneRef:
    """Backend-owned address of one Bio-Formats image plane."""

    source_path: Path
    series_index: int
    plane_index: int

    def __post_init__(self) -> None:
        source_path = Path(self.source_path)
        if not str(source_path):
            raise ValueError("BioFormatsPlaneRef.source_path cannot be empty.")
        for field_name, value in (
            ("series_index", self.series_index),
            ("plane_index", self.plane_index),
        ):
            if not isinstance(value, int) or isinstance(value, bool) or value < 0:
                raise TypeError(
                    f"BioFormatsPlaneRef.{field_name} must be a nonnegative integer."
                )
        object.__setattr__(self, "source_path", source_path)

    def to_backend_address(self) -> str:
        """Serialize this backend-owned reference as canonical compact JSON."""
        return json.dumps(
            {
                "plane_index": self.plane_index,
                "series_index": self.series_index,
                "source_path": str(self.source_path),
            },
            sort_keys=True,
            separators=(",", ":"),
        )

    @classmethod
    def from_backend_address(cls, backend_address: str) -> "BioFormatsPlaneRef":
        """Parse one exact canonical Bio-Formats backend address."""
        if not isinstance(backend_address, str):
            raise TypeError("Bio-Formats backend address must be str.")
        try:
            payload = json.loads(backend_address)
        except json.JSONDecodeError as exc:
            raise ValueError("Invalid Bio-Formats backend address JSON.") from exc
        if not isinstance(payload, dict):
            raise TypeError("Bio-Formats backend address must encode an object.")
        required_fields = {"source_path", "series_index", "plane_index"}
        if set(payload) != required_fields:
            raise ValueError(
                "Bio-Formats backend address fields must be exactly "
                f"{tuple(sorted(required_fields))!r}."
            )
        if not isinstance(payload["source_path"], str):
            raise TypeError("Bio-Formats source_path must be str.")
        return cls(
            source_path=Path(payload["source_path"]),
            series_index=payload["series_index"],
            plane_index=payload["plane_index"],
        )


class BioFormatsStorageBackend(ReadOnlyBackend, PicklableBackend):
    """Load planes addressed directly by ``BioFormatsPlaneRef`` values."""

    _backend_type = Backend.BIOFORMATS.value

    def get_connection_params(self) -> Optional[Dict[str, Any]]:
        return None

    def source_path(
        self,
        backend_address: Union[str, Path],
        *,
        base_path: Path,
    ) -> Path:
        """Return the container path owned by one exact plane address."""

        del base_path
        return BioFormatsPlaneRef.from_backend_address(
            str(backend_address)
        ).source_path

    def set_connection_params(self, params: Optional[Dict[str, Any]]) -> None:
        if params is not None:
            raise ValueError("BioFormatsStorageBackend has no connection parameters.")

    def load(self, file_path: Union[str, Path], **kwargs: Any) -> Any:
        del kwargs
        ref = BioFormatsPlaneRef.from_backend_address(str(file_path))
        from .bioformats_java import load_bioformats_plane

        return load_bioformats_plane(
            source_path=ref.source_path,
            series_index=ref.series_index,
            plane_index=ref.plane_index,
        )

    def sample(
        self,
        file_path: Union[str, Path],
        request: ImageSamplingRequest,
    ) -> ImageSamplingResult:
        ref = BioFormatsPlaneRef.from_backend_address(str(file_path))
        from .bioformats_java import sample_bioformats_plane

        return sample_bioformats_plane(
            source_path=ref.source_path,
            series_index=ref.series_index,
            plane_index=ref.plane_index,
            request=request,
        )

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
            "BioFormatsStorageBackend is a direct address reader, not a workspace."
        )

    def exists(self, path: Union[str, Path]) -> bool:
        try:
            return BioFormatsPlaneRef.from_backend_address(str(path)).source_path.exists()
        except (TypeError, ValueError):
            return False

    def is_file(self, path: Union[str, Path]) -> bool:
        try:
            return BioFormatsPlaneRef.from_backend_address(str(path)).source_path.is_file()
        except (TypeError, ValueError):
            return False

    def is_dir(self, path: Union[str, Path]) -> bool:
        del path
        return False

    def list_dir(self, path: Union[str, Path]) -> List[str]:
        del path
        raise StorageResolutionError(
            "BioFormatsStorageBackend is a direct address reader, not a directory."
        )

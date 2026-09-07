"""OME-Zarr discovery and legacy metadata projection through upstream locations."""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from functools import cached_property
from pathlib import Path

import zarr
from ome_zarr.format import Format, detect_format, format_from_version
from ome_zarr.io import ZarrLocation
from ome_zarr.types import JSONDict

from .exceptions import StorageResolutionError


class OmeZarrLocation(ZarrLocation):
    """Read an NGFF location, preserving historical top-level declarations.

    Earlier PolyStore output contains both top-level NGFF declarations and an
    ``ome`` namespace, sometimes with conflicting well metadata. Top-level
    declarations retain their historical precedence. Standard namespaced stores
    use the upstream metadata projection unchanged.
    """

    @cached_property
    def group(self) -> zarr.Group:
        return zarr.open_group(store=self.store, mode="r")

    @property
    def root_attrs(self) -> JSONDict:
        metadata = super().root_attrs
        metadata.update((name, value) for name, value in self.group.attrs.items() if name != "ome")
        return metadata

    @property
    def fmt(self) -> Format:
        metadata = self.root_attrs
        default = format_from_version(metadata["version"]) if "version" in metadata else super().fmt
        detected = detect_format(metadata, default)
        if "version" not in metadata and not detected.matches(metadata):
            raise StorageResolutionError(
                f"NGFF metadata at {self.path} must declare a supported version."
            )
        return detected

    @property
    def version(self) -> str:
        return self.fmt.version

    @property
    def is_dataset(self) -> bool:
        metadata = self.root_attrs
        return "plate" in metadata or "multiscales" in metadata

    def array(self, path: str, *, axes: Sequence[str]) -> zarr.Array:
        """Resolve an image array and check its declared dimension names."""
        if not isinstance(path, str) or not path:
            raise StorageResolutionError("NGFF dataset path must be nonempty text.")
        array = self.group[path]
        if not isinstance(array, zarr.Array):
            raise StorageResolutionError(f"NGFF dataset {path!r} must be an array.")
        metadata = array.metadata.to_dict()
        if "dimension_names" in metadata and tuple(metadata["dimension_names"]) != tuple(axes):
            raise StorageResolutionError(
                f"NGFF axes {tuple(axes)!r} disagree with array dimension_names "
                f"{metadata['dimension_names']!r} at {self.subpath(path)}."
            )
        return array

    @property
    def base_array(self) -> zarr.Array:
        """Resolve the first declared image resolution, independent of its name."""
        try:
            multiscale = self.root_attrs["multiscales"][0]
            path = multiscale["datasets"][0]["path"]
            axes = tuple(axis["name"] for axis in multiscale["axes"])
        except (KeyError, IndexError, TypeError) as exc:
            raise StorageResolutionError(
                f"NGFF image at {self.path} must declare multiscales with "
                "dataset paths and named axes."
            ) from exc
        return self.array(path, axes=axes)

    @classmethod
    def discover(cls, root: Path) -> Iterator[OmeZarrLocation]:
        """Find outermost NGFF datasets without inspecting chunk directories.

        Filesystem traversal stops at a Zarr group. Its group hierarchy, rather
        than any format-specific metadata filename, owns further traversal.
        """
        if not root.is_dir():
            return
        location = cls(root, mode="r")
        if location.exists() and location.is_dataset:
            yield location
            return
        children = (
            (Path(location.subpath(name)) for name, _group in location.group.groups())
            if location.exists()
            else root.iterdir()
        )
        for child in sorted(children):
            if child.is_dir() and not child.is_symlink():
                yield from cls.discover(child)

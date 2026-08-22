"""Tests for Bio-Formats backend address ownership."""

from pathlib import Path

from polystore.bioformats_storage import BioFormatsPlaneRef, BioFormatsStorageBackend


def test_relative_plane_address_resolves_against_source_collection(
    tmp_path: Path,
) -> None:
    backend = BioFormatsStorageBackend()
    address = BioFormatsPlaneRef(Path("plate.HTD"), 2, 7).to_backend_address()

    resolved = backend.resolve_address(address, base_path=tmp_path)

    assert BioFormatsPlaneRef.from_backend_address(resolved) == BioFormatsPlaneRef(
        tmp_path / "plate.HTD",
        2,
        7,
    )
    assert backend.source_path(address, base_path=tmp_path) == tmp_path / "plate.HTD"


def test_absolute_plane_address_is_already_resolved(tmp_path: Path) -> None:
    backend = BioFormatsStorageBackend()
    ref = BioFormatsPlaneRef(tmp_path / "plate.czi", 3, 11)

    assert (
        backend.resolve_address(
            ref.to_backend_address(),
            base_path=tmp_path / "other",
        )
        == ref.to_backend_address()
    )

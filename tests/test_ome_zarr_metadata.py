"""Physical-store discovery and historical NGFF metadata read contracts."""

from pathlib import Path

import numpy as np
import pytest
import zarr
from ome_zarr.format import Format, FormatV04, FormatV05

from polystore.exceptions import StorageResolutionError
from polystore.ome_zarr_metadata import OmeZarrLocation


@pytest.fixture(params=(FormatV04(), FormatV05()), ids=lambda fmt: fmt.version)
def image_store(request, tmp_path: Path) -> tuple[Path, Format]:
    fmt = request.param
    path = tmp_path / "collection" / "image"
    group = zarr.open_group(path, mode="w", zarr_format=fmt.zarr_format)
    multiscale = {
        "name": "example",
        "axes": [{"name": "y", "type": "space"}, {"name": "x", "type": "space"}],
        "datasets": [{"path": "0"}],
    }
    array_options = {}
    if fmt.zarr_format == 3:
        group.attrs["ome"] = {"version": fmt.version, "multiscales": [multiscale]}
        array_options["dimension_names"] = ("y", "x")
    else:
        multiscale["version"] = fmt.version
        group.attrs["multiscales"] = [multiscale]
    pixels = np.arange(12).reshape(3, 4)
    array = group.create_array(
        "0", shape=pixels.shape, dtype=pixels.dtype, chunks=(1, 2), **array_options
    )
    array[:] = pixels
    return path, fmt


def test_location_projects_metadata_group_and_upstream_format(image_store) -> None:
    path, fmt = image_store
    location = OmeZarrLocation(path)

    assert location.exists()
    assert location.is_dataset
    assert location.fmt == fmt
    assert location.version == fmt.version
    assert location.root_attrs["multiscales"][0]["name"] == "example"
    assert "ome" not in location.root_attrs
    assert location.group.metadata.zarr_format == fmt.zarr_format
    assert location.group["0"].chunks == (1, 2)
    assert location.array("0", axes=("y", "x")).shape == (3, 4)
    np.testing.assert_array_equal(location.base_array[:], np.arange(12).reshape(3, 4))
    np.testing.assert_array_equal(location.group["0"][:], np.arange(12).reshape(3, 4))


def test_discovery_finds_nested_store_without_descending_into_arrays(
    image_store,
    monkeypatch,
) -> None:
    path, _fmt = image_store
    original_iterdir = Path.iterdir

    def checked_iterdir(directory):
        assert directory != path and path not in directory.parents
        return original_iterdir(directory)

    monkeypatch.setattr(Path, "iterdir", checked_iterdir)

    assert [Path(location.path) for location in OmeZarrLocation.discover(path.parent)] == [path]
    assert [Path(location.path) for location in OmeZarrLocation.discover(path)] == [path]


def test_discovery_uses_zarr_group_hierarchy(image_store) -> None:
    path, fmt = image_store
    parent = zarr.open_group(path.parent, mode="a", zarr_format=fmt.zarr_format)
    parent.require_group("unrelated")

    assert [Path(location.path) for location in OmeZarrLocation.discover(path.parent)] == [path]


def test_plate_discovery_does_not_duplicate_its_image(image_store) -> None:
    path, fmt = image_store
    parent = zarr.open_group(path.parent, mode="a", zarr_format=fmt.zarr_format)
    plate = {"name": "plate"}
    if fmt.zarr_format == 3:
        parent.attrs["ome"] = {"version": fmt.version, "plate": plate}
    else:
        plate["version"] = fmt.version
        parent.attrs["plate"] = plate

    assert [Path(location.path) for location in OmeZarrLocation.discover(path.parent)] == [
        path.parent
    ]


def test_legacy_namespace_version_does_not_hide_top_level_plate(tmp_path: Path) -> None:
    group = zarr.open_group(tmp_path, mode="w", zarr_format=2)
    plate = {"name": "legacy", "version": "0.4"}
    group.attrs["ome"] = {"version": "0.4"}
    group.attrs["plate"] = plate

    location = OmeZarrLocation(tmp_path)

    assert location.is_dataset
    assert location.root_attrs["plate"] == plate
    assert location.fmt == FormatV04()


def test_legacy_top_level_well_overrides_conflicting_namespace(tmp_path: Path) -> None:
    group = zarr.open_group(tmp_path, mode="w", zarr_format=2)
    nested = {"version": "0.5", "images": [{"path": "0", "acquisition": 0}]}
    declared = {"version": "0.4", "images": [{"path": "0"}]}
    group.attrs["ome"] = {"version": "0.5", "well": nested}
    group.attrs["well"] = declared

    location = OmeZarrLocation(tmp_path)

    assert location.root_attrs["well"] == declared
    assert location.fmt == FormatV04()
    assert location.version == "0.4"
    assert group.attrs["ome"]["well"] == nested


def test_absent_and_ordinary_paths_are_not_datasets(tmp_path: Path) -> None:
    assert list(OmeZarrLocation.discover(tmp_path / "absent")) == []
    assert list(OmeZarrLocation.discover(tmp_path)) == []


def test_discovery_does_not_follow_directory_symlink_cycles(tmp_path: Path) -> None:
    try:
        (tmp_path / "cycle").symlink_to(tmp_path, target_is_directory=True)
    except OSError as exc:
        pytest.skip(f"Directory symlinks unavailable: {exc}")

    assert list(OmeZarrLocation.discover(tmp_path)) == []


def test_array_dimension_names_must_match_ngff_axes(tmp_path: Path) -> None:
    group = zarr.open_group(tmp_path, mode="w", zarr_format=3)
    group.create_array("0", shape=(3, 4), dtype="uint16", dimension_names=("x", "y"))

    with pytest.raises(StorageResolutionError, match="disagree with array dimension_names"):
        OmeZarrLocation(tmp_path).array("0", axes=("y", "x"))


def test_ngff_dataset_path_must_reference_an_array(tmp_path: Path) -> None:
    group = zarr.open_group(tmp_path, mode="w", zarr_format=3)
    group.require_group("not-an-array")

    with pytest.raises(StorageResolutionError, match="must be an array"):
        OmeZarrLocation(tmp_path).array("not-an-array", axes=("y", "x"))


def test_base_array_follows_the_declared_resolution_path(tmp_path: Path) -> None:
    group = zarr.open_group(tmp_path, mode="w", zarr_format=3)
    group.create_array("unrelated", shape=(1, 1), dtype="uint16")
    pixels = np.arange(6, dtype=np.uint16).reshape(2, 3)
    array = group.create_array(
        "resolution/full",
        shape=pixels.shape,
        dtype=pixels.dtype,
        dimension_names=("y", "x"),
    )
    array[:] = pixels
    group.attrs["ome"] = {
        "version": "0.5",
        "multiscales": [
            {
                "axes": [
                    {"name": "y", "type": "space"},
                    {"name": "x", "type": "space"},
                ],
                "datasets": [{"path": "resolution/full"}],
            }
        ],
    }

    np.testing.assert_array_equal(OmeZarrLocation(tmp_path).base_array[:], pixels)


@pytest.mark.parametrize(
    "multiscales",
    [None, [], [{}], [{"datasets": []}], [{"datasets": [{"path": 1}], "axes": []}]],
)
def test_base_array_rejects_malformed_image_metadata(tmp_path: Path, multiscales) -> None:
    group = zarr.open_group(tmp_path, mode="w", zarr_format=3)
    group.attrs["ome"] = {"version": "0.5", "multiscales": multiscales}

    with pytest.raises(StorageResolutionError, match="NGFF"):
        _ = OmeZarrLocation(tmp_path).base_array


@pytest.mark.parametrize("declaration", [{"version": "99.0"}, {}])
def test_unknown_or_missing_legacy_version_is_not_assumed_current(
    tmp_path: Path,
    declaration,
) -> None:
    group = zarr.open_group(tmp_path, mode="w", zarr_format=2)
    group.attrs["multiscales"] = [declaration]

    with pytest.raises(StorageResolutionError, match="supported version"):
        _ = OmeZarrLocation(tmp_path).fmt

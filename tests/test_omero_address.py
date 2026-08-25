"""Tests for declaration-owned OMERO virtual plane addresses."""

import pytest

from polystore import (
    OMEROPlaneAddress,
    OMEROPlaneAxis,
    OMEROPlaneCoordinates,
    OMEROWellAddress,
)


@pytest.mark.parametrize(
    ("row_index", "column_index", "label"),
    (
        (0, 0, "A01"),
        (25, 98, "Z99"),
        (26, 0, "AA01"),
        (31, 47, "AF48"),
    ),
)
def test_omero_well_labels_round_trip_across_multi_letter_rows(
    row_index: int,
    column_index: int,
    label: str,
) -> None:
    well = OMEROWellAddress(row_index, column_index)

    assert well.label == label
    assert OMEROWellAddress.from_label(label.lower()) == well


@pytest.mark.parametrize("label", ("", "A00", "1A", "A-1"))
def test_omero_well_labels_reject_invalid_coordinates(label: str) -> None:
    with pytest.raises(ValueError, match="OMERO well"):
        OMEROWellAddress.from_label(label)


def test_omero_plane_filenames_round_trip_canonical_components() -> None:
    address = OMEROPlaneAddress(
        well=OMEROWellAddress.from_label("AA01"),
        coordinates=OMEROPlaneCoordinates(
            {
                OMEROPlaneAxis.SITE: "9",
                OMEROPlaneAxis.CHANNEL: 2,
                OMEROPlaneAxis.Z_INDEX: "3",
                OMEROPlaneAxis.TIMEPOINT: 1,
            }
        ),
        extension="ome.tif",
    )

    assert address.filename() == "AA01_s009_w2_z003_t001.ome.tif"
    assert OMEROPlaneAddress.from_filename(address.filename()) == address
    assert address.coordinates[OMEROPlaneAxis.SITE] == 9
    assert address.coordinates[OMEROPlaneAxis.Z_INDEX] == 3


def test_omero_plane_coordinates_require_nominal_axis_keys() -> None:
    with pytest.raises(TypeError, match="OMEROPlaneAxis keys"):
        OMEROPlaneCoordinates(  # type: ignore[arg-type]
            {
                "site": 1,
                "channel": 2,
                "z_index": 3,
                "timepoint": 4,
            }
        )

    with pytest.raises(ValueError, match="missing timepoint"):
        OMEROPlaneCoordinates(
            {
                OMEROPlaneAxis.SITE: 1,
                OMEROPlaneAxis.CHANNEL: 2,
                OMEROPlaneAxis.Z_INDEX: 3,
            }
        )


def test_omero_plane_parser_accepts_result_suffix_without_mirroring_grammar() -> None:
    address = OMEROPlaneAddress.from_filename(
        r"C:\virtual\A01_s001_w2_z003_t004_segmentation_step7.tif"
    )

    assert address == OMEROPlaneAddress(
        well=OMEROWellAddress.from_label("A01"),
        coordinates=OMEROPlaneCoordinates(
            {
                OMEROPlaneAxis.SITE: 1,
                OMEROPlaneAxis.CHANNEL: 2,
                OMEROPlaneAxis.Z_INDEX: 3,
                OMEROPlaneAxis.TIMEPOINT: 4,
            }
        ),
        extension=".tif",
    )


def test_omero_image_name_preserves_sparse_site_identity() -> None:
    image_name = OMEROPlaneAddress.image_name(well="AF48", site=9)

    assert image_name == "AF48_s009"
    assert (
        OMEROPlaneAddress.site_for_well_sample(
            well="AF48",
            image_name=image_name,
            ordinal=1,
        )
        == 9
    )


def test_native_omero_image_name_uses_well_sample_order() -> None:
    assert (
        OMEROPlaneAddress.site_for_well_sample(
            well="A01",
            image_name="native acquisition name",
            ordinal=3,
        )
        == 3
    )


@pytest.mark.parametrize(
    "filename",
    ("A01_s001_w2_z003.tif", "not-an-omero-plane.tif"),
)
def test_omero_plane_parser_rejects_incomplete_addresses(filename: str) -> None:
    assert OMEROPlaneAddress.from_filename(filename) is None


def test_omero_plane_parser_rejects_zero_coordinates() -> None:
    with pytest.raises(ValueError, match="site must be a positive integer"):
        OMEROPlaneAddress.from_filename("A01_s000_w2_z003_t001.tif")

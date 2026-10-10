"""Tests for declaration-owned Zarr batch semantics."""

import pytest

from polystore.zarr_batch import (
    ATTR_DIMENSIONS,
    ATTR_IMAGE_COORDINATE,
    ZarrBatchAxis,
    ZarrBatchAxisRole,
    ZarrBatchLayout,
    ZarrStoredBatchSemantics,
)


def test_layout_rejects_incomplete_dense_coordinates() -> None:
    with pytest.raises(ValueError, match="one item for every coordinate"):
        ZarrBatchLayout(
            axes=(ZarrBatchAxis("c", ("1", "2")),),
            item_coordinates=((0,),),
        )


def test_array_axes_take_their_type_from_the_ngff_axis_name() -> None:
    layout = ZarrBatchLayout(
        axes=(
            ZarrBatchAxis("t", ("1",)),
            ZarrBatchAxis("c", ("1",)),
            ZarrBatchAxis("z", ("1",)),
        ),
        item_coordinates=((0, 0, 0),),
    )
    assert layout.ngff_axes == (
        {"name": "t", "type": "time"},
        {"name": "c", "type": "channel"},
        {"name": "z", "type": "space"},
        {"name": "y", "type": "space"},
        {"name": "x", "type": "space"},
    )
    with pytest.raises(ValueError, match="not an OME-NGFF axis"):
        ZarrBatchAxis("band", ("1",))


def test_layout_rejects_multiple_hcs_image_axes() -> None:
    image_role = ZarrBatchAxisRole.HCS_IMAGE
    with pytest.raises(ValueError, match="at most one HCS image axis"):
        ZarrBatchLayout(
            axes=(
                ZarrBatchAxis("field", ("1",), image_role),
                ZarrBatchAxis("scene", ("1",), image_role),
            ),
            item_coordinates=((0, 0),),
        )


def test_stored_semantics_projects_labels_without_owning_their_meaning() -> None:
    semantics = ZarrStoredBatchSemantics.from_attrs(
        {
            ATTR_DIMENSIONS: {
                "t": {"size": 2, "values": ["late", "early"], "role": "array"}
            },
            ATTR_IMAGE_COORDINATE: {"field": "site-7"},
        }
    )

    assert semantics.array_axis_value("t", 1, "fallback") == "early"
    assert semantics.array_axis_value("z", 0, "fallback") == "fallback"
    assert semantics.image_axis_value("field", "fallback") == "site-7"

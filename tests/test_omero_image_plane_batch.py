"""Tests for typed OMERO image-plane batches."""

import numpy as np
import pytest

from polystore.omero_local import ImagePlaneBatch


def test_image_plane_batch_owns_dimensions_order_padding_and_dtype() -> None:
    batch = ImagePlaneBatch()
    batch.add(z=1, c=0, t=0, data=np.full((1, 2), 3, dtype=np.uint8))
    batch.add(z=0, c=1, t=0, data=np.full((2, 1), 4, dtype=np.uint16))

    planes = list(batch.iter_omero_planes())

    assert (batch.size_z, batch.size_c, batch.size_t) == (2, 2, 1)
    assert len(planes) == 4
    assert all(plane.shape == (2, 2) for plane in planes)
    assert all(plane.dtype == np.dtype(np.uint16) for plane in planes)
    np.testing.assert_array_equal(planes[0], np.zeros((2, 2), dtype=np.uint16))
    np.testing.assert_array_equal(planes[1], np.array([[3, 3], [0, 0]], dtype=np.uint16))
    np.testing.assert_array_equal(planes[2], np.array([[4, 0], [4, 0]], dtype=np.uint16))
    np.testing.assert_array_equal(planes[3], np.zeros((2, 2), dtype=np.uint16))


def test_image_plane_batch_rejects_non_plane_payload() -> None:
    batch = ImagePlaneBatch()

    with pytest.raises(ValueError, match="must be 2D"):
        batch.add(z=0, c=0, t=0, data=np.zeros((2, 3, 4)))


def test_empty_image_plane_batch_cannot_materialize() -> None:
    with pytest.raises(ValueError, match="without planes"):
        list(ImagePlaneBatch().iter_omero_planes())

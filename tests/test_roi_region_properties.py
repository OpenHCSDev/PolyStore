"""Reuse region properties without changing parent-label or contour geometry."""

from collections import Counter

import numpy as np
import pytest
from scipy import ndimage
from skimage import measure

from polystore.disk import DiskStorageBackend
from polystore.roi import MaskShape, extract_rois_from_labeled_mask, load_rois_from_zip


def hole_labels():
    labels = np.zeros((32, 32), dtype=np.int32)
    labels[3:29, 3:29] = 7
    labels[8:24, 8:24] = 0
    labels[12:16, 12:16] = 42
    labels[19:21, 19:21] = 42
    return labels


def border_labels():
    labels = np.zeros((32, 32), dtype=np.int32)
    labels[:4, :6] = 7
    labels[:4, -6:] = 7
    labels[-4:, :6] = 7
    labels[-4:, -6:] = 7
    labels[:12, 14:18] = 42
    labels[14:18, 10:22] = 42
    return labels


def topology_labels():
    labels = np.zeros((32, 32), dtype=np.int32)
    labels[2:8, 2:8] = 7
    labels[2:8, 12:18] = 7
    labels[4, 8:12] = 7  # One-pixel neck between two lobes.
    labels[4:6, 4:6] = 0
    labels[np.arange(20, 26), np.arange(20, 26)] = 188  # Diagonal contact.
    labels[31, 31] = 188  # Disconnected border pixel, same parent.
    return labels


@pytest.fixture(
    params=(hole_labels, border_labels, topology_labels), ids=("holes", "borders", "topology")
)
def labels(request):
    return request.param()


@pytest.mark.parametrize("origin", ((0, 0), (10, 20)))
def test_region_crop_contours_equal_full_canvas_and_archive_reopen(labels, origin, tmp_path):
    original = labels.copy()
    parents = extract_rois_from_labeled_mask(
        labels, min_area=1, spatial_origin_yx=origin, source_spatial_shape_yx=(64, 64)
    )
    regions = measure.regionprops(labels)
    assert [roi.metadata["label"] for roi in parents] == [region.label for region in regions]
    expected_member_labels = []
    expected_contours = []
    for roi, region in zip(parents, regions, strict=True):
        # Independent full-canvas tracing tests the crop/offset optimization,
        # retaining the existing external marching-squares topology contract.
        contours = measure.find_contours(np.pad(labels == region.label, 1), level=0.5)
        contours = [contour + np.asarray(origin) - 1 for contour in contours]
        assert len(roi.shapes) == len(contours)
        assert roi.metadata["area"] == np.count_nonzero(labels == region.label)
        assert roi.metadata["centroid"] == tuple(np.asarray(region.centroid) + origin)
        min_y, min_x, max_y, max_x = region.bbox
        assert roi.metadata["bbox"] == (
            min_y + origin[0],
            min_x + origin[1],
            max_y + origin[0],
            max_x + origin[1],
        )
        assert roi.metadata["source_spatial_shape_yx"] == (64, 64)
        for shape, contour in zip(roi.shapes, contours, strict=True):
            np.testing.assert_array_equal(shape.coordinates, contour)
            np.testing.assert_array_equal(contour[0], contour[-1])
        expected_contours.extend(contours)
        expected_member_labels.extend([region.label] * len(contours))
    archive = tmp_path / "topology.roi.zip"
    DiskStorageBackend().save(parents, archive)
    restored = load_rois_from_zip(archive)
    assert [roi.metadata["label"] for roi in restored] == expected_member_labels
    assert Counter(roi.metadata["label"] for roi in restored) == Counter(expected_member_labels)
    metadata_by_parent = {roi.metadata["label"]: roi.metadata for roi in parents}
    for roi, contour in zip(restored, expected_contours, strict=True):
        assert roi.metadata == metadata_by_parent[roi.metadata["label"]]
        np.testing.assert_array_equal(roi.shapes[0].coordinates, contour)
    # Contours and metadata preserve rings, but the polygon contract does not
    # declare hole subtraction. This is not a ZIP-to-label-raster proof.
    np.testing.assert_array_equal(labels, original)


def test_hole_rings_keep_opposite_orientation():
    (parent, _islands) = extract_rois_from_labeled_mask(hole_labels(), min_area=1)
    assert parent.metadata["label"] == 7
    assert len(parent.shapes) == 2
    signed_areas = []
    for shape in parent.shapes:
        y, x = shape.coordinates.T
        signed_areas.append(np.sum(x[:-1] * y[1:] - x[1:] * y[:-1]) / 2)
    assert signed_areas[0] * signed_areas[1] < 0
    assert abs(signed_areas[0]) > abs(signed_areas[1])


def test_existing_mask_representation_preserves_exact_topology(labels):
    parents = extract_rois_from_labeled_mask(labels, min_area=1, extract_contours=False)
    reconstructed = np.zeros_like(labels)
    for roi in parents:
        (shape,) = roi.shapes
        assert isinstance(shape, MaskShape)
        np.testing.assert_array_equal(shape.mask, labels == roi.metadata["label"])
        reconstructed[shape.mask] = roi.metadata["label"]
    np.testing.assert_array_equal(reconstructed, labels)


@pytest.mark.parametrize("extract_contours", (True, False))
def test_region_properties_scan_labels_once_and_honor_min_area(
    labels, monkeypatch, extract_contours
):
    scans = []
    original_find_objects = ndimage.find_objects

    def counted_scan(mask, *args, **kwargs):
        scans.append(mask.shape)
        return original_find_objects(mask, *args, **kwargs)

    monkeypatch.setattr(ndimage, "find_objects", counted_scan)
    rois = extract_rois_from_labeled_mask(labels, min_area=10, extract_contours=extract_contours)
    assert scans == [labels.shape]
    expected = [
        label
        for label in np.unique(labels)
        if label != 0 and np.count_nonzero(labels == label) >= 10
    ]
    assert [roi.metadata["label"] for roi in rois] == expected

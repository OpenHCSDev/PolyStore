import numpy as np

from polystore.roi import MaskShape
from polystore.roi import PolygonShape
from polystore.roi import load_rois_from_json
from polystore.roi import extract_rois_from_labeled_mask


def test_extract_rois_from_labeled_mask_applies_spatial_origin_to_polygons():
    labels = np.zeros((8, 8), dtype=np.int32)
    labels[2:6, 3:7] = 1

    rois = extract_rois_from_labeled_mask(
        labels,
        min_area=0,
        extract_contours=True,
        spatial_origin_yx=(10, 20),
    )

    assert len(rois) == 1
    assert rois[0].metadata["bbox"] == (12, 23, 16, 27)
    assert rois[0].metadata["centroid"] == (13.5, 24.5)
    assert isinstance(rois[0].shapes[0], PolygonShape)
    assert float(rois[0].shapes[0].coordinates[:, 0].min()) >= 11.5
    assert float(rois[0].shapes[0].coordinates[:, 1].min()) >= 22.5


def test_extract_rois_from_labeled_mask_applies_spatial_origin_to_mask_bbox():
    labels = np.zeros((8, 8), dtype=np.int32)
    labels[2:6, 3:7] = 1

    rois = extract_rois_from_labeled_mask(
        labels,
        min_area=0,
        extract_contours=False,
        spatial_origin_yx=(10, 20),
    )

    assert len(rois) == 1
    assert isinstance(rois[0].shapes[0], MaskShape)
    assert rois[0].shapes[0].bbox == (12, 23, 16, 27)


def test_extract_rois_from_labeled_mask_records_source_canvas_shape():
    labels = np.zeros((8, 8), dtype=np.int32)
    labels[2:6, 3:7] = 1

    rois = extract_rois_from_labeled_mask(
        labels,
        min_area=0,
        source_spatial_shape_yx=(100, 200),
    )

    assert len(rois) == 1
    assert rois[0].metadata["source_spatial_shape_yx"] == (100, 200)


def test_load_rois_from_json_decodes_shapes_through_nominal_registry(tmp_path):
    roi_path = tmp_path / "rois.json"
    roi_path.write_text(
        """
        [
          {
            "metadata": {"label": 1},
            "shapes": [
              {"type": "polygon", "coordinates": [[1, 2], [3, 4], [5, 6]]},
              {"type": "mask", "mask": [[true, false], [false, true]], "bbox": [10, 20, 12, 22]}
            ]
          }
        ]
        """
    )

    rois = load_rois_from_json(roi_path)

    assert len(rois) == 1
    assert isinstance(rois[0].shapes[0], PolygonShape)
    assert isinstance(rois[0].shapes[1], MaskShape)
    assert rois[0].shapes[1].bbox == (10, 20, 12, 22)

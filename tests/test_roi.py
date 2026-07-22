import numpy as np
import pytest

from polystore.disk import DiskStorageBackend
from polystore.roi import (
    ROI,
    EllipseShape,
    MaskShape,
    PolygonShape,
    extract_rois_from_labeled_mask,
    load_rois_from_json,
    load_rois_from_zip,
)
from polystore.roi_converters import NapariROIConverter


def test_napari_roi_converter_projects_ellipse_as_native_bounding_box():
    metadata = {"label": 7, "area": 18.0, "centroid": (10.0, 20.0)}

    payloads = NapariROIConverter.rois_to_shapes(
        [
            ROI(
                shapes=[
                    EllipseShape(
                        center_y=10.0,
                        center_x=20.0,
                        radius_y=3.0,
                        radius_x=5.0,
                    )
                ],
                metadata=metadata,
            )
        ]
    )

    assert payloads == [
        {
            "type": "ellipse",
            "coordinates": [
                [7.0, 15.0],
                [7.0, 25.0],
                [13.0, 25.0],
                [13.0, 15.0],
            ],
            "metadata": metadata,
        }
    ]


def test_napari_roi_converter_removes_only_redundant_polygon_vertices():
    coordinates = np.array(
        [
            [0.0, 0.0],
            [0.0, 1.0],
            [0.0, 1.0],
            [0.0, 2.0],
            [1.0, 2.0],
            [2.0, 2.0],
            [2.0, 1.0],
            [2.0, 0.0],
            [1.0, 0.0],
            [0.0, 0.0],
        ]
    )
    metadata = {"label": 11}
    shape = PolygonShape(coordinates)

    payloads = NapariROIConverter.rois_to_shapes(
        [ROI(shapes=[shape], metadata=metadata)]
    )

    assert payloads == [
        {
            "type": "polygon",
            "coordinates": [
                [0.0, 0.0],
                [0.0, 2.0],
                [2.0, 2.0],
                [2.0, 0.0],
            ],
            "metadata": metadata,
        }
    ]
    assert np.array_equal(shape.coordinates, coordinates)


def test_napari_polygon_projection_retains_collinear_backtracking_vertex():
    coordinates = np.array(
        [
            [0.0, 0.0],
            [0.0, 2.0],
            [0.0, 1.0],
            [1.0, 1.0],
        ]
    )

    payload = NapariROIConverter.rois_to_shapes(
        [ROI(shapes=[PolygonShape(coordinates)], metadata={"label": 12})]
    )[0]

    assert payload["coordinates"] == coordinates.tolist()


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


def test_roi_zip_roundtrip_preserves_source_canvas_shape_metadata(tmp_path):
    pytest.importorskip("roifile")
    path = tmp_path / "labels.roi.zip"
    rois = [
        ROI(
            shapes=[
                PolygonShape(
                    np.array(
                        [[10, 20], [10, 22], [12, 22], [12, 20]],
                        dtype=float,
                    )
                )
            ],
            metadata={"label": 7, "source_spatial_shape_yx": (100, 200)},
        )
    ]

    DiskStorageBackend()._save_rois(rois, path)
    loaded_rois = load_rois_from_zip(path)

    assert loaded_rois[0].metadata["label"] == 7
    assert loaded_rois[0].metadata["source_spatial_shape_yx"] == (100, 200)


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

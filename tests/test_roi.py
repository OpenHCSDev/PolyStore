import numpy as np
import pytest

from polystore.disk import DiskStorageBackend
from polystore.roi import (
    ROI,
    ROI_ZIP_METADATA_MEMBER,
    EllipseShape,
    MaskShape,
    PointShape,
    PolygonShape,
    PolylineShape,
    extract_rois_from_labeled_mask,
    load_rois_from_json,
    load_rois_from_zip,
    roi_zip_metadata_payload,
)
from polystore.roi_converters import NapariROIConverter, UnsupportedImageJROIShapeError


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


def test_imagej_point_archive_roundtrip(tmp_path):
    import zipfile

    roifile = pytest.importorskip("roifile")
    path = tmp_path / "points.roi.zip"
    metadata = {
        "label": 7,
        "centroid": (1.25, 3.5),
        "plane_indices": (2,),
        "plane_shape": (3,),
        "object_name": "centres",
        "source_image_name": "synthetic_source",
    }
    DiskStorageBackend().save([ROI([PointShape(1.25, 3.5)], metadata)], path)
    with zipfile.ZipFile(path) as archive:
        native = roifile.ImagejRoi.frombytes(archive.read("0001.roi"))
    assert native.roitype == roifile.ROI_TYPE.POINT
    np.testing.assert_array_equal(native.coordinates(), [[3.5, 1.25]])
    # Generic leading plane indices are not automatically semantic Z.
    assert (native.c_position, native.z_position, native.t_position) == (0, 0, 0)
    restored = DiskStorageBackend().load(path)
    assert restored[0].shapes == [PointShape(1.25, 3.5)]
    assert restored[0].metadata == metadata
    assert NapariROIConverter.rois_to_shapes(restored)[0]["coordinates"] == [[1.25, 3.5]]


def test_external_imagej_point_archive_preserves_every_point(tmp_path):
    import zipfile

    roifile = pytest.importorskip("roifile")
    path = tmp_path / "external.roi.zip"
    xy = np.array([[3.5, 1.25], [7.75, 9.5]], dtype=np.float32)
    native = roifile.ImagejRoi.frompoints(xy, z=2)
    native.roitype = roifile.ROI_TYPE.POINT
    metadata = {"label": 9, "plane_indices": (2,), "plane_shape": (3,)}
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("points.roi", native.tobytes())
        archive.writestr(
            ROI_ZIP_METADATA_MEMBER, roi_zip_metadata_payload({"points.roi": metadata})
        )
    with zipfile.ZipFile(path) as archive:
        assert roifile.ImagejRoi.frombytes(archive.read("points.roi")).z_position == 3
    restored = load_rois_from_zip(path)
    assert len(restored) == 1
    assert restored[0].shapes == [PointShape(1.25, 3.5), PointShape(9.5, 7.75)]
    assert restored[0].metadata == metadata


@pytest.mark.parametrize(
    "shape,wire_kind",
    [
        (PolygonShape(np.array([[1.25, 3.5], [1.25, 4.5], [2.25, 3.5]])), "FREEHAND"),
        (PolylineShape(np.array([[1.25, 3.5], [2.25, 4.5]])), "POLYLINE"),
        (EllipseShape(10.25, 20.5, 3.125, 5.25), "OVAL"),
    ],
)
def test_imagej_geometry_archive_roundtrip_preserves_native_shape(tmp_path, shape, wire_kind):
    import zipfile

    roifile = pytest.importorskip("roifile")
    path = tmp_path / "geometry.roi.zip"
    metadata = {"label": 3, "source_spatial_shape_yx": (100, 200)}
    DiskStorageBackend().save([ROI([shape], metadata)], path)
    with zipfile.ZipFile(path) as archive:
        native = roifile.ImagejRoi.frombytes(archive.read("0001.roi"))
    assert native.roitype == roifile.ROI_TYPE[wire_kind]
    restored = load_rois_from_zip(path)[0]
    assert type(restored.shapes[0]) is type(shape)
    if isinstance(shape, EllipseShape):
        assert restored.shapes[0] == shape
        assert native.subpixelrect
    else:
        np.testing.assert_array_equal(restored.shapes[0].coordinates, shape.coordinates)
    assert restored.metadata == metadata


def test_external_integer_imagej_oval_preserves_geometry(tmp_path):
    import zipfile

    roifile = pytest.importorskip("roifile")
    native = roifile.ImagejRoi(roitype=roifile.ROI_TYPE.OVAL, left=3, top=1, right=13, bottom=7)
    path = tmp_path / "integer-oval.roi.zip"
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("oval.roi", native.tobytes())
        archive.writestr(ROI_ZIP_METADATA_MEMBER, roi_zip_metadata_payload({"oval.roi": {}}))
    assert load_rois_from_zip(path)[0].shapes == [EllipseShape(4, 8, 3, 5)]


@pytest.mark.parametrize("wire_kind", ["NOROI", "RECT", "LINE", "FREELINE", "ANGLE"])
def test_imagej_archive_rejects_unsupported_native_geometry(tmp_path, wire_kind):
    import zipfile

    roifile = pytest.importorskip("roifile")
    native = roifile.ImagejRoi(roitype=roifile.ROI_TYPE[wire_kind])
    path = tmp_path / "unsupported.roi.zip"
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("invalid.roi", native.tobytes())
        archive.writestr(ROI_ZIP_METADATA_MEMBER, roi_zip_metadata_payload({"invalid.roi": {}}))
    with pytest.raises(UnsupportedImageJROIShapeError, match="has 0 geometry codecs"):
        load_rois_from_zip(path)


def test_imagej_archive_rejects_malformed_geometry_and_missing_metadata(tmp_path):
    import zipfile

    roifile = pytest.importorskip("roifile")
    empty_point = roifile.ImagejRoi(roitype=roifile.ROI_TYPE.POINT)
    # The old writer's one-point FREEHAND is malformed, not a POINT alias.
    malformed_polygon = roifile.ImagejRoi.frompoints(np.array([[3.5, 1.25]]))
    nonfinite_point = roifile.ImagejRoi.frompoints(np.array([[3.5, 1.25]]))
    nonfinite_point.roitype = roifile.ROI_TYPE.POINT
    nonfinite_point.subpixel_coordinates[0, 0] = np.nan
    for index, native in enumerate((empty_point, malformed_polygon, nonfinite_point)):
        path = tmp_path / f"malformed-{index}.roi.zip"
        with zipfile.ZipFile(path, "w") as archive:
            archive.writestr("invalid.roi", native.tobytes())
            archive.writestr(ROI_ZIP_METADATA_MEMBER, roi_zip_metadata_payload({"invalid.roi": {}}))
        with pytest.raises(ValueError):
            load_rois_from_zip(path)

    valid_point = roifile.ImagejRoi.frompoints(np.array([[3.5, 1.25]]))
    valid_point.roitype = roifile.ROI_TYPE.POINT
    path = tmp_path / "missing-metadata.roi.zip"
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("points.roi", valid_point.tobytes())
    with pytest.raises(ValueError, match="metadata"):
        load_rois_from_zip(path)


def test_imagej_archive_rejects_specialized_subtypes_and_masks(tmp_path):
    import zipfile

    roifile = pytest.importorskip("roifile")
    native = roifile.ImagejRoi.frompoints(np.array([[1, 1], [2, 2], [1, 2]]))
    native.subtype = roifile.ROI_SUBTYPE.ELLIPSE
    path = tmp_path / "specialized.roi.zip"
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("specialized.roi", native.tobytes())
        archive.writestr(ROI_ZIP_METADATA_MEMBER, roi_zip_metadata_payload({"specialized.roi": {}}))
    with pytest.raises(UnsupportedImageJROIShapeError, match="subtype or composite"):
        load_rois_from_zip(path)
    with pytest.raises(UnsupportedImageJROIShapeError):
        from polystore.roi_converters import FijiROIConverter

        FijiROIConverter.rois_to_imagej_members(
            [ROI([MaskShape(np.ones((2, 2), dtype=bool), (0, 0, 2, 2))])]
        )

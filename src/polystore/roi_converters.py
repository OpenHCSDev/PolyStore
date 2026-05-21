"""
ROI conversion utilities for streaming backends and viewer servers.

Provides a single source of truth for converting ROI objects to:
- Napari shapes format
- ImageJ ROI bytes
"""

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Any, ClassVar, Dict, List, Tuple

import numpy as np
from metaclass_registry import AutoRegisterMeta

from .roi import EllipseShape, PointShape, PolygonShape, PolylineShape, ROI, ShapeType

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class NapariShapeTypeAlias:
    """Inert alias from Napari wire shape names to ROI shape types."""

    alias: str
    shape_type: ShapeType


NAPARI_SHAPE_TYPE_ALIASES = (
    NapariShapeTypeAlias("path", ShapeType.POLYLINE),
    NapariShapeTypeAlias("points", ShapeType.POINT),
)


class NapariShapeConverter(ABC, metaclass=AutoRegisterMeta):
    """Registered conversion behavior for one ROI shape type."""

    __registry_key__ = "shape_type"
    __skip_if_no_key__ = True

    shape_type: ClassVar[ShapeType | None] = None

    @classmethod
    def for_shape_dict(cls, shape_dict: Dict[str, Any]) -> "NapariShapeConverter":
        return cls.__registry__[_shape_type_from_napari(shape_dict["type"])]()

    def append_common_properties(
        self,
        metadata: Dict[str, Any],
        properties: dict[str, list[Any]],
        centroid: tuple[Any, Any],
        *,
        area: Any | None = None,
    ) -> None:
        properties["label"].append(metadata.get("label", ""))
        properties["area"].append(metadata.get("area", 0) if area is None else area)
        properties["centroid_y"].append(centroid[0])
        properties["centroid_x"].append(centroid[1])

    @abstractmethod
    def add_dimensions(self, shape_dict: Dict[str, Any], prepend_dims: np.ndarray) -> np.ndarray:
        """Add dimensions to a 2D shape to make it nD."""

    @abstractmethod
    def append_napari_format(
        self,
        shape_dict: Dict[str, Any],
        napari_shapes: list[np.ndarray],
        shape_types: list[str],
        properties: dict[str, list[Any]],
    ) -> None:
        """Append this shape to a Napari layer payload."""


def _shape_type_from_napari(shape_type: object) -> ShapeType:
    if isinstance(shape_type, ShapeType):
        return shape_type
    value = str(shape_type.value) if isinstance(shape_type, Enum) else str(shape_type)
    for alias in NAPARI_SHAPE_TYPE_ALIASES:
        if alias.alias == value:
            return alias.shape_type
    return ShapeType(value)


class CoordinateNapariShapeConverter(NapariShapeConverter):
    """Shared converter for coordinate-list shapes."""

    napari_shape_type: ClassVar[str]

    def add_dimensions(self, shape_dict: Dict[str, Any], prepend_dims: np.ndarray) -> np.ndarray:
        coordinates = np.array(shape_dict["coordinates"])
        return np.hstack([np.tile(prepend_dims, (len(coordinates), 1)), coordinates])

    def append_napari_format(
        self,
        shape_dict: Dict[str, Any],
        napari_shapes: list[np.ndarray],
        shape_types: list[str],
        properties: dict[str, list[Any]],
    ) -> None:
        metadata = shape_dict.get("metadata", {})
        napari_shapes.append(np.array(shape_dict["coordinates"]))
        shape_types.append(self.napari_shape_type)
        self.append_common_properties(
            metadata,
            properties,
            metadata.get("centroid", (0, 0)),
        )


class PolygonNapariShapeConverter(CoordinateNapariShapeConverter):
    shape_type = ShapeType.POLYGON
    napari_shape_type = "polygon"


class PolylineNapariShapeConverter(CoordinateNapariShapeConverter):
    shape_type = ShapeType.POLYLINE
    napari_shape_type = "path"


class EllipseNapariShapeConverter(NapariShapeConverter):
    shape_type = ShapeType.ELLIPSE

    def add_dimensions(self, shape_dict: Dict[str, Any], prepend_dims: np.ndarray) -> np.ndarray:
        center = shape_dict["center"]
        radii = shape_dict["radii"]
        corners = np.array(
            [
                [center[0] - radii[0], center[1] - radii[1]],
                [center[0] - radii[0], center[1] + radii[1]],
                [center[0] + radii[0], center[1] + radii[1]],
                [center[0] + radii[0], center[1] - radii[1]],
            ]
        )
        return np.hstack([np.tile(prepend_dims, (4, 1)), corners])

    def append_napari_format(
        self,
        shape_dict: Dict[str, Any],
        napari_shapes: list[np.ndarray],
        shape_types: list[str],
        properties: dict[str, list[Any]],
    ) -> None:
        metadata = shape_dict.get("metadata", {})
        center = np.array(shape_dict["center"])
        radii = np.array(shape_dict["radii"])
        napari_shapes.append(np.array([center - radii, center + radii]))
        shape_types.append("ellipse")
        self.append_common_properties(
            metadata,
            properties,
            metadata.get("centroid", (0, 0)),
        )


class PointNapariShapeConverter(NapariShapeConverter):
    shape_type = ShapeType.POINT

    def add_dimensions(self, shape_dict: Dict[str, Any], prepend_dims: np.ndarray) -> np.ndarray:
        return np.concatenate([prepend_dims, shape_dict["coordinates"]]).reshape(1, -1)

    def append_napari_format(
        self,
        shape_dict: Dict[str, Any],
        napari_shapes: list[np.ndarray],
        shape_types: list[str],
        properties: dict[str, list[Any]],
    ) -> None:
        metadata = shape_dict.get("metadata", {})
        coordinates = shape_dict["coordinates"]
        napari_shapes.append(np.array([coordinates]))
        shape_types.append("point")
        self.append_common_properties(metadata, properties, coordinates, area=0)


class NapariROIConverter:
    """Convert ROI objects to Napari shapes format."""

    @staticmethod
    def add_dimensions_to_shape(shape_dict: Dict[str, Any], prepend_dims: List[float]) -> np.ndarray:
        """Add dimensions to a 2D shape to make it nD."""
        return NapariShapeConverter.for_shape_dict(shape_dict).add_dimensions(
            shape_dict,
            np.array(prepend_dims),
        )

    @staticmethod
    def rois_to_shapes(rois: List[ROI]) -> List[Dict[str, Any]]:
        """Convert ROI objects to Napari shapes data."""
        shapes_data = []
        for roi in rois:
            if roi.shapes and all(isinstance(shape, PointShape) for shape in roi.shapes):
                points = [[shape.y, shape.x] for shape in roi.shapes]
                shapes_data.append({"type": "points", "coordinates": points, "metadata": roi.metadata})
            else:
                for shape in roi.shapes:
                    if isinstance(shape, PolygonShape):
                        shapes_data.append(
                            {"type": "polygon", "coordinates": shape.coordinates.tolist(), "metadata": roi.metadata}
                        )
                    elif isinstance(shape, PolylineShape):
                        shapes_data.append(
                            {"type": "path", "coordinates": shape.coordinates.tolist(), "metadata": roi.metadata}
                        )
                    elif isinstance(shape, EllipseShape):
                        shapes_data.append(
                            {
                                "type": "ellipse",
                                "center": [shape.center_y, shape.center_x],
                                "radii": [shape.radius_y, shape.radius_x],
                                "metadata": roi.metadata,
                            }
                        )
                    elif isinstance(shape, PointShape):
                        shapes_data.append({"type": "point", "coordinates": [shape.y, shape.x], "metadata": roi.metadata})
        return shapes_data

    @staticmethod
    def shapes_to_napari_format(shapes_data: List[Dict]) -> Tuple[List[np.ndarray], List[str], Dict]:
        """Convert shape dicts to Napari layer format."""
        napari_shapes = []
        shape_types = []
        properties = {"label": [], "area": [], "centroid_y": [], "centroid_x": []}

        for shape_dict in shapes_data:
            NapariShapeConverter.for_shape_dict(shape_dict).append_napari_format(
                shape_dict,
                napari_shapes,
                shape_types,
                properties,
            )

        return napari_shapes, shape_types, properties


class FijiROIConverter:
    """Convert ROI objects to ImageJ ROI bytes."""

    @staticmethod
    def rois_to_imagej_bytes(rois: List[ROI], roi_prefix: str = "") -> List[bytes]:
        """Convert ROI objects to ImageJ ROI bytes."""
        try:
            from roifile import ImagejRoi, ROI_TYPE
        except ImportError:
            raise ImportError("roifile library required for ImageJ ROI conversion. Install with: pip install roifile")

        roi_bytes_list = []
        for roi in rois:
            for shape in roi.shapes:
                if isinstance(shape, PolygonShape):
                    coords_xy = shape.coordinates[:, [1, 0]]
                    ij_roi = ImagejRoi.frompoints(coords_xy)
                    ij_roi.name = f"{roi_prefix}_ROI_{roi.metadata.get('label', '')}".rstrip("_")
                    roi_bytes_list.append(ij_roi.tobytes())
                elif isinstance(shape, PolylineShape):
                    coords_xy = shape.coordinates[:, [1, 0]]
                    ij_roi = ImagejRoi.frompoints(coords_xy)
                    ij_roi.roitype = ROI_TYPE.POLYLINE
                    ij_roi.name = f"{roi_prefix}_ROI_{roi.metadata.get('label', '')}".rstrip("_")
                    roi_bytes_list.append(ij_roi.tobytes())
                elif isinstance(shape, EllipseShape):
                    center_x = shape.center_x
                    center_y = shape.center_y
                    radius_x = shape.radius_x
                    radius_y = shape.radius_y
                    left = center_x - radius_x
                    top = center_y - radius_y
                    width = 2 * radius_x
                    height = 2 * radius_y
                    ij_roi = ImagejRoi.frompoints(np.array([[left, top], [left + width, top + height]]))
                    ij_roi.roitype = ImagejRoi.OVAL if hasattr(ImagejRoi, "OVAL") else ROI_TYPE.OVAL
                    ij_roi.name = f"{roi_prefix}_ROI_{roi.metadata.get('label', '')}".rstrip("_")
                    roi_bytes_list.append(ij_roi.tobytes())
                elif isinstance(shape, PointShape):
                    coords_xy = np.array([[shape.x, shape.y]])
                    ij_roi = ImagejRoi.frompoints(coords_xy)
                    ij_roi.name = f"{roi_prefix}_ROI_{roi.metadata.get('label', '')}".rstrip("_")
                    roi_bytes_list.append(ij_roi.tobytes())

        return roi_bytes_list

    @staticmethod
    def encode_rois_for_transmission(roi_bytes_list: List[bytes]) -> List[str]:
        """Base64 encode ROI bytes for JSON transmission."""
        import base64
        return [base64.b64encode(roi_bytes).decode("utf-8") for roi_bytes in roi_bytes_list]

    @staticmethod
    def decode_rois_from_transmission(encoded_rois: List[str]) -> List[bytes]:
        """Decode base64-encoded ROI bytes."""
        import base64
        return [base64.b64decode(roi_encoded) for roi_encoded in encoded_rois]

    @staticmethod
    def bytes_to_java_roi(roi_bytes: bytes, scyjava_module) -> Any:
        """Convert ROI bytes to Java ROI object via temporary file."""
        import os
        import tempfile

        RoiDecoder = scyjava_module.jimport("ij.io.RoiDecoder")
        with tempfile.NamedTemporaryFile(suffix=".roi", delete=False) as tmp:
            tmp.write(roi_bytes)
            tmp_path = tmp.name

        try:
            roi_decoder = RoiDecoder(tmp_path)
            return roi_decoder.getRoi()
        finally:
            os.unlink(tmp_path)

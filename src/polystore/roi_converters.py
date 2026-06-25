"""
ROI conversion utilities for streaming backends and viewer servers.

Provides a single source of truth for converting ROI objects to:
- Napari shapes format
- ImageJ ROI bytes
"""

import logging
from abc import ABC, abstractmethod
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any, ClassVar, Dict, List, Tuple

import numpy as np
from metaclass_registry import AutoRegisterMeta

from .roi import (
    EllipseShape,
    MaskShape,
    PointShape,
    PolygonShape,
    PolylineShape,
    ROI,
    ROIShape,
    ShapeType,
    ShapeTypeRegistryBase,
)
from .streaming_constants import StreamingDataType

logger = logging.getLogger(__name__)


class UnsupportedImageJROIShapeError(ValueError):
    """Raised when an ROI shape has no ImageJ .roi representation."""


class UnsupportedNapariROIShapeError(ValueError):
    """Raised when an ROI shape has no Napari vector-payload representation."""


@dataclass(frozen=True, slots=True)
class ImageJROIMember:
    """ImageJ ROI archive member with the metadata that must follow it."""

    imagej_roi: Any
    metadata: Dict[str, Any]


@dataclass(frozen=True, slots=True)
class NapariShapeTypeAlias:
    """Inert alias from Napari wire shape names to ROI shape types."""

    alias: str
    shape_type: ShapeType


NAPARI_SHAPE_TYPE_ALIASES = (
    NapariShapeTypeAlias("path", ShapeType.POLYLINE),
    NapariShapeTypeAlias("points", ShapeType.POINT),
)


@dataclass(frozen=True, slots=True)
class NapariShapeMetadata:
    """Required metadata carried by one Napari ROI shape payload."""

    label: Any
    area: Any
    centroid_yx: tuple[Any, Any]

    @classmethod
    def from_shape_payload(
        cls,
        shape_dict: Mapping[str, Any],
        *,
        area: Any | None = None,
        centroid_yx: Sequence[Any] | None = None,
    ) -> "NapariShapeMetadata":
        metadata = required_shape_metadata(shape_dict)
        return cls.from_metadata(metadata, area=area, centroid_yx=centroid_yx)

    @classmethod
    def from_metadata(
        cls,
        metadata: Mapping[str, Any],
        *,
        area: Any | None = None,
        centroid_yx: Sequence[Any] | None = None,
    ) -> "NapariShapeMetadata":
        if "label" not in metadata:
            raise ValueError("Napari shape metadata missing required 'label'.")
        resolved_area = cls._required_value(metadata, "area") if area is None else area
        resolved_centroid = (
            cls._required_value(metadata, "centroid")
            if centroid_yx is None
            else centroid_yx
        )
        if len(resolved_centroid) != 2:
            raise ValueError(
                "Napari shape metadata 'centroid' must contain exactly two values."
            )
        return cls(
            label=metadata["label"],
            area=resolved_area,
            centroid_yx=(resolved_centroid[0], resolved_centroid[1]),
        )

    @staticmethod
    def _required_value(metadata: Mapping[str, Any], field: str) -> Any:
        if field not in metadata:
            raise ValueError(f"Napari shape metadata missing required {field!r}.")
        return metadata[field]


def required_shape_metadata(shape_dict: Mapping[str, Any]) -> Mapping[str, Any]:
    """Return the required metadata mapping from a Napari ROI shape payload."""
    if "metadata" not in shape_dict:
        raise ValueError("Napari shape payload missing required 'metadata'.")
    metadata = shape_dict["metadata"]
    if not isinstance(metadata, Mapping):
        raise TypeError("Napari shape payload 'metadata' must be a mapping.")
    return metadata


@dataclass(slots=True)
class NapariShapeProperties:
    """Mutable Napari shape-property columns collected during layer conversion."""

    label: list[Any] = field(default_factory=list)
    area: list[Any] = field(default_factory=list)
    centroid_y: list[Any] = field(default_factory=list)
    centroid_x: list[Any] = field(default_factory=list)

    def append(self, metadata: NapariShapeMetadata) -> None:
        self.label.append(metadata.label)
        self.area.append(metadata.area)
        self.centroid_y.append(metadata.centroid_yx[0])
        self.centroid_x.append(metadata.centroid_yx[1])

    def to_mapping(self) -> dict[str, list[Any]]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class NapariEllipsePayload:
    """Required geometric fields for one Napari ellipse payload."""

    center_yx: np.ndarray
    radii_yx: np.ndarray

    @classmethod
    def from_shape_payload(
        cls,
        shape_dict: Mapping[str, Any],
    ) -> "NapariEllipsePayload":
        return cls(
            center_yx=np.array(cls._required_field(shape_dict, "center")),
            radii_yx=np.array(cls._required_field(shape_dict, "radii")),
        )

    @staticmethod
    def _required_field(shape_dict: Mapping[str, Any], field_name: str) -> Any:
        if field_name not in shape_dict:
            raise ValueError(
                f"Napari ellipse payload missing required {field_name!r}."
            )
        return shape_dict[field_name]

    def corner_rows(self) -> np.ndarray:
        return np.array(
            [
                [
                    self.center_yx[0] - self.radii_yx[0],
                    self.center_yx[1] - self.radii_yx[1],
                ],
                [
                    self.center_yx[0] - self.radii_yx[0],
                    self.center_yx[1] + self.radii_yx[1],
                ],
                [
                    self.center_yx[0] + self.radii_yx[0],
                    self.center_yx[1] + self.radii_yx[1],
                ],
                [
                    self.center_yx[0] + self.radii_yx[0],
                    self.center_yx[1] - self.radii_yx[1],
                ],
            ]
        )

    def bounding_box_rows(self) -> np.ndarray:
        return np.array(
            [
                self.center_yx - self.radii_yx,
                self.center_yx + self.radii_yx,
            ]
        )


class NapariShapeConverter(ShapeTypeRegistryBase, ABC, metaclass=AutoRegisterMeta):
    """Registered conversion behavior for one ROI shape type."""

    @classmethod
    def for_shape_dict(cls, shape_dict: Dict[str, Any]) -> "NapariShapeConverter":
        return cls.__registry__[_shape_type_from_napari(shape_dict["type"])]()

    def append_common_properties(
        self,
        metadata: NapariShapeMetadata,
        properties: NapariShapeProperties,
    ) -> None:
        properties.append(metadata)

    @abstractmethod
    def add_dimensions(self, shape_dict: Dict[str, Any], prepend_dims: np.ndarray) -> np.ndarray:
        """Add dimensions to a 2D shape to make it nD."""

    @abstractmethod
    def append_napari_format(
        self,
        shape_dict: Dict[str, Any],
        napari_shapes: list[np.ndarray],
        shape_types: list[str],
        properties: NapariShapeProperties,
    ) -> None:
        """Append this shape to a Napari layer payload."""


def _shape_type_from_napari(shape_type: object) -> ShapeType:
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
        properties: NapariShapeProperties,
    ) -> None:
        napari_shapes.append(np.array(shape_dict["coordinates"]))
        shape_types.append(self.napari_shape_type)
        self.append_common_properties(
            NapariShapeMetadata.from_shape_payload(shape_dict),
            properties,
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
        ellipse = NapariEllipsePayload.from_shape_payload(shape_dict)
        return np.hstack([np.tile(prepend_dims, (4, 1)), ellipse.corner_rows()])

    def append_napari_format(
        self,
        shape_dict: Dict[str, Any],
        napari_shapes: list[np.ndarray],
        shape_types: list[str],
        properties: NapariShapeProperties,
    ) -> None:
        ellipse = NapariEllipsePayload.from_shape_payload(shape_dict)
        napari_shapes.append(ellipse.bounding_box_rows())
        shape_types.append("ellipse")
        self.append_common_properties(
            NapariShapeMetadata.from_shape_payload(shape_dict),
            properties,
        )


class PointNapariShapeConverter(NapariShapeConverter):
    shape_type = ShapeType.POINT

    def add_dimensions(self, shape_dict: Dict[str, Any], prepend_dims: np.ndarray) -> np.ndarray:
        coordinates = np.array(shape_dict["coordinates"])
        if coordinates.ndim == 1:
            coordinates = coordinates.reshape(1, -1)
        return np.hstack([np.tile(prepend_dims, (len(coordinates), 1)), coordinates])

    def append_napari_format(
        self,
        shape_dict: Dict[str, Any],
        napari_shapes: list[np.ndarray],
        shape_types: list[str],
        properties: NapariShapeProperties,
    ) -> None:
        coordinates = np.array(shape_dict["coordinates"])
        if coordinates.ndim == 1:
            coordinates = coordinates.reshape(1, -1)
        for coordinate in coordinates:
            napari_shapes.append(np.array([coordinate]))
            shape_types.append("point")
            self.append_common_properties(
                NapariShapeMetadata.from_shape_payload(
                    shape_dict,
                    centroid_yx=coordinate,
                    area=0,
                ),
                properties,
            )


class ROIShapeConverterRegistryBase(ShapeTypeRegistryBase):
    """Shared lookup behavior for ROI-shape converter registries."""

    @classmethod
    def for_shape(cls, shape: ROIShape) -> Any:
        return cls.__registry__[shape.shape_type]()


class ROIShapeNapariPayloadConverter(
    ROIShapeConverterRegistryBase,
    ABC,
    metaclass=AutoRegisterMeta,
):
    """Registered projection from ROI shape objects into Napari wire payloads."""

    streaming_data_type: ClassVar[StreamingDataType] = StreamingDataType.SHAPES

    @classmethod
    def streaming_data_type_for_rois(cls, rois: List[ROI]) -> StreamingDataType:
        shape_stream_types = tuple(
            cls.for_shape(shape).streaming_data_type
            for roi in rois
            for shape in roi.shapes
        )
        if shape_stream_types and all(
            stream_type == StreamingDataType.POINTS
            for stream_type in shape_stream_types
        ):
            return StreamingDataType.POINTS
        return StreamingDataType.SHAPES

    @abstractmethod
    def shape_payloads(
        self,
        shape: ROIShape,
        metadata: Dict[str, Any],
    ) -> tuple[Dict[str, Any], ...]:
        """Return one or more Napari shape dictionaries for this ROI shape."""


class CoordinateROIShapeNapariPayloadConverter(ROIShapeNapariPayloadConverter):
    """Shared Napari payload projection for coordinate-list ROI shapes."""

    napari_payload_type: ClassVar[str]

    def shape_payloads(
        self,
        shape: ROIShape,
        metadata: Dict[str, Any],
    ) -> tuple[Dict[str, Any], ...]:
        return (
            {
                "type": self.napari_payload_type,
                "coordinates": self.coordinates_yx(shape).tolist(),
                "metadata": metadata,
            },
        )

    @abstractmethod
    def coordinates_yx(self, shape: ROIShape) -> np.ndarray:
        """Return shape coordinates as an Nx2 YX array."""


class PolygonROIShapeNapariPayloadConverter(CoordinateROIShapeNapariPayloadConverter):
    shape_type = ShapeType.POLYGON
    napari_payload_type = "polygon"

    def coordinates_yx(self, shape: PolygonShape) -> np.ndarray:
        return shape.coordinates


class PolylineROIShapeNapariPayloadConverter(CoordinateROIShapeNapariPayloadConverter):
    shape_type = ShapeType.POLYLINE
    napari_payload_type = "path"

    def coordinates_yx(self, shape: PolylineShape) -> np.ndarray:
        return shape.coordinates


class EllipseROIShapeNapariPayloadConverter(ROIShapeNapariPayloadConverter):
    shape_type = ShapeType.ELLIPSE

    def shape_payloads(
        self,
        shape: EllipseShape,
        metadata: Dict[str, Any],
    ) -> tuple[Dict[str, Any], ...]:
        return (
            {
                "type": "ellipse",
                "center": [shape.center_y, shape.center_x],
                "radii": [shape.radius_y, shape.radius_x],
                "metadata": metadata,
            },
        )


class PointROIShapeNapariPayloadConverter(ROIShapeNapariPayloadConverter):
    shape_type = ShapeType.POINT
    streaming_data_type = StreamingDataType.POINTS

    def shape_payloads(
        self,
        shape: PointShape,
        metadata: Dict[str, Any],
    ) -> tuple[Dict[str, Any], ...]:
        return (
            {
                "type": "points",
                "coordinates": [[shape.y, shape.x]],
                "metadata": metadata,
            },
        )


class MaskROIShapeNapariPayloadConverter(ROIShapeNapariPayloadConverter):
    shape_type = ShapeType.MASK

    def shape_payloads(
        self,
        shape: MaskShape,
        metadata: Dict[str, Any],
    ) -> tuple[Dict[str, Any], ...]:
        raise UnsupportedNapariROIShapeError(
            "MaskShape cannot be represented as a Napari vector ROI payload."
        )


class ImageJROIShapeConverter(
    ROIShapeConverterRegistryBase,
    ABC,
    metaclass=AutoRegisterMeta,
):
    """Registered projection from ROI shape objects into ImageJ ROI records."""

    @abstractmethod
    def imagej_roi(self, shape: ROIShape, name: str) -> Any:
        """Return a roifile ImagejRoi for this ROI shape."""


class PolygonImageJROIShapeConverter(ImageJROIShapeConverter):
    shape_type = ShapeType.POLYGON

    def imagej_roi(self, shape: PolygonShape, name: str) -> Any:
        from roifile import ImagejRoi

        imagej_roi = ImagejRoi.frompoints(shape.coordinates[:, [1, 0]])
        imagej_roi.name = name
        return imagej_roi


class PolylineImageJROIShapeConverter(ImageJROIShapeConverter):
    shape_type = ShapeType.POLYLINE

    def imagej_roi(self, shape: PolylineShape, name: str) -> Any:
        from roifile import ImagejRoi, ROI_TYPE

        imagej_roi = ImagejRoi.frompoints(shape.coordinates[:, [1, 0]])
        imagej_roi.roitype = ROI_TYPE.POLYLINE
        imagej_roi.name = name
        return imagej_roi


class EllipseImageJROIShapeConverter(ImageJROIShapeConverter):
    shape_type = ShapeType.ELLIPSE

    def imagej_roi(self, shape: EllipseShape, name: str) -> Any:
        from roifile import ImagejRoi, ROI_TYPE

        left = shape.center_x - shape.radius_x
        top = shape.center_y - shape.radius_y
        width = 2 * shape.radius_x
        height = 2 * shape.radius_y
        imagej_roi = ImagejRoi.frompoints(
            np.array([[left, top], [left + width, top + height]])
        )
        imagej_roi.roitype = ROI_TYPE.OVAL
        imagej_roi.name = name
        return imagej_roi


class PointImageJROIShapeConverter(ImageJROIShapeConverter):
    shape_type = ShapeType.POINT

    def imagej_roi(self, shape: PointShape, name: str) -> Any:
        from roifile import ImagejRoi

        imagej_roi = ImagejRoi.frompoints(np.array([[shape.x, shape.y]]))
        imagej_roi.name = name
        return imagej_roi


class MaskImageJROIShapeConverter(ImageJROIShapeConverter):
    shape_type = ShapeType.MASK

    def imagej_roi(self, shape: MaskShape, name: str) -> Any:
        raise UnsupportedImageJROIShapeError(
            "MaskShape cannot be represented in ImageJ .roi format."
        )


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
            for shape in roi.shapes:
                shapes_data.extend(
                    ROIShapeNapariPayloadConverter.for_shape(shape).shape_payloads(
                        shape,
                        roi.metadata,
                    )
                )
        return shapes_data

    @staticmethod
    def shapes_to_napari_format(shapes_data: List[Dict]) -> Tuple[List[np.ndarray], List[str], Dict]:
        """Convert shape dicts to Napari layer format."""
        napari_shapes = []
        shape_types = []
        properties = NapariShapeProperties()

        for shape_dict in shapes_data:
            NapariShapeConverter.for_shape_dict(shape_dict).append_napari_format(
                shape_dict,
                napari_shapes,
                shape_types,
                properties,
            )

        return napari_shapes, shape_types, properties.to_mapping()


class FijiROIConverter:
    """Convert ROI objects to ImageJ ROI bytes."""

    @staticmethod
    def rois_to_imagej_members(
        rois: List[ROI],
        roi_prefix: str = "",
    ) -> List[ImageJROIMember]:
        """Convert ROI objects to ImageJ ROI members with per-member metadata."""
        try:
            import roifile  # noqa: F401
        except ImportError:
            raise ImportError("roifile library required for ImageJ ROI conversion. Install with: pip install roifile")

        members: list[ImageJROIMember] = []
        for roi_index, roi in enumerate(rois, start=1):
            for shape_index, shape in enumerate(roi.shapes, start=1):
                imagej_roi = ImageJROIShapeConverter.for_shape(shape).imagej_roi(
                    shape,
                    FijiROIConverter.imagej_roi_name(
                        roi_prefix=roi_prefix,
                        roi_index=roi_index,
                        shape_index=shape_index,
                    ),
                )
                members.append(
                    ImageJROIMember(
                        imagej_roi=imagej_roi,
                        metadata=dict(roi.metadata),
                    )
                )
        return members

    @staticmethod
    def imagej_roi_name(
        *,
        roi_prefix: str,
        roi_index: int,
        shape_index: int,
    ) -> str:
        """Return the stable ImageJ ROI name for one projected shape."""
        stem = f"ROI_{roi_index}_{shape_index}"
        if roi_prefix:
            return f"{roi_prefix}_{stem}"
        return stem

    @staticmethod
    def rois_to_imagej_bytes(rois: List[ROI], roi_prefix: str = "") -> List[bytes]:
        """Convert ROI objects to ImageJ ROI bytes."""
        return [
            member.imagej_roi.tobytes()
            for member in FijiROIConverter.rois_to_imagej_members(
                rois,
                roi_prefix=roi_prefix,
            )
        ]

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
    def transmission_to_java_rois(
        encoded_rois: List[str],
        scyjava_module,
    ) -> List[Any]:
        """Decode transmitted ImageJ ROI bytes into Java ROI instances."""
        return [
            FijiROIConverter.bytes_to_java_roi(roi_bytes, scyjava_module)
            for roi_bytes in FijiROIConverter.decode_rois_from_transmission(
                encoded_rois
            )
        ]

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

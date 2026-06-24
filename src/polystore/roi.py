"""
Generic ROI (Region of Interest) system for polystore.

Provides backend-agnostic ROI extraction and representation.
ROIs can be materialized to multiple backends (disk, streaming, OMERO).
"""

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, ClassVar, Dict, List, Optional, Tuple, Union

import numpy as np
from metaclass_registry import AutoRegisterMeta
from zmqruntime.viewer_protocol import (
    ViewerSourceSpatialDomainPayload,
    ViewerSourceSpatialWireField,
)

from .constants import Backend
from .formats import FileFormat

logger = logging.getLogger(__name__)


ROI_ZIP_EXTENSION = FileFormat.ROI.extensions[0]
ROI_ZIP_METADATA_MEMBER = "__polystore_roi_metadata__.json"
ROI_ZIP_SUFFIXES = tuple(Path(f"archive{ROI_ZIP_EXTENSION}").suffixes)
ROI_TUPLE_METADATA_KEYS = frozenset(
    (
        "bbox",
        "centroid",
        "plane_indices",
        "plane_shape",
        ViewerSourceSpatialWireField.SOURCE_SPATIAL_SHAPE_YX.value,
        ViewerSourceSpatialWireField.SPATIAL_ORIGIN_YX.value,
    )
)


@dataclass(frozen=True, slots=True)
class ROIArchivePath:
    """Normalized filesystem path for an ImageJ ROI zip archive."""

    path: Path

    @classmethod
    def from_output_path(cls, output_path: Union[str, Path]) -> "ROIArchivePath":
        path = Path(output_path)
        if tuple(path.suffixes[-len(ROI_ZIP_SUFFIXES):]) == ROI_ZIP_SUFFIXES:
            return cls(path)
        return cls(path.with_suffix(ROI_ZIP_EXTENSION))


def roi_zip_metadata_payload(metadata_by_filename: Dict[str, Dict[str, Any]]) -> str:
    """Serialize per-ROI metadata into an ImageJ-compatible zip sidecar."""
    import json

    return json.dumps(
        {
            filename: _jsonable_roi_metadata(metadata)
            for filename, metadata in metadata_by_filename.items()
        },
        sort_keys=True,
    )


def load_roi_zip_metadata(zip_file: Any) -> Dict[str, Dict[str, Any]]:
    """Load per-ROI metadata sidecar from a .roi.zip archive."""
    import json

    if ROI_ZIP_METADATA_MEMBER not in zip_file.namelist():
        return {}
    payload = json.loads(zip_file.read(ROI_ZIP_METADATA_MEMBER).decode("utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(
            f"Invalid ROI metadata sidecar in {ROI_ZIP_METADATA_MEMBER}: expected mapping."
        )
    return {
        str(filename): _restore_roi_metadata(metadata)
        for filename, metadata in payload.items()
        if isinstance(metadata, dict)
    }


def _jsonable_roi_metadata(value: Any) -> Any:
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, np.ndarray):
        return value.tolist()
    if isinstance(value, tuple):
        return [_jsonable_roi_metadata(item) for item in value]
    if isinstance(value, list):
        return [_jsonable_roi_metadata(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _jsonable_roi_metadata(item) for key, item in value.items()}
    return value


def _restore_roi_metadata(metadata: Dict[str, Any]) -> Dict[str, Any]:
    restored = dict(metadata)
    for key in ROI_TUPLE_METADATA_KEYS:
        value = restored.get(key)
        if isinstance(value, list):
            restored[key] = tuple(value)
    return restored


class ShapeType(Enum):
    """ROI shape types."""
    POLYGON = "polygon"
    POLYLINE = "polyline"
    MASK = "mask"
    POINT = "point"
    ELLIPSE = "ellipse"


class ShapeTypeRegistryBase(ABC):
    """Shared declaration surface for shape-type keyed registries."""

    __registry_key__ = "shape_type"
    __skip_if_no_key__ = True

    shape_type: ClassVar[ShapeType | None] = None


class ROIShape(ABC):
    """Nominal base for all ROI shape records."""

    shape_type: ClassVar[ShapeType]


@dataclass(frozen=True)
class PolygonShape(ROIShape):
    """Polygon ROI shape defined by vertex coordinates."""
    coordinates: np.ndarray  # Nx2 array of (y, x) coordinates
    shape_type: ClassVar[ShapeType] = ShapeType.POLYGON

    def __post_init__(self):
        if self.coordinates.ndim != 2 or self.coordinates.shape[1] != 2:
            raise ValueError(f"Polygon coordinates must be Nx2 array, got shape {self.coordinates.shape}")
        if len(self.coordinates) < 3:
            raise ValueError(f"Polygon must have at least 3 vertices, got {len(self.coordinates)}")


@dataclass(frozen=True)
class PolylineShape(ROIShape):
    """Polyline ROI shape defined by path coordinates (open path, not closed polygon)."""
    coordinates: np.ndarray  # Nx2 array of (y, x) coordinates
    shape_type: ClassVar[ShapeType] = ShapeType.POLYLINE

    def __post_init__(self):
        if self.coordinates.ndim != 2 or self.coordinates.shape[1] != 2:
            raise ValueError(f"Polyline coordinates must be Nx2 array, got shape {self.coordinates.shape}")
        if len(self.coordinates) < 2:
            raise ValueError(f"Polyline must have at least 2 points, got {len(self.coordinates)}")


@dataclass(frozen=True)
class MaskShape(ROIShape):
    """Binary mask ROI shape."""
    mask: np.ndarray  # 2D boolean array
    bbox: Tuple[int, int, int, int]  # (min_y, min_x, max_y, max_x)
    shape_type: ClassVar[ShapeType] = ShapeType.MASK

    def __post_init__(self):
        if self.mask.ndim != 2:
            raise ValueError(f"Mask must be 2D array, got shape {self.mask.shape}")
        if self.mask.dtype != bool:
            raise ValueError(f"Mask must be boolean array, got dtype {self.mask.dtype}")


@dataclass(frozen=True)
class PointShape(ROIShape):
    """Point ROI shape."""
    y: float
    x: float
    shape_type: ClassVar[ShapeType] = ShapeType.POINT


@dataclass(frozen=True)
class EllipseShape(ROIShape):
    """Ellipse ROI shape."""
    center_y: float
    center_x: float
    radius_y: float
    radius_x: float
    shape_type: ClassVar[ShapeType] = ShapeType.ELLIPSE


@dataclass(frozen=True)
class ROI:
    """Region of Interest with metadata."""
    shapes: List[Any]
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if not self.shapes:
            raise ValueError("ROI must have at least one shape")
        for shape in self.shapes:
            if not isinstance(shape, ROIShape):
                raise ValueError(f"Shape {shape} must be an ROIShape")


@dataclass(frozen=True, slots=True)
class LabeledMaskROIExtractionRequest:
    """Request to extract ROIs from a labeled mask or stack."""

    labeled_mask: np.ndarray
    min_area: int = 10
    extract_contours: bool = True
    spatial_origin_yx: Optional[Tuple[int, int]] = None
    source_spatial_shape_yx: Optional[Tuple[int, int]] = None


class LabeledMaskROIExtractor(ABC, metaclass=AutoRegisterMeta):
    """Registered extraction behavior for one labeled-mask dimensional family."""

    __registry_key__ = "__name__"
    __skip_if_no_key__ = True

    @classmethod
    def for_request(
        cls,
        request: LabeledMaskROIExtractionRequest,
    ) -> "LabeledMaskROIExtractor":
        for extractor_type in cls.__registry__.values():
            extractor = extractor_type()
            if extractor.accepts(request.labeled_mask):
                return extractor
        raise ValueError(
            "No ROI extractor registered for labeled mask shape "
            f"{request.labeled_mask.shape}."
        )

    @abstractmethod
    def accepts(self, labeled_mask: np.ndarray) -> bool:
        """Return whether this extractor owns the mask dimensionality."""

    @abstractmethod
    def extract(self, request: LabeledMaskROIExtractionRequest) -> List[ROI]:
        """Extract ROIs from the request."""


class TwoDimensionalLabeledMaskROIExtractor(LabeledMaskROIExtractor):
    """Extract ROIs from a single 2D labeled mask."""

    def accepts(self, labeled_mask: np.ndarray) -> bool:
        return labeled_mask.ndim == 2

    def extract(self, request: LabeledMaskROIExtractionRequest) -> List[ROI]:
        from skimage import measure
        from skimage.measure import regionprops
        from scipy.ndimage import find_objects

        labeled_mask = request.labeled_mask
        if not np.issubdtype(labeled_mask.dtype, np.integer):
            labeled_mask = labeled_mask.astype(np.int32)

        regions = regionprops(labeled_mask)
        slices = find_objects(labeled_mask)
        origin_y, origin_x = request.spatial_origin_yx or (0, 0)

        rois = []
        for region in regions:
            if region.area < request.min_area:
                continue
            min_y, min_x, max_y, max_x = region.bbox

            metadata = {
                "label": int(region.label),
                "area": float(region.area),
                "perimeter": float(region.perimeter),
                "centroid": (
                    float(region.centroid[0] + origin_y),
                    float(region.centroid[1] + origin_x),
                ),
                "bbox": (
                    int(min_y + origin_y),
                    int(min_x + origin_x),
                    int(max_y + origin_y),
                    int(max_x + origin_x),
                ),
            }
            metadata.update(
                ViewerSourceSpatialDomainPayload(
                    origin_yx=request.spatial_origin_yx,
                    source_shape_yx=request.source_spatial_shape_yx,
                ).to_wire_mapping()
            )

            shapes = []
            if request.extract_contours:
                label_idx = region.label - 1
                if label_idx < len(slices) and slices[label_idx] is not None:
                    slice_y, slice_x = slices[label_idx]
                    cropped_mask = labeled_mask[slice_y, slice_x]
                    binary_mask = (cropped_mask == region.label).astype(np.uint8)
                    padded_mask = np.pad(binary_mask, pad_width=1, mode="constant", constant_values=0)
                    contours = measure.find_contours(padded_mask, level=0.5)
                    offset_y = slice_y.start
                    offset_x = slice_x.start
                    padding_offset = np.array([offset_y + origin_y, offset_x + origin_x]) - 1
                    for contour in contours:
                        if len(contour) >= 3:
                            contour_full = contour + padding_offset
                            shapes.append(PolygonShape(coordinates=contour_full))
            else:
                binary_mask = labeled_mask == region.label
                shapes.append(MaskShape(mask=binary_mask, bbox=metadata["bbox"]))

            if shapes:
                rois.append(ROI(shapes=shapes, metadata=metadata))

        logger.info(f"Extracted {len(rois)} ROIs from labeled mask")
        return rois


class NonSpatialLabeledMaskROIExtractor(LabeledMaskROIExtractor):
    """Treat scalar and otherwise non-spatial label payloads as empty ROI sets."""

    def accepts(self, labeled_mask: np.ndarray) -> bool:
        return labeled_mask.ndim < 2

    def extract(self, request: LabeledMaskROIExtractionRequest) -> List[ROI]:
        return []


class StackedLabeledMaskROIExtractor(LabeledMaskROIExtractor):
    """Extract ROIs from all 2D planes in a labeled-mask stack."""

    def accepts(self, labeled_mask: np.ndarray) -> bool:
        return labeled_mask.ndim > 2

    def extract(self, request: LabeledMaskROIExtractionRequest) -> List[ROI]:
        stack = request.labeled_mask
        leading_shape = stack.shape[:-2]
        rois: list[ROI] = []
        for plane_indices in np.ndindex(leading_shape):
            plane_request = LabeledMaskROIExtractionRequest(
                labeled_mask=stack[plane_indices],
                min_area=request.min_area,
                extract_contours=request.extract_contours,
                spatial_origin_yx=request.spatial_origin_yx,
                source_spatial_shape_yx=request.source_spatial_shape_yx,
            )
            for roi in TwoDimensionalLabeledMaskROIExtractor().extract(plane_request):
                rois.append(self._with_plane_metadata(roi, plane_indices, leading_shape))
        return rois

    @staticmethod
    def _with_plane_metadata(
        roi: ROI,
        plane_indices: tuple[int, ...],
        leading_shape: tuple[int, ...],
    ) -> ROI:
        return ROI(
            shapes=roi.shapes,
            metadata={
                **roi.metadata,
                "plane_indices": tuple(int(index) for index in plane_indices),
                "plane_shape": tuple(int(size) for size in leading_shape),
            },
        )


class ROIJsonShapeDecoder(ShapeTypeRegistryBase, ABC, metaclass=AutoRegisterMeta):
    """Decode one serialized ROI shape variant."""

    @classmethod
    def for_serialized_shape(cls, shape_dict: Dict[str, Any]) -> "ROIJsonShapeDecoder":
        record = SerializedROIShapeRecord(shape_dict)
        shape_type = record.shape_type()
        try:
            shape_key = ShapeType(shape_type)
        except ValueError:
            raise ValueError(f"Unknown ROI shape type: {shape_type!r}") from None
        return cls.__registry__[shape_key]()

    @abstractmethod
    def decode(self, shape_dict: Dict[str, Any]) -> Any:
        """Return the concrete ROI shape represented by ``shape_dict``."""


@dataclass(frozen=True, slots=True)
class SerializedROIShapeRecord:
    """Typed access to one serialized ROI shape record."""

    payload: Dict[str, Any]

    def shape_type(self) -> str:
        value = self.required("type")
        if not isinstance(value, str):
            raise TypeError("Serialized ROI shape 'type' must be a string.")
        return value

    def coordinates(self) -> np.ndarray:
        return np.array(self.required("coordinates"))

    def mask(self) -> np.ndarray:
        return np.array(self.required("mask"), dtype=bool)

    def bbox(self) -> Tuple[int, int, int, int]:
        return tuple(self.required("bbox"))

    def point_yx(self) -> Tuple[float, float]:
        return (self.numeric("y"), self.numeric("x"))

    def ellipse(self) -> "SerializedEllipseShape":
        return SerializedEllipseShape(
            center_y=self.numeric("center_y"),
            center_x=self.numeric("center_x"),
            radius_y=self.numeric("radius_y"),
            radius_x=self.numeric("radius_x"),
        )

    def numeric(self, key: str) -> float:
        value = self.required(key)
        if not isinstance(value, (int, float)):
            raise TypeError(f"Serialized ROI shape field {key!r} must be numeric.")
        return float(value)

    def required(self, key: str) -> Any:
        if key not in self.payload:
            raise ValueError(f"Serialized ROI shape missing required field {key!r}.")
        return self.payload[key]


@dataclass(frozen=True, slots=True)
class SerializedEllipseShape:
    """Nominal serialized ellipse shape fields."""

    center_y: float
    center_x: float
    radius_y: float
    radius_x: float

    def to_shape(self) -> EllipseShape:
        return EllipseShape(
            center_y=self.center_y,
            center_x=self.center_x,
            radius_y=self.radius_y,
            radius_x=self.radius_x,
        )


@dataclass(frozen=True, slots=True)
class SerializedROIRecord:
    """Typed access to one serialized ROI record."""

    payload: Dict[str, Any]

    def metadata(self) -> Dict[str, Any]:
        value = self.required("metadata")
        if not isinstance(value, dict):
            raise TypeError("Serialized ROI 'metadata' must be a mapping.")
        return value

    def shapes(self) -> Tuple[Dict[str, Any], ...]:
        value = self.required("shapes")
        if not isinstance(value, list):
            raise TypeError("Serialized ROI 'shapes' must be a list.")
        return tuple(value)

    def required(self, key: str) -> Any:
        if key not in self.payload:
            raise ValueError(f"Serialized ROI missing required field {key!r}.")
        return self.payload[key]


class PolygonROIJsonShapeDecoder(ROIJsonShapeDecoder):
    shape_type = ShapeType.POLYGON

    def decode(self, shape_dict: Dict[str, Any]) -> PolygonShape:
        return PolygonShape(
            coordinates=SerializedROIShapeRecord(shape_dict).coordinates()
        )


class PolylineROIJsonShapeDecoder(ROIJsonShapeDecoder):
    shape_type = ShapeType.POLYLINE

    def decode(self, shape_dict: Dict[str, Any]) -> PolylineShape:
        return PolylineShape(
            coordinates=SerializedROIShapeRecord(shape_dict).coordinates()
        )


class MaskROIJsonShapeDecoder(ROIJsonShapeDecoder):
    shape_type = ShapeType.MASK

    def decode(self, shape_dict: Dict[str, Any]) -> MaskShape:
        record = SerializedROIShapeRecord(shape_dict)
        return MaskShape(
            mask=record.mask(),
            bbox=record.bbox(),
        )


class PointROIJsonShapeDecoder(ROIJsonShapeDecoder):
    shape_type = ShapeType.POINT

    def decode(self, shape_dict: Dict[str, Any]) -> PointShape:
        y, x = SerializedROIShapeRecord(shape_dict).point_yx()
        return PointShape(y=y, x=x)


class EllipseROIJsonShapeDecoder(ROIJsonShapeDecoder):
    shape_type = ShapeType.ELLIPSE

    def decode(self, shape_dict: Dict[str, Any]) -> EllipseShape:
        return SerializedROIShapeRecord(shape_dict).ellipse().to_shape()


def extract_rois_from_labeled_mask(
    labeled_mask: np.ndarray,
    min_area: int = 10,
    extract_contours: bool = True,
    spatial_origin_yx: Optional[Tuple[int, int]] = None,
    source_spatial_shape_yx: Optional[Tuple[int, int]] = None,
) -> List[ROI]:
    """Extract ROIs from a labeled segmentation mask."""
    request = LabeledMaskROIExtractionRequest(
        labeled_mask=np.asarray(labeled_mask),
        min_area=min_area,
        extract_contours=extract_contours,
        spatial_origin_yx=spatial_origin_yx,
        source_spatial_shape_yx=source_spatial_shape_yx,
    )
    return LabeledMaskROIExtractor.for_request(request).extract(request)


def _get_backend_from_filemanager(filemanager: Any, backend: Union[str, Backend]):
    backend_name = backend.value if isinstance(backend, Backend) else str(backend)
    return filemanager._get_backend(backend_name)


def materialize_rois(
    rois: List[ROI],
    output_path: str,
    filemanager: Any,
    backend: Union[str, Backend],
    images_dir: str | None = None,
) -> str:
    """Materialize ROIs to backend-specific format."""
    backend_obj = _get_backend_from_filemanager(filemanager, backend)
    return backend_obj._save_rois(rois, Path(output_path), images_dir=images_dir)


def load_rois_from_json(json_path: Path) -> List[ROI]:
    """Load ROIs from JSON file."""
    import json

    if not json_path.exists():
        raise FileNotFoundError(f"ROI JSON file not found: {json_path}")

    with open(json_path, "r") as f:
        rois_data = json.load(f)

    if not isinstance(rois_data, list):
        raise ValueError(f"Invalid ROI JSON format: expected list, got {type(rois_data)}")

    rois = []
    for roi_dict in rois_data:
        record = SerializedROIRecord(roi_dict)
        metadata = record.metadata()
        shapes = []
        for shape_dict in record.shapes():
            decoder = ROIJsonShapeDecoder.for_serialized_shape(shape_dict)
            shapes.append(decoder.decode(shape_dict))

        if shapes:
            rois.append(ROI(shapes=shapes, metadata=metadata))

    logger.info(f"Loaded {len(rois)} ROIs from {json_path}")
    return rois


def load_rois_from_zip(zip_path: Path) -> List[ROI]:
    """Load ROIs from .roi.zip archive (ImageJ standard format)."""
    import zipfile

    if not zip_path.exists():
        raise FileNotFoundError(f"ROI zip file not found: {zip_path}")

    try:
        from roifile import ImagejRoi, ROI_TYPE
    except ImportError:
        raise ImportError("roifile library required for loading .roi.zip files. Install with: pip install roifile")

    rois = []
    with zipfile.ZipFile(zip_path, "r") as zf:
        metadata_by_filename = load_roi_zip_metadata(zf)
        for filename in zf.namelist():
            if not filename.endswith(".roi"):
                continue
            roi_bytes = zf.read(filename)
            ij_roi = ImagejRoi.frombytes(roi_bytes)
            coords = ij_roi.coordinates()
            if coords is None or len(coords) == 0:
                raise ValueError(f"ImageJ ROI member {filename!r} has no coordinates.")
            coords_yx = coords[:, [1, 0]]
            if ij_roi.roitype == ROI_TYPE.POLYLINE:
                shape = PolylineShape(coordinates=coords_yx)
            else:
                shape = PolygonShape(coordinates=coords_yx)
            if filename not in metadata_by_filename:
                raise ValueError(
                    f"ROI archive {zip_path} missing metadata sidecar entry for {filename!r}."
                )
            metadata = dict(metadata_by_filename[filename])
            rois.append(ROI(shapes=[shape], metadata=metadata))

    if not rois:
        raise ValueError(f"No valid ROIs found in {zip_path}")

    logger.info(f"Loaded {len(rois)} ROIs from {zip_path}")
    return rois

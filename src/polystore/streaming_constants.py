"""
Streaming-related enums for polystore.

Provides type-safe enums for data types and shape types used in streaming
backends and viewer integrations.
"""

from enum import Enum


class StreamingDataType(Enum):
    """Types of data that can be streamed to viewers."""
    IMAGE = "image"
    SHAPES = "shapes"  # Napari shapes layer
    POINTS = "points"  # Napari points layer (e.g., skeleton tracings)
    ROIS = "rois"      # Fiji ROI payloads

    @property
    def uses_napari_vector_payload(self) -> bool:
        """Whether napari should receive this type through vector layer payloads."""
        return self in (type(self).SHAPES, type(self).POINTS)

    @property
    def napari_layer_suffix(self) -> str:
        """Layer-key suffix contributed by this data type."""
        return {
            type(self).IMAGE: "",
            type(self).SHAPES: "_shapes",
            type(self).POINTS: "_points",
            type(self).ROIS: "",
        }[self]


class NapariShapeType(Enum):
    """Napari shape types for ROI visualization."""
    POLYGON = "polygon"
    ELLIPSE = "ellipse"
    POINT = "point"
    LINE = "line"
    PATH = "path"
    RECTANGLE = "rectangle"

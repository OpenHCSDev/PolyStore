"""Native Napari inspection adapters, isolated from optional-dependency-free core."""

from dataclasses import dataclass

from napari.layers import Layer
from zmqruntime.viewer_protocol import ViewerNativeLayerTransform
from polystore.streaming.receivers.core.layer_inspection import NativeLayerTransformInspectionABC


@dataclass(frozen=True, slots=True)
class NapariNativeLayerTransformInspection(NativeLayerTransformInspectionABC):
    """Public native Napari Layer properties are the transform authority."""

    layer: Layer

    def snapshot(self) -> ViewerNativeLayerTransform:
        return ViewerNativeLayerTransform(
            scale=tuple(self.layer.scale),
            translate=tuple(self.layer.translate),
        )

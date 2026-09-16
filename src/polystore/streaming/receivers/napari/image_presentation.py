"""Concrete Napari image presentation, with receiver-scoped optional imports."""

from dataclasses import dataclass

from napari.layers import Image, Layer
from zmqruntime.viewer_protocol import ViewerNativeImageIntensityPresentation

from polystore.streaming.receivers.core.image_presentation import NativeImageIntensityPresentationABC


@dataclass(frozen=True, slots=True)
class NapariNativeImageIntensityPresentation(NativeImageIntensityPresentationABC):
    """Napari Image public properties are the only presentation authority."""

    layer: Image

    def __post_init__(self) -> None:
        if not isinstance(self.layer, Image):
            raise TypeError("Native image intensity presentation requires a Napari Image.")

    @classmethod
    def for_layer(cls, layer: Layer | None) -> NativeImageIntensityPresentationABC | None:
        return cls(layer) if isinstance(layer, Image) else None

    def snapshot(self) -> ViewerNativeImageIntensityPresentation:
        return ViewerNativeImageIntensityPresentation(
            contrast_limits=tuple(self.layer.contrast_limits),
            gamma=self.layer.gamma,
        )

    def apply(self, presentation: ViewerNativeImageIntensityPresentation) -> None:
        if not isinstance(presentation, ViewerNativeImageIntensityPresentation):
            raise TypeError("Native image presentation requires its nominal value contract.")
        self.layer.contrast_limits = presentation.contrast_limits
        self.layer.gamma = presentation.gamma

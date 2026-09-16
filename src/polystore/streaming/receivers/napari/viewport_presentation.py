"""Native Napari 2D camera presentation, isolated from generic imports."""

from dataclasses import dataclass

from napari.components import ViewerModel
from zmqruntime.viewer_protocol import ViewerNativeViewportPresentation

from polystore.streaming.receivers.core.viewport_presentation import (
    NativeViewportPresentationABC,
)


@dataclass(frozen=True, slots=True)
class NapariNativeViewportPresentation(NativeViewportPresentationABC):
    """The viewer's public Camera is the sole presentation authority."""

    viewer: ViewerModel

    @classmethod
    def for_viewer(cls, viewer: ViewerModel | None) -> NativeViewportPresentationABC | None:
        if isinstance(viewer, ViewerModel) and viewer.dims.ndisplay == 2:
            return cls(viewer)
        return None

    def _require_2d(self) -> None:
        if self.viewer.dims.ndisplay != 2:
            raise ValueError("Native viewport presentation currently requires 2D viewing.")

    def snapshot(self) -> ViewerNativeViewportPresentation:
        self._require_2d()
        return ViewerNativeViewportPresentation(
            tuple(self.viewer.camera.center), self.viewer.camera.zoom
        )

    def apply(self, presentation: ViewerNativeViewportPresentation) -> None:
        self._require_2d()
        if not isinstance(presentation, ViewerNativeViewportPresentation):
            raise TypeError("Native viewport presentation requires its typed value.")
        self.viewer.camera.center = presentation.center
        self.viewer.camera.zoom = presentation.zoom

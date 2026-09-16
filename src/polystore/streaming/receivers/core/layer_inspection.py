"""Optional-dependency-free native viewer inspection contracts."""

from abc import ABC, abstractmethod

from zmqruntime.viewer_protocol import ViewerNativeLayerTransform


class NativeLayerTransformInspectionABC(ABC):
    """Nominal receiver capability for inspecting the mounted native transform."""

    @abstractmethod
    def snapshot(self) -> ViewerNativeLayerTransform:
        """Read actual native coordinates without mutating layer or camera state."""

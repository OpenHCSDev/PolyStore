"""Optional-dependency-free native image presentation capability."""

from abc import ABC, abstractmethod

from zmqruntime.viewer_protocol import ViewerNativeImageIntensityPresentation


class NativeImageIntensityPresentationABC(ABC):
    """Inspect and apply a complete presentation through native properties."""

    @abstractmethod
    def snapshot(self) -> ViewerNativeImageIntensityPresentation: ...

    @abstractmethod
    def apply(self, presentation: ViewerNativeImageIntensityPresentation) -> None: ...

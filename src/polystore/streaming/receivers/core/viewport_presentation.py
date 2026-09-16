"""Optional-dependency-free native viewport presentation capability."""

from abc import ABC, abstractmethod

from zmqruntime.viewer_protocol import ViewerNativeViewportPresentation


class NativeViewportPresentationABC(ABC):
    """Inspect and apply native camera properties without retaining a mirror."""

    @abstractmethod
    def snapshot(self) -> ViewerNativeViewportPresentation: ...

    @abstractmethod
    def apply(self, presentation: ViewerNativeViewportPresentation) -> None: ...

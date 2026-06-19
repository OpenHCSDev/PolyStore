"""Nominal transport helpers for blocking viewer stream backends."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import (
    ClassVar,
    TypeAlias,
)

from polystore.registry import AutoRegisterMeta
from polystore.streaming.identity import StreamProducerIdentity
from zmqruntime.config import ZMQConfig
from zmqruntime.viewer_protocol import (
    ViewerAckPolicy,
    ViewerTransportEndpoint,
    ViewerTransportMode,
    ViewerWireMapping,
    ViewerWireValue,
)


DisplayComponentToken: TypeAlias = str | Enum
DisplayModeToken: TypeAlias = str | Enum | None
ViewerComponentMetadataByPath: TypeAlias = (
    Mapping[str, ViewerWireMapping]
    | Sequence[ViewerWireMapping]
    | None
)


class ViewerDisplayConfigABC(ABC):
    """Display-config surface required by viewer streaming backends."""

    COMPONENT_ORDER: Sequence[DisplayComponentToken]

    @abstractmethod
    def component_modes(self) -> Mapping[DisplayComponentToken, DisplayModeToken]:
        """Return mode assignments by display component."""


class ViewerFilenameParserABC(ABC):
    """Filename parser surface needed by viewer streaming metadata."""

    @abstractmethod
    def parse_filename(self, filename: str) -> ViewerWireMapping | None:
        """Return component metadata parsed from a filename."""


class ViewerMetadataHandlerABC(ABC):
    """Metadata-handler surface needed by viewer component labels."""

    @abstractmethod
    def get_component_values(
        self,
        plate_path: str | Path | None,
        component_name: str,
    ) -> ViewerWireValue:
        """Return display-name metadata for one component."""


class ViewerMicroscopeHandlerABC(ABC):
    """Microscope-handler surface used by viewer streaming."""

    parser: ViewerFilenameParserABC
    metadata_handler: ViewerMetadataHandlerABC


class ViewerTransportConfigSelection(ABC, metaclass=AutoRegisterMeta):
    """Nominal selection of the transport config used for one stream request."""

    __registry_key__ = "registry_key"
    registry_key: ClassVar[str | None] = None

    @classmethod
    def select(cls, value) -> "ViewerTransportConfigSelection":
        if isinstance(value, cls):
            return value
        for selection_type in cls.__registry__.values():
            if selection_type.accepts(value):
                return selection_type.from_raw(value)
        raise TypeError(
            "transport_config must be a ZMQConfig, "
            "ViewerTransportConfigSelection, or None."
        )

    @classmethod
    @abstractmethod
    def accepts(cls, value) -> bool:
        """Return whether this registered selection can adapt the raw value."""

    @classmethod
    @abstractmethod
    def from_raw(cls, value) -> "ViewerTransportConfigSelection":
        """Adapt the raw value into a concrete transport-config selection."""

    @abstractmethod
    def resolve(self, default_transport_config: ZMQConfig) -> ZMQConfig:
        """Return the concrete config for this request."""


@dataclass(frozen=True)
class DefaultViewerTransportConfig(ViewerTransportConfigSelection):
    """Use the backend's configured transport settings."""

    registry_key: ClassVar[str] = "default"

    @classmethod
    def accepts(cls, value) -> bool:
        return value is None

    @classmethod
    def from_raw(cls, value) -> "DefaultViewerTransportConfig":
        return cls()

    def resolve(self, default_transport_config: ZMQConfig) -> ZMQConfig:
        return default_transport_config


@dataclass(frozen=True)
class ExplicitViewerTransportConfig(ViewerTransportConfigSelection):
    """Use a caller-supplied transport config for this request."""

    registry_key: ClassVar[str] = "explicit"

    config: ZMQConfig

    @classmethod
    def accepts(cls, value) -> bool:
        return isinstance(value, ZMQConfig)

    @classmethod
    def from_raw(cls, value) -> "ExplicitViewerTransportConfig":
        return cls(value)

    def resolve(self, default_transport_config: ZMQConfig) -> ZMQConfig:
        return self.config


@dataclass(frozen=True)
class ViewerTransportDefaults:
    """Declared transport defaults shared by viewer streaming backends."""

    ack_timeout_ms: int = 30_000

    def ack_policy(self, viewer_name: str) -> ViewerAckPolicy:
        return ViewerAckPolicy(
            viewer_name=viewer_name,
            timeout_ms=self.ack_timeout_ms,
        )


@dataclass(frozen=True)
class ViewerStreamSourceMetadata:
    """Component metadata authority for streamed source items."""

    component_metadata: ViewerWireMapping | None = None
    component_metadata_by_path: ViewerComponentMetadataByPath = None

    def __post_init__(self) -> None:
        if (
            self.component_metadata is not None
            and self.component_metadata_by_path is not None
        ):
            raise ValueError(
                "Viewer stream source context accepts either component_metadata "
                "or component_metadata_by_path, not both."
            )

    def component_metadata_for_item(
        self,
        file_path: str | Path,
        index: int,
    ) -> dict[str, ViewerWireValue]:
        """Return explicit component metadata for one batch item."""
        if self.component_metadata_by_path is None:
            return self._batch_component_metadata(file_path)

        if isinstance(self.component_metadata_by_path, Mapping):
            return self._mapping_component_metadata(
                file_path,
                self.component_metadata_by_path,
            )

        if index < len(self.component_metadata_by_path):
            return self.component_metadata_by_path[index]

        raise IndexError(
            "Viewer stream component_metadata_by_path has no entry for "
            f"item {index} at {file_path!r}."
        )

    def _batch_component_metadata(
        self,
        file_path: str | Path,
    ) -> dict[str, ViewerWireValue]:
        if self.component_metadata is None:
            raise ValueError(
                "Viewer stream item requires explicit component_metadata or "
                f"component_metadata_by_path; got no metadata for {file_path!r}."
            )
        return self._metadata_payload(
            self.component_metadata,
            f"batch metadata for {file_path!r}",
        )

    def _mapping_component_metadata(
        self,
        file_path: str | Path,
        metadata_by_path: Mapping[str, ViewerWireMapping],
    ) -> dict[str, ViewerWireValue]:
        path = Path(file_path)
        for key in (str(file_path), path.as_posix(), path.name):
            if key in metadata_by_path:
                return self._metadata_payload(
                    metadata_by_path[key],
                    f"path metadata for {file_path!r}",
                )
        raise KeyError(
            "Viewer stream component_metadata_by_path has no entry for "
            f"{file_path!r}."
        )

    @staticmethod
    def _metadata_payload(
        value: ViewerWireMapping,
        source_label: str,
    ) -> dict[str, ViewerWireValue]:
        if not isinstance(value, Mapping):
            raise TypeError(
                "Viewer stream component metadata must be a mapping "
                f"for {source_label}; got {type(value).__name__}."
            )
        return dict(value)


@dataclass(frozen=True)
class ViewerStreamSourceIdentity:
    """Stable source identity shared by all stream batches for one plate."""

    microscope_handler: ViewerMicroscopeHandlerABC
    plate_path: str | Path | None = None


class ViewerStreamKwarg(str, Enum):
    """Raw kwarg names accepted at the top-level viewer stream boundary."""

    STREAM_REQUEST = "stream_request"


@dataclass(frozen=True)
class ViewerStreamSource:
    """Source provenance and metadata authority for one viewer stream."""

    identity: ViewerStreamSourceIdentity
    metadata: ViewerStreamSourceMetadata = field(
        default_factory=ViewerStreamSourceMetadata
    )


@dataclass(frozen=True)
class ViewerStreamRequest:
    """Typed view of backend kwargs at the viewer streaming boundary."""

    viewer_transport: ViewerTransportEndpoint
    display_config: ViewerDisplayConfigABC
    source: ViewerStreamSource
    producer_identity: StreamProducerIdentity
    transport_config: ViewerTransportConfigSelection = DefaultViewerTransportConfig()
    message_extra: ViewerWireMapping | None = None
    images_dir: str | None = None

    @property
    def host(self) -> str:
        return self.viewer_transport.host

    @property
    def port(self) -> int:
        return self.viewer_transport.port

    @property
    def transport_mode(self) -> ViewerTransportMode:
        return self.viewer_transport.transport_mode

    def message_extra_payload(self) -> dict[str, ViewerWireValue]:
        return ViewerMessageExtraAuthority.payload(self.message_extra)


ViewerStreamKwargPayloadMapping: TypeAlias = Mapping[
    str,
    "ViewerStreamRequest",
]


@dataclass(frozen=True)
class ViewerStreamBackendKwargs:
    """The only accepted FileManager kwarg payload for viewer stream backends."""

    stream_request: ViewerStreamRequest

    @classmethod
    def from_kwargs(
        cls,
        kwargs: ViewerStreamKwargPayloadMapping,
    ) -> "ViewerStreamBackendKwargs":
        expected = frozenset((ViewerStreamKwarg.STREAM_REQUEST.value,))
        actual = frozenset(kwargs)
        if actual != expected:
            raise ValueError(
                "Viewer stream backends require exactly one kwarg: stream_request"
            )
        value = kwargs[ViewerStreamKwarg.STREAM_REQUEST.value]
        if not isinstance(value, ViewerStreamRequest):
            raise TypeError("stream_request must be a ViewerStreamRequest instance")
        return cls(value)

    def to_kwargs(self) -> dict[str, ViewerStreamRequest]:
        return {ViewerStreamKwarg.STREAM_REQUEST.value: self.stream_request}


class ViewerMessageExtraAuthority:
    """Formal boundary for absent caller-supplied viewer message extras."""

    @staticmethod
    def payload(message_extra: Mapping[str, ViewerWireValue] | None) -> dict[str, ViewerWireValue]:
        if message_extra is None:
            return {}
        return dict(message_extra)

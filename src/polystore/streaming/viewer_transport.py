"""Nominal transport helpers for blocking viewer stream backends."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from enum import Enum
from pathlib import Path
from typing import (
    ClassVar,
    TypeAlias,
)

from polystore.registry import AutoRegisterMeta
from polystore.streaming_constants import StreamingDataType
from polystore.streaming.identity import StreamProducerIdentity
from zmqruntime.config import ZMQConfig
from zmqruntime.viewer_protocol import (
    ViewerAckPolicy,
    ViewerBatchDisplayPayload,
    ViewerBatchContextWireField,
    ViewerBatchItemPayload,
    ViewerTransportEndpoint,
    ViewerTransportMode,
    ViewerWireMapping,
    ViewerWireValue,
)


DisplayComponentToken: TypeAlias = str | Enum
DisplayModeToken: TypeAlias = str | Enum | None
ViewerIndexedComponentMetadata: TypeAlias = Sequence[ViewerWireMapping]
ViewerPathComponentMetadata: TypeAlias = Mapping[str, ViewerWireMapping]


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


class ViewerSourceComponentMetadataPayload(dict[str, ViewerWireValue]):
    """Validated component metadata payload for one streamed source item."""

    @classmethod
    def from_mapping(
        cls,
        value: ViewerWireMapping,
        *,
        source_label: str,
    ) -> "ViewerSourceComponentMetadataPayload":
        if not isinstance(value, Mapping):
            raise TypeError(
                "Viewer stream component metadata must be a mapping "
                f"for {source_label}; got {type(value).__name__}."
            )
        return cls(dict(value))


class ViewerStreamSourceMetadata(ABC, metaclass=AutoRegisterMeta):
    """Component metadata authority for streamed source items."""

    __registry_key__ = "metadata_kind"
    __skip_if_no_key__ = True
    metadata_kind: ClassVar[str | None] = None

    @abstractmethod
    def component_metadata_for_item(
        self,
        file_path: str | Path,
        index: int,
    ) -> ViewerSourceComponentMetadataPayload:
        """Return explicit component metadata for one batch item."""


@dataclass(frozen=True)
class BatchViewerStreamSourceMetadata(ViewerStreamSourceMetadata):
    """One component metadata payload shared by every streamed item."""

    metadata_kind: ClassVar[str] = "batch"
    component_metadata: ViewerWireMapping

    def component_metadata_for_item(
        self,
        file_path: str | Path,
        index: int,
    ) -> ViewerSourceComponentMetadataPayload:
        return ViewerSourceComponentMetadataPayload.from_mapping(
            self.component_metadata,
            source_label=f"batch metadata for {file_path!r}",
        )


@dataclass(frozen=True)
class PathMappedViewerStreamSourceMetadata(ViewerStreamSourceMetadata):
    """Component metadata selected by stream item path identity."""

    metadata_kind: ClassVar[str] = "path_mapped"
    metadata_by_path: ViewerPathComponentMetadata

    def component_metadata_for_item(
        self,
        file_path: str | Path,
        index: int,
    ) -> ViewerSourceComponentMetadataPayload:
        path = Path(file_path)
        for key in (str(file_path), path.as_posix(), path.name):
            if key in self.metadata_by_path:
                return ViewerSourceComponentMetadataPayload.from_mapping(
                    self.metadata_by_path[key],
                    source_label=f"path metadata for {file_path!r}",
                )
        raise KeyError(
            "Viewer stream path-mapped component metadata has no entry for "
            f"{file_path!r}."
        )


@dataclass(frozen=True)
class IndexedViewerStreamSourceMetadata(ViewerStreamSourceMetadata):
    """Component metadata selected by stream item batch position."""

    metadata_kind: ClassVar[str] = "indexed"
    metadata_by_index: ViewerIndexedComponentMetadata

    def component_metadata_for_item(
        self,
        file_path: str | Path,
        index: int,
    ) -> ViewerSourceComponentMetadataPayload:
        if index >= len(self.metadata_by_index):
            raise IndexError(
                "Viewer stream indexed component metadata has no entry for "
                f"item {index} at {file_path!r}."
            )
        return ViewerSourceComponentMetadataPayload.from_mapping(
            self.metadata_by_index[index],
            source_label=f"indexed metadata for {file_path!r}",
        )


@dataclass(frozen=True)
class ViewerStreamProducer:
    """Producer identity carrier that owns viewer item identity projection."""

    identity: StreamProducerIdentity

    @classmethod
    def from_identity(
        cls,
        identity: StreamProducerIdentity,
    ) -> "ViewerStreamProducer":
        return cls(identity=identity)

    def batch_item_payload(
        self,
        item_source: "ViewerStreamBatchItemSource",
    ) -> ViewerBatchItemPayload:
        return ViewerBatchItemPayload.from_parts(
            item_payload=item_source.item_payload,
            data_type=item_source.wire_data_type,
            metadata=item_source.metadata,
            producer_identity=self.identity.to_payload(),
            image_id=item_source.image_id,
        )


@dataclass(frozen=True)
class ViewerStreamItemPayload:
    """Typed item payload produced by a concrete viewer streaming backend."""

    item_payload: ViewerWireMapping
    streaming_data_type: StreamingDataType

    @property
    def wire_data_type(self) -> str:
        return self.streaming_data_type.value


@dataclass(frozen=True)
class ViewerStreamBatchItemInput(ViewerStreamItemPayload):
    """Nominal input for constructing one viewer batch item source."""

    stream_source: "ViewerStreamSource"
    file_path: str | Path
    index: int
    image_id: str


@dataclass(frozen=True)
class ViewerStreamBatchItemSource(ViewerStreamItemPayload):
    """Declared source payload for one viewer batch item."""

    metadata: ViewerSourceComponentMetadataPayload
    image_id: str

    @classmethod
    def from_input(
        cls,
        source_input: ViewerStreamBatchItemInput,
    ) -> "ViewerStreamBatchItemSource":
        return cls(
            item_payload=source_input.item_payload,
            streaming_data_type=source_input.streaming_data_type,
            metadata=source_input.stream_source.metadata.component_metadata_for_item(
                source_input.file_path,
                source_input.index,
            ),
            image_id=source_input.image_id,
        )


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
    metadata: ViewerStreamSourceMetadata


@dataclass(frozen=True)
class ViewerStreamDisplaySemantics:
    """Normalized display-axis semantics for a viewer stream request."""

    display_config: ViewerDisplayConfigABC

    @property
    def component_order(self) -> tuple[str, ...]:
        return tuple(str(component) for component in self.display_config.COMPONENT_ORDER)

    @property
    def component_modes(self) -> dict[str, str]:
        return {
            str(component): str(mode.value if isinstance(mode, Enum) else mode)
            for component, mode in self.display_config.component_modes().items()
        }

    def batch_display_payload(
        self,
        extra: Mapping[str | Enum, ViewerWireValue] | None = None,
    ) -> ViewerBatchDisplayPayload:
        if extra is None:
            extra_payload: dict[str | Enum, ViewerWireValue] = {}
        else:
            extra_payload = dict(extra)
        return ViewerBatchDisplayPayload(
            component_modes=self.component_modes,
            component_order=self.component_order,
            extra=extra_payload,
        )


@dataclass(frozen=True, kw_only=True, slots=True)
class ViewerStreamMessageContext:
    """Viewer message context carried through stream request boundaries."""

    message_extra: ViewerWireMapping | None = None
    images_dir: str | None = None

    def message_extra_payload(self) -> dict[str, ViewerWireValue]:
        return ViewerMessageExtraAuthority.payload(self.message_extra)

    def message_extra_payload_with_images_dir(self) -> dict[str, ViewerWireValue]:
        payload = self.message_extra_payload()
        payload[ViewerBatchContextWireField.IMAGES_DIR.value] = self.images_dir
        return payload


@dataclass(frozen=True, kw_only=True)
class ViewerStreamRequest(ViewerStreamMessageContext):
    """Typed view of backend kwargs at the viewer streaming boundary."""

    viewer_transport: ViewerTransportEndpoint
    display_config: ViewerDisplayConfigABC
    source: ViewerStreamSource
    producer: ViewerStreamProducer
    transport_config: ViewerTransportConfigSelection = DefaultViewerTransportConfig()

    @classmethod
    def from_message_context(
        cls,
        *,
        message_context: ViewerStreamMessageContext,
        viewer_transport: ViewerTransportEndpoint,
        display_config: ViewerDisplayConfigABC,
        source: ViewerStreamSource,
        producer: ViewerStreamProducer,
        transport_config: ViewerTransportConfigSelection = DefaultViewerTransportConfig(),
    ) -> "ViewerStreamRequest":
        return cls(
            viewer_transport=viewer_transport,
            display_config=display_config,
            source=source,
            producer=producer,
            transport_config=transport_config,
            message_extra=message_context.message_extra,
            images_dir=message_context.images_dir,
        )

    @property
    def host(self) -> str:
        return self.viewer_transport.host

    @property
    def port(self) -> int:
        return self.viewer_transport.port

    @property
    def transport_mode(self) -> ViewerTransportMode:
        return self.viewer_transport.transport_mode

    @property
    def display_semantics(self) -> ViewerStreamDisplaySemantics:
        return ViewerStreamDisplaySemantics(self.display_config)


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

    def with_single_item_component_metadata(
        self,
        component_metadata: ViewerWireMapping,
    ) -> "ViewerStreamBackendKwargs":
        """Return kwargs with component metadata for a single streamed item."""
        stream_request = self.stream_request
        source = replace(
            stream_request.source,
            metadata=BatchViewerStreamSourceMetadata(
                component_metadata=dict(component_metadata),
            ),
        )
        return type(self)(replace(stream_request, source=source))


class ViewerMessageExtraAuthority:
    """Formal boundary for absent caller-supplied viewer message extras."""

    @staticmethod
    def payload(message_extra: Mapping[str, ViewerWireValue] | None) -> dict[str, ViewerWireValue]:
        if message_extra is None:
            return {}
        return dict(message_extra)

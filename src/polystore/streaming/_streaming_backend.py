"""
Streaming backend interfaces for polystore.

Provides abstract base classes for streaming data destinations that send
data to external systems without persistent storage capabilities.
"""

from __future__ import annotations

import logging
import time
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from multiprocessing import resource_tracker, shared_memory
from multiprocessing.shared_memory import _USE_POSIX
from pathlib import Path
from types import MappingProxyType
from typing import TypeAlias
import numpy as np
import zmq
from arraybridge import convert_memory, detect_memory_type
from arraybridge.types import MemoryType as ArrayBridgeMemoryType

from ..base import DataSink
from ..formats import PIXEL_PAYLOAD_EXTENSIONS
from ..streaming_constants import StreamingDataType
from ..roi import ROI, ROI_ZIP_EXTENSION
from ..roi_converters import ROIShapeNapariPayloadConverter
from ..zmq_config import POLYSTORE_ZMQ_CONFIG
from .viewer_transport import (
    ViewerMicroscopeHandlerABC,
    ViewerStreamBatchItemInput,
    ViewerStreamBatchItemSource,
    ViewerStreamBackendKwargs,
    ViewerStreamItemPayload,
    ViewerStreamRequest,
    ViewerTransportDefaults,
)
from zmqruntime.ack_listener import GlobalAckListener
from zmqruntime.config import ZMQConfig
from zmqruntime.viewer_protocol import (
    ViewerBatchItemWireField,
    ViewerBatchMessagePayload,
    ViewerComponentMetadataPayload,
    ViewerDisplayConfigWireField,
    ViewerTransportEndpoint,
    ViewerWirePayload,
    ViewerWireMapping,
    ViewerWireValue,
)

logger = logging.getLogger(__name__)


FilePath: TypeAlias = str | Path
RoiStreamPayload: TypeAlias = Sequence[ROI]
StreamablePayload: TypeAlias = np.ndarray | Sequence[ViewerWireValue] | RoiStreamPayload
ComponentValue = str | int | float | bool | tuple | None
ViewerDisplayPayloadExtraValues: TypeAlias = Mapping[
    str | ViewerDisplayConfigWireField,
    ViewerWireValue,
]
STREAMING_TRANSPORT_DEFAULTS = ViewerTransportDefaults()


@dataclass(frozen=True)
class ViewerDisplayPayloadExtra:
    """Nominal viewer-specific display payload extension."""

    values: ViewerDisplayPayloadExtraValues = field(
        default_factory=lambda: MappingProxyType({})
    )

    @classmethod
    def from_mapping(
        cls,
        values: ViewerDisplayPayloadExtraValues,
    ) -> "ViewerDisplayPayloadExtra":
        return cls(values)

    def to_wire_mapping(self) -> dict[str, ViewerWireValue]:
        return ViewerWirePayload.mapping(
            self.values,
            context="viewer display payload extra",
        )


EMPTY_DISPLAY_PAYLOAD_EXTRA = ViewerDisplayPayloadExtra()


@dataclass(frozen=True)
class StreamingComponentDomainValue:
    """Viewer component value normalized for a batch-level domain."""

    value: ComponentValue

    @classmethod
    def from_wire(
        cls,
        value: ViewerWireValue,
    ) -> "StreamingComponentDomainValue":
        if isinstance(value, (str, int, float, bool)) or value is None:
            return cls(value)
        if isinstance(value, tuple):
            return cls(value)
        if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
            return cls(tuple(value))
        raise TypeError(
            "Streaming component values must be JSON scalar or tuple-like, "
            f"got {type(value).__name__}."
        )


@dataclass(frozen=True)
class StreamingBatchImageMetadata:
    """Validated metadata carried by one prepared viewer batch item."""

    values: ViewerWireMapping

    @classmethod
    def from_image_payload(
        cls,
        image_payload: ViewerWireMapping,
    ) -> "StreamingBatchImageMetadata":
        metadata = image_payload[ViewerBatchItemWireField.METADATA.value]
        if not isinstance(metadata, Mapping):
            raise TypeError(
                "Streaming batch item metadata must be a mapping, "
                f"got {type(metadata).__name__}."
            )
        return cls(
            ViewerWirePayload.mapping(
                metadata,
                context="streaming batch item metadata",
            )
        )

    def component_value(self, component: str) -> ComponentValue | None:
        if component not in self.values:
            return None
        return StreamingComponentDomainValue.from_wire(
            self.values[component]
        ).value


class StreamingComponentValueDomainAuthority:
    """Build batch-level component value domains from stream item metadata."""

    @staticmethod
    def wire_payload(
        stream_request: ViewerStreamRequest,
        batch_images: Sequence[ViewerWireMapping],
    ) -> dict[str, ViewerWireValue]:
        component_order = stream_request.display_semantics.component_order
        values_by_component: dict[str, list[ComponentValue]] = {
            component: [] for component in component_order
        }
        for image_payload in batch_images:
            metadata = StreamingBatchImageMetadata.from_image_payload(image_payload)
            for component in component_order:
                value = metadata.component_value(component)
                if value is None:
                    continue
                if value not in values_by_component[component]:
                    values_by_component[component].append(value)
        return {
            component: values
            for component, values in values_by_component.items()
            if values
        }

@dataclass(frozen=True)
class StreamingComponentNamesRequest:
    """Component-label metadata requested for one viewer batch."""

    component_names: Sequence[str]
    log_prefix: str | None = None
    verbose: bool = False

    @classmethod
    def from_stream_request(
        cls,
        stream_request: ViewerStreamRequest,
        log_prefix: str | None = None,
        verbose: bool = False,
    ) -> "StreamingComponentNamesRequest":
        return cls(
            component_names=stream_request.display_semantics.component_order,
            log_prefix=log_prefix,
            verbose=verbose,
        )


@dataclass(frozen=True)
class StreamingBatchMessageRequest:
    """Inputs for building one viewer batch message."""

    data_list: list[StreamablePayload]
    file_paths: list[FilePath]
    stream_request: ViewerStreamRequest
    component_names_request: StreamingComponentNamesRequest
    display_payload_extra: ViewerDisplayPayloadExtra = field(
        default_factory=ViewerDisplayPayloadExtra
    )


@dataclass(frozen=True)
class StreamingPreparedBatchItems:
    """Prepared per-item viewer payloads before batch-level metadata is attached."""

    batch_images: list[dict[str, ViewerWireValue]]
    image_ids: list[str]


@dataclass(frozen=True)
class StreamingBuiltBatch(StreamingPreparedBatchItems):
    """Prepared viewer message and per-item transmission bookkeeping."""

    message: dict[str, ViewerWireValue]


@dataclass(frozen=True)
class StreamingItemPath:
    """Nominal path identity for one item in a viewer stream batch."""

    value: FilePath

    @property
    def wire_value(self) -> str:
        return str(self.value)


@dataclass(frozen=True)
class StreamingPayloadFileRequest:
    """Shared payload/file identity for viewer item preparation requests."""

    data: StreamablePayload
    item_path: StreamingItemPath


@dataclass(frozen=True)
class StreamingItemPreparationRequest(StreamingPayloadFileRequest):
    """Inputs needed to prepare one payload for a viewer batch item."""

    streaming_data_type: StreamingDataType


@dataclass(frozen=True)
class StreamingSharedMemoryRequest(StreamingPayloadFileRequest):
    """Inputs needed to allocate one image payload into shared memory."""

    shm_prefix: str


@dataclass(frozen=True)
class StreamingSharedMemoryPayload:
    """Wire payload describing a shared-memory image allocation."""

    item_path: StreamingItemPath
    shape: tuple[int, ...]
    dtype: str
    shm_name: str

    def to_wire_mapping(self) -> dict[str, ViewerWireValue]:
        return {
            ViewerBatchItemWireField.PATH.value: self.item_path.wire_value,
            ViewerBatchItemWireField.SHAPE.value: self.shape,
            ViewerBatchItemWireField.DTYPE.value: self.dtype,
            ViewerBatchItemWireField.SHM_NAME.value: self.shm_name,
        }


@dataclass(frozen=True)
class StreamingSharedMemoryBlock:
    """Allocated shared memory and the wire payload that names it."""

    shared_memory: shared_memory.SharedMemory
    payload: StreamingSharedMemoryPayload


@dataclass(frozen=True)
class StreamingSharedMemoryName:
    """Unique shared-memory name for one viewer transfer block."""

    value: str

    @classmethod
    def unique(
        cls,
        shm_prefix: str,
    ) -> "StreamingSharedMemoryName":
        return cls(f"{shm_prefix}{uuid.uuid4().hex[:12]}")


class StreamingPayloadMemoryAuthority:
    """Memory conversion authority for streamable image payloads."""

    @staticmethod
    def to_numpy(data: StreamablePayload) -> np.ndarray:
        if isinstance(data, np.ndarray):
            return data
        if isinstance(data, (list, tuple)):
            return np.asarray(data)
        return convert_memory(
            data,
            detect_memory_type(data),
            ArrayBridgeMemoryType.NUMPY.value,
            gpu_id=0,
        )


class StreamingSharedMemoryAuthority:
    """Own sender allocation and receiver attachment for shared-memory streams."""

    @staticmethod
    def _release_receiver_tracking(memory: shared_memory.SharedMemory) -> None:
        """Keep a non-owning POSIX receiver from unlinking sender-owned memory."""
        if _USE_POSIX:
            resource_tracker.unregister(memory._name, "shared_memory")

    @classmethod
    def copy_sender_owned_array(
        cls,
        *,
        name: str,
        shape: Sequence[int],
        dtype: str | np.dtype,
    ) -> np.ndarray:
        """Copy an attached array without transferring allocation ownership."""
        memory = shared_memory.SharedMemory(name=name)
        try:
            cls._release_receiver_tracking(memory)
            return np.ndarray(shape, dtype=dtype, buffer=memory.buf).copy()
        finally:
            memory.close()

    @classmethod
    def create(
        cls,
        request: StreamingSharedMemoryRequest,
    ) -> StreamingSharedMemoryBlock:
        np_data = StreamingPayloadMemoryAuthority.to_numpy(request.data)
        shm_name = StreamingSharedMemoryName.unique(request.shm_prefix).value
        shm = shared_memory.SharedMemory(
            create=True,
            size=np_data.nbytes,
            name=shm_name,
        )

        shm_array = np.ndarray(np_data.shape, dtype=np_data.dtype, buffer=shm.buf)
        shm_array[:] = np_data[:]

        return StreamingSharedMemoryBlock(
            shared_memory=shm,
            payload=StreamingSharedMemoryPayload(
                item_path=request.item_path,
                shape=tuple(int(dimension) for dimension in np_data.shape),
                dtype=str(np_data.dtype),
                shm_name=shm_name,
            ),
        )


class StreamingDataTypeAuthority:
    """Detect the viewer payload kind for one streamed object."""

    @staticmethod
    def detect(data: StreamablePayload) -> StreamingDataType:
        is_roi = isinstance(data, list) and len(data) > 0 and isinstance(data[0], ROI)

        if not is_roi:
            return StreamingDataType.IMAGE

        return ROIShapeNapariPayloadConverter.streaming_data_type_for_rois(data)


class StreamingComponentNamesMetadataCollector:
    """Collect viewer component-label metadata for one batch."""

    @staticmethod
    def collect(
        plate_path: FilePath | None,
        microscope_handler: ViewerMicroscopeHandlerABC,
        request: StreamingComponentNamesRequest,
    ) -> dict[str, ViewerWireValue]:
        component_names_metadata = {}

        if plate_path is None:
            if request.verbose and request.log_prefix:
                logger.warning("%s: No plate_path in kwargs", request.log_prefix)
            return component_names_metadata

        for component_name in request.component_names:
            metadata = microscope_handler.metadata_handler.get_component_values(
                plate_path,
                component_name,
            )
            if request.verbose and request.log_prefix:
                logger.info(
                    "%s: Got %s metadata: %s",
                    request.log_prefix,
                    component_name,
                    metadata,
                )
            if metadata:
                component_names_metadata[component_name] = metadata

        return component_names_metadata


class StreamingDisplayPayloadBuilder:
    """Build the shared viewer display-config payload."""

    @staticmethod
    def build(
        stream_request: ViewerStreamRequest,
        display_payload_extra: ViewerDisplayPayloadExtra,
    ):
        return stream_request.display_semantics.batch_display_payload(
            display_payload_extra.to_wire_mapping()
        )


class StreamingBatchItemPreparationAuthority:
    """Prepare per-item viewer payloads and transmission bookkeeping."""

    @staticmethod
    def prepare(
        backend: "StreamingBackend",
        request: StreamingBatchMessageRequest,
    ) -> StreamingPreparedBatchItems:
        batch_images = []
        image_ids = []

        for index, (data, file_path) in enumerate(
            zip(request.data_list, request.file_paths)
        ):
            item_path = StreamingItemPath(file_path)
            image_id = str(uuid.uuid4())
            image_ids.append(image_id)

            streaming_data_type = StreamingDataTypeAuthority.detect(data)
            item_payload = backend._prepare_batch_item(
                StreamingItemPreparationRequest(
                    data=data,
                    item_path=item_path,
                    streaming_data_type=streaming_data_type,
                )
            )

            batch_images.append(
                request.stream_request.producer.batch_item_payload(
                    ViewerStreamBatchItemSource.from_input(
                        ViewerStreamBatchItemInput(
                            stream_source=request.stream_request.source,
                            item_payload=item_payload.item_payload,
                            streaming_data_type=item_payload.streaming_data_type,
                            file_path=item_path.value,
                            index=index,
                            image_id=image_id,
                        )
                    )
                ).to_wire_mapping()
            )

        return StreamingPreparedBatchItems(
            batch_images=batch_images,
            image_ids=image_ids,
        )


class StreamingComponentMetadataPayloadAuthority:
    """Resolve the component metadata payload for one viewer batch."""

    @staticmethod
    def payload(
        request: StreamingBatchMessageRequest,
        prepared_items: StreamingPreparedBatchItems,
    ) -> ViewerComponentMetadataPayload:
        declared = ViewerComponentMetadataPayload.from_optional_wire_mapping(
            request.stream_request.message_extra_payload()
        )
        if declared is not None:
            return declared
        return ViewerComponentMetadataPayload(
            component_names_metadata=(
                StreamingComponentNamesMetadataCollector.collect(
                    request.stream_request.source.identity.plate_path,
                    request.stream_request.source.identity.microscope_handler,
                    request.component_names_request,
                )
            ),
            component_value_domain=(
                StreamingComponentValueDomainAuthority.wire_payload(
                    request.stream_request,
                    prepared_items.batch_images,
                )
            ),
        )


class StreamingBatchMessageBuilder:
    """Build complete viewer batch messages from prepared items."""

    @classmethod
    def build(
        cls,
        backend: "StreamingBackend",
        request: StreamingBatchMessageRequest,
    ) -> StreamingBuiltBatch:
        if len(request.data_list) != len(request.file_paths):
            raise ValueError("data_list and file_paths must have the same length")

        prepared_items = StreamingBatchItemPreparationAuthority.prepare(
            backend,
            request,
        )

        component_metadata_payload = (
            StreamingComponentMetadataPayloadAuthority.payload(
                request,
                prepared_items,
            )
        )

        display_payload = StreamingDisplayPayloadBuilder.build(
            request.stream_request,
            request.display_payload_extra,
        )
        message = ViewerBatchMessagePayload.from_parts(
            images=prepared_items.batch_images,
            display_payload=display_payload,
            component_metadata=component_metadata_payload,
            timestamp=time.time(),
            extra=ViewerComponentMetadataPayload.strip_component_metadata(
                backend.message_extra(request.stream_request)
            ),
        ).to_wire_mapping()

        return StreamingBuiltBatch(
            message=message,
            batch_images=prepared_items.batch_images,
            image_ids=prepared_items.image_ids,
        )


class StreamingBackend(DataSink):
    """
    Abstract base class for ZeroMQ-based streaming backends.

    Provides common ZeroMQ publisher management, shared memory handling,
    and component metadata parsing for all streaming backends.

    Subclasses must define abstract class attributes:
    - VIEWER_TYPE: str (e.g., 'napari', 'fiji')
    - SHM_PREFIX: str (e.g., 'napari_', 'fiji_')
    - _backend_type: str (e.g., 'napari_stream', 'fiji_stream')

    All streaming backends use generic 'host' and 'port' kwargs for polymorphism.

    Inherits from DataSink (which inherits from BackendBase for automatic registration).
    """

    # Abstract class attributes that subclasses must define
    VIEWER_TYPE: str
    SHM_PREFIX: str

    # Class attribute: streaming backends only support image array data and ROIs
    supports_arbitrary_files: bool = False

    # Extensions that streaming backends can handle
    # Subclasses can override to add support for specific formats
    SUPPORTED_EXTENSIONS: frozenset[str] = PIXEL_PAYLOAD_EXTENSIONS | {
        ROI_ZIP_EXTENSION
    }

    def supports_file_path(self, path: FilePath) -> bool:
        """Return whether the stream backend can render this output path."""
        name = Path(path).name.lower()
        return any(name.endswith(ext) for ext in self.SUPPORTED_EXTENSIONS)

    @property
    def requires_filesystem_validation(self) -> bool:
        """Streaming backends don't require filesystem validation."""
        return False

    def _filter_streamable_files(
        self,
        data_list: list[StreamablePayload],
        file_paths: list[FilePath],
    ) -> tuple[list[StreamablePayload], list[FilePath], list[FilePath]]:
        """
        Filter data to only include files with supported extensions.

        Args:
            data_list: List of data objects
            file_paths: List of file paths

        Returns:
            Tuple of (filtered_data, filtered_paths, skipped_paths)
        """
        filtered_data = []
        filtered_paths = []
        skipped_paths = []

        for data, path in zip(data_list, file_paths):
            if self.supports_file_path(path):
                filtered_data.append(data)
                filtered_paths.append(path)
            else:
                skipped_paths.append(path)

        if skipped_paths:
            logger.info(
                f"{self.VIEWER_TYPE}: Skipping {len(skipped_paths)} non-streamable files: "
                f"{[str(p) for p in skipped_paths]}"
            )

        return filtered_data, filtered_paths, skipped_paths

    def __init__(self, transport_config: ZMQConfig = POLYSTORE_ZMQ_CONFIG):
        """Initialize ZeroMQ and shared memory infrastructure."""
        self._publishers = {}
        self._context = None
        self._shared_memory_blocks = {}
        self._transport_config = transport_config

    def create_shared_memory_payload(
        self,
        data: StreamablePayload,
        file_path: FilePath,
    ) -> dict[str, ViewerWireValue]:
        block = StreamingSharedMemoryAuthority.create(
            StreamingSharedMemoryRequest(
                data=data,
                item_path=StreamingItemPath(file_path),
                shm_prefix=self.SHM_PREFIX,
            )
        )
        self._shared_memory_blocks[block.payload.shm_name] = block.shared_memory
        return block.payload.to_wire_mapping()

    def _register_with_queue_tracker(
        self,
        transport_endpoint: ViewerTransportEndpoint,
        image_ids: list[str],
        transport_config: ZMQConfig,
    ) -> None:
        """
        Register sent images with queue tracker (common for all streaming backends).

        Args:
            port: Port number for tracker lookup
            image_ids: List of image IDs to register
        """
        listener = GlobalAckListener()
        listener.start(
            port=transport_config.shared_ack_port,
            transport_mode=transport_endpoint.resolved_transport_mode(),
            config=transport_config,
        )

        from zmqruntime.queue_tracker import GlobalQueueTrackerRegistry
        registry = GlobalQueueTrackerRegistry()
        tracker = registry.get_or_create_tracker(
            transport_endpoint.port,
            self.VIEWER_TYPE,
        )
        for image_id in image_ids:
            tracker.register_sent(image_id)

    def _cleanup_shared_memory_blocks(self, batch_images, unlink: bool = False) -> None:
        for img in batch_images:
            shm_name = img.get(ViewerBatchItemWireField.SHM_NAME.value)
            if shm_name and shm_name in self._shared_memory_blocks:
                try:
                    shm = self._shared_memory_blocks.pop(shm_name)
                    shm.close()
                    if unlink:
                        shm.unlink()
                except Exception as e:
                    logger.warning(f"Failed to cleanup shared memory {shm_name}: {e}")

    def _prepare_batch_item(
        self,
        request: StreamingItemPreparationRequest,
    ) -> ViewerStreamItemPayload:
        raise NotImplementedError

    def display_payload_extra(
        self,
        stream_request: ViewerStreamRequest,
    ) -> ViewerDisplayPayloadExtra:
        return EMPTY_DISPLAY_PAYLOAD_EXTRA

    def message_extra(
        self,
        stream_request: ViewerStreamRequest,
    ) -> dict[str, ViewerWireValue]:
        return stream_request.message_extra_payload()

    def component_names_request(
        self,
        stream_request: ViewerStreamRequest,
    ) -> StreamingComponentNamesRequest:
        return StreamingComponentNamesRequest.from_stream_request(stream_request)

    def after_batch_message_built(
        self,
        stream_request: ViewerStreamRequest,
        built_batch: StreamingBuiltBatch,
    ) -> None:
        pass

    def save_batch(
        self,
        data_list: list[StreamablePayload],
        file_paths: list[FilePath],
        **kwargs,
    ) -> None:
        """Stream a batch of image or ROI payloads to this viewer."""
        data_list, file_paths, _skipped_paths = self._filter_streamable_files(
            data_list,
            file_paths,
        )
        if not data_list:
            return

        stream_request = ViewerStreamBackendKwargs.from_kwargs(kwargs).stream_request
        built_batch = StreamingBatchMessageBuilder.build(
            self,
            StreamingBatchMessageRequest(
                data_list=data_list,
                file_paths=file_paths,
                stream_request=stream_request,
                component_names_request=self.component_names_request(stream_request),
                display_payload_extra=self.display_payload_extra(stream_request),
            ),
        )
        self.after_batch_message_built(stream_request, built_batch)

        transport_config = stream_request.transport_config.resolve(
            self._transport_config
        )
        transport_endpoint = stream_request.viewer_transport
        self._register_with_queue_tracker(
            transport_endpoint,
            built_batch.image_ids,
            transport_config=transport_config,
        )
        url = transport_endpoint.data_url(transport_config)

        if self._context is None:
            self._context = zmq.Context()

        viewer_name = str(self.VIEWER_TYPE).title()
        viewer_label = viewer_name.upper()
        ack_policy = STREAMING_TRANSPORT_DEFAULTS.ack_policy(viewer_name)
        socket = self._context.socket(zmq.REQ)
        ack_policy.apply_socket_options(socket)
        socket.connect(url)
        time.sleep(0.1)

        try:
            logger.info(
                "📤 %s BACKEND: Sending batch of %d images to %s on port %s "
                "(REQ/REP - blocking until ack)",
                viewer_label,
                len(built_batch.batch_images),
                viewer_name,
                transport_endpoint.port,
            )
            socket.send_json(built_batch.message)
            ack_response = ack_policy.receive(
                socket,
                lambda: self._cleanup_shared_memory_blocks(
                    built_batch.batch_images,
                    unlink=True,
                ),
                port=transport_endpoint.port,
            )
            logger.info(
                "✅ %s BACKEND: Received ack from %s: %s",
                viewer_label,
                viewer_name,
                ack_policy.status(ack_response),
            )
        finally:
            socket.close()

        # A successful REP certifies that the viewer copied every shared-memory
        # payload into receiver-owned memory. The sender remains the allocation
        # owner and releases each block only after that transfer boundary.
        self._cleanup_shared_memory_blocks(built_batch.batch_images, unlink=True)

    def save(self, data: StreamablePayload | str, file_path: FilePath, **kwargs) -> None:
        """
        Stream single item (common for all streaming backends).

        Args:
            data: Data to stream
            file_path: Path identifier
            **kwargs: Backend-specific arguments
        """
        if isinstance(data, str):
            return  # Ignore text data
        self.save_batch([data], [file_path], **kwargs)

    def cleanup(self) -> None:
        """
        Clean up shared memory and ZeroMQ resources (common for all streaming backends).
        """
        logger.info(f"🔥 CLEANUP: Starting cleanup for {self.VIEWER_TYPE}")

        # Clean up shared memory blocks
        logger.info(f"🔥 CLEANUP: About to clean {len(self._shared_memory_blocks)} shared memory blocks")
        for shm_name, shm in self._shared_memory_blocks.items():
            try:
                shm.close()
                shm.unlink()
            except Exception as e:
                logger.warning(f"Failed to cleanup shared memory {shm_name}: {e}")
        self._shared_memory_blocks.clear()
        logger.info(f"🔥 CLEANUP: Shared memory cleanup complete")

        # Close publishers
        logger.info(f"🔥 CLEANUP: About to close {len(self._publishers)} publishers")
        for key, publisher in self._publishers.items():
            try:
                logger.info(f"🔥 CLEANUP: Closing publisher {key}")
                publisher.close()
                logger.info(f"🔥 CLEANUP: Publisher {key} closed")
            except Exception as e:
                logger.warning(f"Failed to close publisher {key}: {e}")
        self._publishers.clear()
        logger.info(f"🔥 CLEANUP: Publishers cleanup complete")

        # Terminate context
        if self._context:
            try:
                logger.info(f"🔥 CLEANUP: About to terminate ZMQ context")
                self._context.term()
                logger.info(f"🔥 CLEANUP: ZMQ context terminated")
            except Exception as e:
                logger.warning(f"Failed to terminate ZMQ context: {e}")
            self._context = None

        logger.info(f"🔥 CLEANUP: {self.VIEWER_TYPE} streaming backend cleaned up")

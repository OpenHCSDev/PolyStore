"""
Streaming backend interfaces for polystore.

Provides abstract base classes for streaming data destinations that send
data to external systems without persistent storage capabilities.
"""

import logging
import os
import time
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, List, Mapping, Set, Union
import numpy as np
from arraybridge import convert_memory, detect_memory_type
from arraybridge.types import MemoryType as ArrayBridgeMemoryType

from ..base import DataSink
from ..constants import TransportMode
from ..streaming_constants import StreamingDataType
from ..roi import ROI, PointShape
from ..zmq_config import POLYSTORE_ZMQ_CONFIG
from zmqruntime.ack_listener import GlobalAckListener
from zmqruntime.transport import coerce_transport_mode

logger = logging.getLogger(__name__)


PrepareStreamingItem = Callable[[Any, Union[str, Path], Any], tuple[dict, str]]
ComponentMetadataByPath = (
    Mapping[str, Mapping[str, Any] | None]
    | Sequence[Mapping[str, Any] | None]
    | None
)


@dataclass(frozen=True)
class StreamingComponentMetadata:
    """Message metadata for one streamed item."""

    parsed_filename_metadata: Mapping[str, Any]
    source: str

    def to_payload(self) -> dict[str, Any]:
        if isinstance(self.parsed_filename_metadata, Mapping):
            metadata = dict(self.parsed_filename_metadata)
        else:
            raise TypeError(
                "Streaming component metadata must be a mapping, "
                f"got {type(self.parsed_filename_metadata).__name__}."
            )
        metadata["source"] = self.source
        return metadata


@dataclass(frozen=True)
class StreamingBatchRequest:
    """Shared provenance for one streaming batch."""

    data_list: List[Any]
    file_paths: List[Union[str, Path]]
    microscope_handler: Any
    source: str
    prepare_item: PrepareStreamingItem
    component_metadata: Mapping[str, Any] | None = None
    component_metadata_by_path: ComponentMetadataByPath = None


class StreamingPayloadMemoryAuthority:
    """Memory conversion authority for streamable image payloads."""

    @staticmethod
    def to_numpy(data: Any) -> np.ndarray:
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
    VIEWER_TYPE: str = None
    SHM_PREFIX: str = None

    # Class attribute: streaming backends only support image array data and ROIs
    supports_arbitrary_files: bool = False

    # Extensions that streaming backends can handle
    # Subclasses can override to add support for specific formats
    SUPPORTED_EXTENSIONS: set[str] = {'.tif', '.tiff', '.png', '.jpg', '.jpeg', '.roi.zip'}

    @property
    def requires_filesystem_validation(self) -> bool:
        """Streaming backends don't require filesystem validation."""
        return False

    def _filter_streamable_files(
        self,
        data_list: List[Any],
        file_paths: List[Union[str, Path]],
    ) -> tuple[List[Any], List[Union[str, Path]], List[Union[str, Path]]]:
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
            path_obj = Path(path)
            name = path_obj.name.lower()
            
            # Check if extension is supported
            is_supported = any(name.endswith(ext) for ext in self.SUPPORTED_EXTENSIONS)
            
            if is_supported:
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

    def __init__(self, transport_config=None):
        """Initialize ZeroMQ and shared memory infrastructure."""
        self._publishers = {}
        self._context = None
        self._shared_memory_blocks = {}
        self._transport_config = transport_config or POLYSTORE_ZMQ_CONFIG

    def _parse_component_metadata(
        self,
        file_path: Union[str, Path],
        microscope_handler,
        source: str,
        component_metadata: Mapping[str, Any] | None = None,
    ) -> dict:
        """
        Parse component metadata from filename (common for all streaming backends).

        Args:
            file_path: Path to parse
            microscope_handler: Handler with parser
            source: Pre-built source value (step_name during execution, subdir when loading from disk)

        Returns:
            Component metadata dict with source added
        """
        filename = os.path.basename(str(file_path))
        parsed_metadata = (
            component_metadata
            if component_metadata is not None
            else microscope_handler.parser.parse_filename(filename)
        )
        if parsed_metadata is None:
            raise ValueError(
                "Streaming component metadata requires explicit component_metadata "
                f"or a parser-readable filename; got {filename!r}."
            )
        return StreamingComponentMetadata(parsed_metadata, source).to_payload()

    @staticmethod
    def _component_metadata_for_item(
        *,
        file_path: Union[str, Path],
        index: int,
        component_metadata: Mapping[str, Any] | None,
        component_metadata_by_path: ComponentMetadataByPath,
    ) -> Mapping[str, Any] | None:
        """Return explicit component metadata for one batch item when provided."""
        if component_metadata_by_path is None:
            return component_metadata

        if isinstance(component_metadata_by_path, Mapping):
            path = Path(file_path)
            for key in (str(file_path), path.as_posix(), path.name):
                if key in component_metadata_by_path:
                    return component_metadata_by_path[key]
            return component_metadata

        if index < len(component_metadata_by_path):
            return component_metadata_by_path[index]

        return component_metadata

    def _detect_data_type(self, data: Any):
        """
        Detect if data is ROI (shapes/points) or image (common for all streaming backends).

        Args:
            data: Data to check

        Returns:
            StreamingDataType enum value (IMAGE, SHAPES, or POINTS)
        """
        is_roi = isinstance(data, list) and len(data) > 0 and isinstance(data[0], ROI)
        
        if not is_roi:
            return StreamingDataType.IMAGE
        
        # Check if all ROIs contain only PointShape objects (for points layer)
        all_points = all(
            roi.shapes and all(isinstance(shape, PointShape) for shape in roi.shapes)
            for roi in data
        )
        
        return StreamingDataType.POINTS if all_points else StreamingDataType.SHAPES

    def _create_shared_memory(self, data: Any, file_path: Union[str, Path]) -> dict:
        """
        Create shared memory for image data (common for all streaming backends).

        Args:
            data: Image data to put in shared memory
            file_path: Path identifier

        Returns:
            Dict with shared memory metadata
        """
        np_data = StreamingPayloadMemoryAuthority.to_numpy(data)

        # Create shared memory with hash-based naming to avoid "File name too long" errors
        # Hash the timestamp and object ID to create a short, unique name
        from multiprocessing import shared_memory, resource_tracker
        import hashlib
        timestamp = time.time_ns()
        obj_id = id(data)
        hash_input = f"{obj_id}_{timestamp}"
        hash_suffix = hashlib.md5(hash_input.encode()).hexdigest()[:8]
        shm_name = f"{self.SHM_PREFIX}{hash_suffix}"
        shm = shared_memory.SharedMemory(create=True, size=np_data.nbytes, name=shm_name)

        # Unregister from resource tracker - we manage cleanup manually
        # This prevents resource tracker warnings when worker processes exit
        # before the viewer has unlinked the shared memory
        try:
            resource_tracker.unregister(shm._name, "shared_memory")
        except Exception:
            pass  # Ignore errors if already unregistered

        shm_array = np.ndarray(np_data.shape, dtype=np_data.dtype, buffer=shm.buf)
        shm_array[:] = np_data[:]
        self._shared_memory_blocks[shm_name] = shm

        return {
            'path': str(file_path),
            'shape': np_data.shape,
            'dtype': str(np_data.dtype),
            'shm_name': shm_name,
        }

    def _register_with_queue_tracker(
        self,
        port: int,
        image_ids: List[str],
        transport_mode: TransportMode | None = None,
        transport_config=None,
    ) -> None:
        """
        Register sent images with queue tracker (common for all streaming backends).

        Args:
            port: Port number for tracker lookup
            image_ids: List of image IDs to register
        """
        listener = GlobalAckListener()
        transport_config = transport_config or self._transport_config
        listener.start(
            port=transport_config.shared_ack_port,
            transport_mode=coerce_transport_mode(transport_mode),
            config=transport_config,
        )

        from zmqruntime.queue_tracker import GlobalQueueTrackerRegistry
        registry = GlobalQueueTrackerRegistry()
        tracker = registry.get_or_create_tracker(port, self.VIEWER_TYPE)
        for image_id in image_ids:
            tracker.register_sent(image_id)

    def _build_component_modes(self, display_config) -> dict:
        return display_config.component_modes()

    def _build_display_config_base(self, display_config, component_modes: dict) -> dict:
        return {
            "component_modes": component_modes,
            "component_order": display_config.COMPONENT_ORDER,
        }

    def _collect_component_names_metadata(
        self,
        plate_path,
        microscope_handler,
        component_names: List[str] | None = None,
        log_prefix: str | None = None,
        verbose: bool = False,
    ) -> dict:
        component_names = component_names or ["channel", "well", "site"]
        component_names_metadata = {}

        if not plate_path or not microscope_handler:
            if verbose and log_prefix:
                if not plate_path:
                    logger.warning(f"{log_prefix}: No plate_path in kwargs")
                if not microscope_handler:
                    logger.warning(f"{log_prefix}: No microscope_handler")
            return component_names_metadata

        try:
            for comp_name in component_names:
                metadata = microscope_handler.metadata_handler.get_component_values(
                    plate_path,
                    comp_name,
                )
                if verbose and log_prefix:
                    logger.info(f"{log_prefix}: Got {comp_name} metadata: {metadata}")
                if metadata:
                    component_names_metadata[comp_name] = metadata
        except Exception as e:
            if verbose and log_prefix:
                logger.warning(f"{log_prefix}: Could not get component metadata: {e}", exc_info=True)

        return component_names_metadata

    def _prepare_batch_items(
        self,
        request: StreamingBatchRequest,
    ) -> tuple[list[dict], list[str]]:
        batch_images = []
        image_ids = []

        for index, (data, file_path) in enumerate(
            zip(request.data_list, request.file_paths)
        ):
            image_id = str(uuid.uuid4())
            image_ids.append(image_id)

            data_type = self._detect_data_type(data)
            explicit_component_metadata = self._component_metadata_for_item(
                file_path=file_path,
                index=index,
                component_metadata=request.component_metadata,
                component_metadata_by_path=request.component_metadata_by_path,
            )
            component_metadata = self._parse_component_metadata(
                file_path,
                request.microscope_handler,
                request.source,
                explicit_component_metadata,
            )
            item_data, data_type_value = request.prepare_item(data, file_path, data_type)

            batch_images.append(
                {
                    **item_data,
                    "data_type": data_type_value,
                    "metadata": component_metadata,
                    "image_id": image_id,
                }
            )

        return batch_images, image_ids

    def _build_batch_message(
        self,
        data_list: List[Any],
        file_paths: List[Union[str, Path]],
        microscope_handler,
        source: str,
        display_config,
        prepare_item: PrepareStreamingItem,
        plate_path: Union[str, Path, None] = None,
        component_names_kwargs: dict | None = None,
        component_metadata: Mapping[str, Any] | None = None,
        component_metadata_by_path: ComponentMetadataByPath = None,
        display_payload_extra: dict | None = None,
        message_extra: dict | None = None,
    ) -> tuple[dict, list[dict], list[str]]:
        if len(data_list) != len(file_paths):
            raise ValueError("data_list and file_paths must have the same length")

        batch_images, image_ids = self._prepare_batch_items(
            StreamingBatchRequest(
                data_list=data_list,
                file_paths=file_paths,
                microscope_handler=microscope_handler,
                source=source,
                prepare_item=prepare_item,
                component_metadata=component_metadata,
                component_metadata_by_path=component_metadata_by_path,
            )
        )

        component_modes = self._build_component_modes(display_config)

        component_names_metadata = self._collect_component_names_metadata(
            plate_path,
            microscope_handler,
            **(component_names_kwargs or {}),
        )

        display_payload = self._build_display_config_base(display_config, component_modes)
        if display_payload_extra:
            display_payload.update(display_payload_extra)

        message = {
            "type": "batch",
            "images": batch_images,
            "display_config": display_payload,
            "component_names_metadata": component_names_metadata,
            "timestamp": time.time(),
        }
        if message_extra:
            message.update(message_extra)

        return message, batch_images, image_ids

    def _cleanup_shared_memory_blocks(self, batch_images, unlink: bool = False) -> None:
        for img in batch_images:
            shm_name = img.get("shm_name")
            if shm_name and shm_name in self._shared_memory_blocks:
                try:
                    shm = self._shared_memory_blocks.pop(shm_name)
                    shm.close()
                    if unlink:
                        shm.unlink()
                except Exception as e:
                    logger.warning(f"Failed to cleanup shared memory {shm_name}: {e}")

    def save(self, data: Any, file_path: Union[str, Path], **kwargs) -> None:
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

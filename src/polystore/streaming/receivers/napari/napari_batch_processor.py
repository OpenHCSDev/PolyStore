import logging
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Generic, TypeVar

logger = logging.getLogger(__name__)
NapariBatchItemT = TypeVar("NapariBatchItemT")
NapariDisplayPayloadT = TypeVar("NapariDisplayPayloadT")
NapariComponentNamesMetadataT = TypeVar("NapariComponentNamesMetadataT")


@dataclass(frozen=True)
class NapariBatchDisplayRequest(
    Generic[
        NapariBatchItemT,
        NapariDisplayPayloadT,
        NapariComponentNamesMetadataT,
    ]
):
    """Nominal request for one debounced Napari display update."""

    layer_key: str
    items: Sequence[NapariBatchItemT]
    display_payload: NapariDisplayPayloadT
    component_names_metadata: NapariComponentNamesMetadataT

    def dispatch_to(self, napari_server) -> object:
        return napari_server.display_layer_batch(
            layer_key=self.layer_key,
            items=self.items,
            display_payload=self.display_payload,
            component_names_metadata=self.component_names_metadata,
        )


class NapariBatchProcessor:
    """
    Batch processor for Napari viewer display operations.

    Napari layer mutation must run on the Qt event-loop thread. OpenHCS owns that
    Qt-thread debounce before this processor is called, so this class only
    adapts batch payloads into the server display operation.
    """

    def __init__(
        self,
        napari_server,
        batch_size: int | None = None,
        debounce_delay_ms: int = 1000,
        max_debounce_wait_ms: int = 5000,
    ):
        """
        Initialize batch processor.

        Args:
            napari_server: Reference to NapariViewerServer for display operations
            batch_size: Reserved for compatibility with viewer configuration
            debounce_delay_ms: Qt-thread debounce delay owned by the caller
            max_debounce_wait_ms: Reserved for compatibility with viewer configuration
        """
        self.napari_server = napari_server
        self.batch_size = batch_size
        self.debounce_delay_ms = debounce_delay_ms
        self.max_debounce_wait_ms = max_debounce_wait_ms

        logger.info(
            f"NapariBatchProcessor: Created with batch_size={batch_size}, "
            f"debounce={debounce_delay_ms}ms, max_wait={max_debounce_wait_ms}ms"
        )

    def add_items(
        self,
        layer_key: str,
        items: Sequence[NapariBatchItemT],
        display_payload: NapariDisplayPayloadT,
        component_names_metadata: NapariComponentNamesMetadataT,
    ) -> object:
        """
        Display items already released by the Qt-thread debounce.

        Args:
            layer_key: Unique identifier for the layer
            items: List of items to add (images or ROIs)
            display_payload: Viewer-owned display payload object
            component_names_metadata: Component name mappings for dimension labels
        """
        display_work = NapariBatchDisplayRequest(
            layer_key=layer_key,
            items=items,
            display_payload=display_payload,
            component_names_metadata=component_names_metadata,
        ).dispatch_to(self.napari_server)
        logger.debug(
            "NapariBatchProcessor: Added %d items to batch for layer '%s'",
            len(items),
            layer_key,
        )
        return display_work

    def flush(self) -> None:
        """Compatibility no-op; OpenHCS owns the Qt-thread debounce timer."""

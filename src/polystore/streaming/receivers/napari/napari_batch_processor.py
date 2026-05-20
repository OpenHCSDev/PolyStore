import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


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
        batch_size: Optional[int] = None,
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
        items: List[Dict[str, Any]],
        display_config: Dict[str, Any],
        component_names_metadata: Dict[str, Any],
    ):
        """
        Display items already released by the Qt-thread debounce.
        
        Args:
            layer_key: Unique identifier for the layer
            items: List of items to add (images or ROIs)
            display_config: Display configuration dict
            component_names_metadata: Component name mappings for dimension labels
        """
        self._process_batch(
            items,
            {
                "display_config": display_config,
                "component_names_metadata": component_names_metadata,
                "layer_key": layer_key,
            },
        )
        logger.debug(
            "NapariBatchProcessor: Added %d items to batch for layer '%s'",
            len(items),
            layer_key,
        )

    def flush(self) -> None:
        """Compatibility no-op; OpenHCS owns the Qt-thread debounce timer."""

    def _process_batch(self, items: List[Dict[str, Any]], context: Dict[str, Any]) -> None:
        """Process callback used by shared debounced batch engine."""
        self.napari_server.display_layer_batch(
            layer_key=context["layer_key"],
            items=items,
            display_config=context["display_config"],
            component_names_metadata=context["component_names_metadata"],
        )

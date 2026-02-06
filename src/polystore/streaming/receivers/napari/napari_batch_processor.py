import logging
from typing import Any, Dict, List, Optional

from polystore.streaming.receivers.core import DebouncedBatchEngine

logger = logging.getLogger(__name__)


class NapariBatchProcessor:
    """
    Batch processor for Napari viewer with configurable batching strategies.
    
    Accumulates items and displays them based on batch_size configuration:
    - None: Wait for all items in operation, then display once
    - N: Display every N items incrementally
    
    Uses debouncing to collect items arriving in rapid succession.
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
            batch_size: Number of items to batch before displaying
                       None = wait for all (default), N = display every N items
            debounce_delay_ms: Wait time after last item before processing (ms)
            max_debounce_wait_ms: Maximum total wait time before forcing display (ms)
        """
        self.napari_server = napari_server
        self.batch_size = batch_size
        self.debounce_delay_ms = debounce_delay_ms
        self.max_debounce_wait_ms = max_debounce_wait_ms
        
        self._engine = DebouncedBatchEngine(
            process_fn=self._process_batch,
            debounce_delay_ms=debounce_delay_ms,
            max_debounce_wait_ms=max_debounce_wait_ms,
        )
        
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
        Add items to the batch for processing.
        
        Args:
            layer_key: Unique identifier for the layer
            items: List of items to add (images or ROIs)
            display_config: Display configuration dict
            component_names_metadata: Component name mappings for dimension labels
        """
        self._engine.enqueue(
            items=items,
            context={
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
        """Force immediate processing of the pending batch."""
        self._engine.flush()

    def _process_batch(self, items: List[Dict[str, Any]], context: Dict[str, Any]) -> None:
        """Process callback used by shared debounced batch engine."""
        self.napari_server._display_layer_batch(
            layer_key=context["layer_key"],
            items=items,
            display_config=context["display_config"],
            component_names_metadata=context["component_names_metadata"],
        )

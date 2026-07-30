import logging
from typing import Any

from polystore.streaming.receivers.core import DebouncedBatchEngine

logger = logging.getLogger(__name__)


class FijiBatchProcessor:
    """
    Batch processor for Fiji viewer with configurable batching strategies.

    Uses debouncing to collect items arriving in rapid succession.
    """

    def __init__(
        self,
        fiji_server,
        debounce_delay_ms: int = 500,
        max_debounce_wait_ms: int = 2000,
    ):
        """
        Initialize batch processor.

        Args:
            fiji_server: Reference to FijiViewerServer for display operations
            debounce_delay_ms: Wait time after last item before processing (ms)
            max_debounce_wait_ms: Maximum total wait time before forcing display (ms)
        """
        self.fiji_server = fiji_server
        self.debounce_delay_ms = debounce_delay_ms
        self.max_debounce_wait_ms = max_debounce_wait_ms

        self._engine = DebouncedBatchEngine(
            process_fn=self._process_batch,
            debounce_delay_ms=debounce_delay_ms,
            max_debounce_wait_ms=max_debounce_wait_ms,
        )

        logger.info(
            "FijiBatchProcessor: Created with debounce=%sms, max_wait=%sms",
            debounce_delay_ms,
            max_debounce_wait_ms,
        )

    def add_items(
        self,
        window_key: str,
        items: list[dict[str, Any]],
        display_config: dict[str, Any],
        images_dir: str,
        component_names_metadata: dict[str, Any],
        component_value_domain: dict[str, Any],
    ):
        """
        Add items to the batch for processing.

        Args:
            window_key: Unique identifier for the Fiji window
            items: List of items to add (images)
            display_config: Display configuration dict
            images_dir: Artifact image directory context.
            component_names_metadata: Component name mappings for dimension labels
            component_value_domain: Component value domains for axis cardinality
        """
        context = {
            "display_config": display_config,
            "images_dir": images_dir,
            "component_names_metadata": component_names_metadata,
            "component_value_domain": component_value_domain,
            "window_key": window_key,
        }
        self._engine.enqueue(items=items, context=context)
        logger.debug(
            "FijiBatchProcessor: Added %d items to batch for window '%s'",
            len(items),
            window_key,
        )

    def flush(self) -> None:
        """Force immediate processing of the pending batch."""
        self._engine.flush()

    def _process_batch(self, items: list[dict[str, Any]], context: dict[str, Any]) -> None:
        """Process callback used by shared debounced batch engine."""
        display_config = context["display_config"]
        images_dir = context["images_dir"]
        component_names_metadata = context["component_names_metadata"]
        component_value_domain = context["component_value_domain"]
        window_key = context["window_key"]
        logger.info(
            "FijiBatchProcessor: Processing batch of %d items for window '%s'",
            len(items),
            window_key,
        )
        self.fiji_server.batch_processor.process_wire_items(
            items=items,
            display_config=display_config,
            images_dir=images_dir,
            component_names_metadata=component_names_metadata,
            component_value_domain=component_value_domain,
        )

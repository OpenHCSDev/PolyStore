"""
Fiji batch processor for efficient image accumulation and hyperstack building.

Handles batching strategies:
- batch_size=None: Wait for all images, build hyperstack once (fastest)
- batch_size=N: Rebuild hyperstack incrementally every N images (provides feedback)

Optimizations:
- Incremental slice updates: Only replaces changed pixels, doesn't rebuild or recalc contrast
- Fast debounce: 500ms delay for responsive display during loading
- Smart rebuild: Only rebuilds when dimensions change (not slice replacements)
"""

import logging
import threading
import time
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class FijiBatchProcessor:
    """
    Batch processor for Fiji viewer with configurable batching strategies.
    
    Accumulates items and builds hyperstacks based on batch_size configuration:
    - None: Wait for all items in operation, then build hyperstack once
    - N: Rebuild hyperstack every N items incrementally
    
    Uses debouncing to collect items arriving in rapid succession.
    """
    
    def __init__(
        self,
        fiji_server,
        batch_size: Optional[int] = None,
        debounce_delay_ms: int = 500,
        max_debounce_wait_ms: int = 2000,
    ):
        """
        Initialize batch processor.
        
        Args:
            fiji_server: Reference to FijiViewerServer for display operations
            batch_size: Number of items to batch before displaying
                       None = wait for all (default), N = display every N items
            debounce_delay_ms: Wait time after last item before processing (ms)
            max_debounce_wait_ms: Maximum total wait time before forcing display (ms)
        """
        self.fiji_server = fiji_server
        self.batch_size = batch_size
        self.debounce_delay_ms = debounce_delay_ms
        self.max_debounce_wait_ms = max_debounce_wait_ms
        
        # Accumulation state
        self._pending_items = []
        self._pending_display_config = None
        self._pending_images_dir = None
        self._pending_component_names_metadata = {}
        self._pending_window_key = None
        
        # Debouncing state
        self._debounce_timer = None
        self._first_item_time = None
        self._lock = threading.Lock()
        
        logger.info(
            f"FijiBatchProcessor: Created with batch_size={batch_size}, "
            f"debounce={debounce_delay_ms}ms, max_wait={max_debounce_wait_ms}ms"
        )
    
    def add_items(
        self,
        window_key: str,
        items: List[Dict[str, Any]],
        display_config: Dict[str, Any],
        images_dir: str,
        component_names_metadata: Dict[str, Any],
    ):
        """
        Add items to the batch for processing.
        
        Args:
            window_key: Unique identifier for the Fiji window
            items: List of items to add (images)
            display_config: Display configuration dict
            images_dir: Source image subdirectory
            component_names_metadata: Component name mappings for dimension labels
        """
        with self._lock:
            # Add items to pending queue
            self._pending_items.extend(items)
            self._pending_display_config = display_config
            self._pending_images_dir = images_dir
            self._pending_component_names_metadata = component_names_metadata
            self._pending_window_key = window_key
            
            # Track first item time for max wait enforcement
            if self._first_item_time is None:
                self._first_item_time = time.time()
            
            current_count = len(self._pending_items)
            logger.debug(
                f"FijiBatchProcessor: Added {len(items)} items to batch "
                f"(total pending: {current_count})"
            )
            
            # Check if we should process immediately
            should_process = False
            
            if self.batch_size is not None and current_count >= self.batch_size:
                # Batch size reached - process immediately
                should_process = True
                logger.info(
                    f"FijiBatchProcessor: Batch size {self.batch_size} reached, "
                    f"processing {current_count} items"
                )
            else:
                # Check max wait time
                elapsed_ms = (time.time() - self._first_item_time) * 1000
                if elapsed_ms >= self.max_debounce_wait_ms:
                    should_process = True
                    logger.info(
                        f"FijiBatchProcessor: Max wait {self.max_debounce_wait_ms}ms exceeded, "
                        f"processing {current_count} items"
                    )
                else:
                    # Schedule debounced processing
                    self._schedule_debounced_processing()
            
            if should_process:
                self._process_pending_batch()
    
    def _schedule_debounced_processing(self):
        """Schedule debounced batch processing (called with lock held)."""
        # Cancel existing timer
        if self._debounce_timer is not None:
            self._debounce_timer.cancel()
        
        # Schedule new timer
        delay_seconds = self.debounce_delay_ms / 1000.0
        self._debounce_timer = threading.Timer(
            delay_seconds,
            self._process_pending_batch
        )
        self._debounce_timer.start()
        
        logger.debug(
            f"FijiBatchProcessor: Scheduled processing in {self.debounce_delay_ms}ms"
        )
    
    def _process_pending_batch(self):
        """Process all pending items as a batch."""
        with self._lock:
            if not self._pending_items:
                return

            items = self._pending_items
            display_config = self._pending_display_config
            images_dir = self._pending_images_dir
            component_names_metadata = self._pending_component_names_metadata
            window_key = self._pending_window_key

            # Clear pending state
            self._pending_items = []
            self._pending_display_config = None
            self._pending_images_dir = None
            self._pending_component_names_metadata = {}
            self._pending_window_key = None
            self._debounce_timer = None
            self._first_item_time = None

            logger.info(
                f"FijiBatchProcessor: Processing batch of {len(items)} items "
                f"for window '{window_key}'"
            )

        # Process outside lock to avoid blocking new items
        try:
            # Delegate to fiji server for actual hyperstack building
            # The server knows how to build hyperstacks from items
            self.fiji_server._process_items_from_batch(
                items=items,
                display_config_dict=display_config,
                images_dir=images_dir,
                component_names_metadata=component_names_metadata,
            )
            logger.info(
                f"FijiBatchProcessor: Processed {len(items)} images"
            )
        except Exception as e:
            logger.error(
                f"FijiBatchProcessor: Error processing batch: {e}",
                exc_info=True
            )


"""Generic debounced batch engine for receiver-side processing."""

from __future__ import annotations

import logging
import threading
import time
from typing import Any, Callable

from polystore.streaming.receivers.core.contracts import BatchEngineABC

logger = logging.getLogger(__name__)


BatchProcessorFn = Callable[[list[dict[str, Any]], dict[str, Any]], None]


class DebouncedBatchEngine(BatchEngineABC):
    """Thread-safe debounce + max-wait batch processor."""

    def __init__(
        self,
        *,
        process_fn: BatchProcessorFn,
        debounce_delay_ms: int,
        max_debounce_wait_ms: int,
    ):
        self._process_fn = process_fn
        self._debounce_delay = debounce_delay_ms / 1000.0
        self._max_wait = max_debounce_wait_ms / 1000.0
        self._lock = threading.Lock()
        self._timer: threading.Timer | None = None
        self._first_enqueue_time: float | None = None
        self._pending_items: list[dict[str, Any]] = []
        self._pending_context: dict[str, Any] = {}

    def enqueue(self, items: list[dict[str, Any]], context: dict[str, Any]) -> None:
        should_process_now = False
        with self._lock:
            self._pending_items.extend(items)
            self._pending_context = context

            if self._first_enqueue_time is None:
                self._first_enqueue_time = time.time()

            if self._timer is not None:
                self._timer.cancel()

            elapsed = time.time() - self._first_enqueue_time
            if elapsed >= self._max_wait:
                should_process_now = True
            else:
                remaining_wait = min(self._debounce_delay, self._max_wait - elapsed)
                self._timer = threading.Timer(remaining_wait, self.flush)
                self._timer.start()

        if should_process_now:
            self.flush()

    def flush(self) -> None:
        drained = self._drain_locked()
        if drained is None:
            return
        items, context = drained
        try:
            self._process_fn(items, context)
        except Exception as exc:
            logger.error("DebouncedBatchEngine: processing failed: %s", exc, exc_info=True)

    def _drain_locked(self) -> tuple[list[dict[str, Any]], dict[str, Any]] | None:
        with self._lock:
            if not self._pending_items:
                self._timer = None
                self._first_enqueue_time = None
                return None

            items = self._pending_items
            context = self._pending_context

            self._pending_items = []
            self._pending_context = {}
            self._timer = None
            self._first_enqueue_time = None

            return items, context

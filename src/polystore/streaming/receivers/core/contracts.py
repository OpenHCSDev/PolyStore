"""ABC contracts for receiver-side batching and projection."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class BatchEngineABC(ABC):
    """Contract for receiver-side batching/debounce engines."""

    @abstractmethod
    def enqueue(self, items: list[dict[str, Any]], context: dict[str, Any]) -> None:
        """Queue items and schedule processing."""
        raise NotImplementedError

    @abstractmethod
    def flush(self) -> None:
        """Immediately process any pending queued items."""
        raise NotImplementedError


class WindowProjectionABC(ABC):
    """Contract for item grouping/projection by component modes."""

    @abstractmethod
    def group(
        self,
        items: list[dict[str, Any]],
        component_modes: dict[str, str],
        component_order: list[str],
    ) -> Any:
        """Group items into window/channel/slice/frame projection."""
        raise NotImplementedError


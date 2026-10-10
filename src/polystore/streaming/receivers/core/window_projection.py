"""Generic window projection utilities for receiver-side batch processing."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Generic, TypeVar

from polystore.streaming.identity import (
    StreamProducerDisplayNameAuthority,
    StreamProducerIdentity,
    StreamRouteKeyAuthority,
)
from zmqruntime.viewer_protocol import (
    ViewerBatchDisplayPayload,
    ViewerBatchItemWireField,
    ViewerWireMapping,
    ViewerWireValue,
)

WindowLabel = tuple[str, ViewerWireValue]
WindowProjectionItemT = TypeVar("WindowProjectionItemT")
WindowProjectionProviderT = TypeVar(
    "WindowProjectionProviderT",
    bound="WindowProjectionPayloadProvider",
)


class WindowProjectionPayloadProvider(ABC):
    """Item that can expose its viewer wire payload for window projection."""

    @abstractmethod
    def window_projection_payload(self) -> Mapping[str, ViewerWireValue]:
        """Return the wire payload used for component/window projection."""


class WindowItemPayload(dict[str, ViewerWireValue]):
    """Normalized wire payload retained for one projected window item."""

    @classmethod
    def from_mapping(
        cls,
        payload: Mapping[str, ViewerWireValue],
    ) -> "WindowItemPayload":
        return cls(dict(payload))


@dataclass(frozen=True, slots=True)
class GroupedWindowItems(Generic[WindowProjectionItemT]):
    """Items of one batch grouped into windows."""

    window_components: tuple[str, ...]
    windows: dict[str, list[WindowProjectionItemT]]
    fixed_window_labels: dict[str, tuple[WindowLabel, ...]]


@dataclass(frozen=True, slots=True)
class WindowProjectionSource(Generic[WindowProjectionItemT]):
    """Validated receiver item source used by window projection."""

    item: WindowProjectionItemT
    payload: Mapping[str, ViewerWireValue]
    metadata: ViewerWireMapping
    producer: StreamProducerIdentity

    @classmethod
    def from_wire_payload(
        cls,
        payload: Mapping[str, ViewerWireValue],
    ) -> "WindowProjectionSource[WindowItemPayload]":
        window_payload = WindowItemPayload.from_mapping(payload)
        return cls.from_item(window_payload, window_payload)

    @classmethod
    def from_wire_payloads(
        cls,
        payloads: Sequence[Mapping[str, ViewerWireValue]],
    ) -> list["WindowProjectionSource[WindowItemPayload]"]:
        return [cls.from_wire_payload(payload) for payload in payloads]

    @classmethod
    def from_payload_provider(
        cls,
        item: WindowProjectionProviderT,
    ) -> "WindowProjectionSource[WindowProjectionProviderT]":
        return cls.from_item(item, item.window_projection_payload())

    @classmethod
    def from_payload_providers(
        cls,
        items: Sequence[WindowProjectionProviderT],
    ) -> list["WindowProjectionSource[WindowProjectionProviderT]"]:
        return [cls.from_payload_provider(item) for item in items]

    @classmethod
    def from_item(
        cls,
        item: WindowProjectionItemT,
        payload: Mapping[str, ViewerWireValue],
    ) -> "WindowProjectionSource[WindowProjectionItemT]":
        metadata = cls._required_mapping(
            payload,
            ViewerBatchItemWireField.METADATA.value,
        )
        producer_identity = cls._required_mapping(
            payload,
            ViewerBatchItemWireField.PRODUCER_IDENTITY.value,
        )
        return cls(
            item=item,
            payload=payload,
            metadata=metadata,
            producer=StreamProducerIdentity.from_payload(producer_identity),
        )

    @staticmethod
    def _required_mapping(
        payload: Mapping[str, ViewerWireValue],
        field_name: str,
    ) -> ViewerWireMapping:
        if field_name not in payload:
            raise ValueError(
                f"Viewer window projection item missing required field {field_name!r}."
            )
        value = payload[field_name]
        if not isinstance(value, Mapping):
            raise TypeError(
                f"Viewer window projection item field {field_name!r} must be a mapping, "
                f"got {type(value).__name__}."
            )
        return dict(value)


def group_items_into_windows(
    items: Sequence[WindowProjectionSource[WindowProjectionItemT]],
    display_layout: ViewerBatchDisplayPayload,
    window_components: Sequence[str],
) -> GroupedWindowItems[WindowProjectionItemT]:
    """Group items into one window per value combination of ``window_components``."""
    window_components = tuple(window_components)

    windows: dict[str, list[WindowProjectionItemT]] = {}
    fixed_window_labels: dict[str, tuple[WindowLabel, ...]] = {}
    projected_sources_by_window: dict[
        str,
        list[WindowProjectionSource[WindowProjectionItemT]],
    ] = {}

    for item in items:
        for component in display_layout.component_order:
            if component not in item.metadata:
                raise ValueError(
                    "Viewer window projection item missing declared component "
                    f"{component!r}."
                )
        key_parts: list[str] = list(item.producer.route_parts())
        fixed_labels: list[WindowLabel] = [
            (
                "producer",
                StreamProducerDisplayNameAuthority.output_label(item.producer),
            )
        ]

        for comp in window_components:
            value = item.metadata[comp]
            key_parts.append(f"{comp}_{value}")
            fixed_labels.append((comp, value))

        window_key = StreamRouteKeyAuthority.join(key_parts)
        data_type_field = ViewerBatchItemWireField.DATA_TYPE.value
        if data_type_field not in item.payload:
            raise ValueError(
                "Viewer window projection item missing required field "
                f"{data_type_field!r}."
            )
        for projected_source in projected_sources_by_window.setdefault(
            window_key,
            [],
        ):
            if (
                projected_source.payload[data_type_field]
                == item.payload[data_type_field]
                and all(
                    projected_source.metadata[component]
                    == item.metadata[component]
                    for component in display_layout.component_order
                )
                and projected_source.producer != item.producer
            ):
                raise ValueError(
                    "Viewer projection has distinct producers for the same "
                    "component coordinate and data type: "
                    f"{projected_source.producer.output_key!r} and "
                    f"{item.producer.output_key!r}."
                )
        projected_sources_by_window[window_key].append(item)
        if window_key not in fixed_window_labels:
            windows[window_key] = []
            fixed_window_labels[window_key] = tuple(fixed_labels)
        windows[window_key].append(item.item)

    return GroupedWindowItems(
        window_components=window_components,
        windows=windows,
        fixed_window_labels=fixed_window_labels,
    )

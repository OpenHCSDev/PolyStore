"""Canonical napari route-key construction."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

from polystore.streaming.identity import StreamProducerIdentity, StreamRouteKeyAuthority
from polystore.streaming_constants import StreamingDataType
from zmqruntime.viewer_protocol import ViewerWireValue


def build_route_key(
    producer_identity: StreamProducerIdentity | Mapping[str, ViewerWireValue],
    component_info: Mapping[str, ViewerWireValue],
    layer_components: Sequence[str],
    data_type: StreamingDataType,
) -> str:
    """Build a hidden route key from producer identity, layer components, and type."""
    producer = StreamProducerIdentity.from_payload(producer_identity)
    route_parts: list[str] = list(producer.route_parts())
    for component in layer_components:
        if component not in component_info:
            raise ValueError(
                f"Napari route key missing layer component {component!r}."
            )
        route_parts.append(f"{component}_{component_info[component]}")

    route_key = StreamRouteKeyAuthority.join(route_parts)

    return f"{route_key}{data_type.napari_layer_suffix}"


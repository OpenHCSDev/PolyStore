"""Nominal transport helpers for blocking viewer stream backends."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

import zmq


@dataclass(frozen=True)
class ViewerTransportDefaults:
    """Declared transport defaults shared by viewer streaming backends."""

    host: str = "localhost"
    source: str = "unknown_source"
    ack_timeout_ms: int = 30_000


@dataclass(frozen=True)
class ViewerStreamKwargs:
    """Typed view of backend kwargs at the viewer streaming boundary."""

    host: str
    port: int
    transport_mode: Any
    transport_config: Any
    display_config: Any
    microscope_handler: Any
    source: str
    plate_path: Any
    component_metadata: Any
    component_metadata_by_path: Any
    images_dir: Any = None

    @classmethod
    def from_kwargs(
        cls,
        kwargs: Mapping[str, Any],
        defaults: ViewerTransportDefaults,
        *,
        include_images_dir: bool = False,
    ) -> "ViewerStreamKwargs":
        return cls(
            host=ViewerKwargAuthority.value_or_default(kwargs, "host", defaults.host),
            port=ViewerKwargAuthority.required(kwargs, "port"),
            transport_mode=ViewerKwargAuthority.required(kwargs, "transport_mode"),
            transport_config=ViewerKwargAuthority.optional(kwargs, "transport_config"),
            display_config=ViewerKwargAuthority.required(kwargs, "display_config"),
            microscope_handler=ViewerKwargAuthority.required(kwargs, "microscope_handler"),
            source=ViewerKwargAuthority.value_or_default(kwargs, "source", defaults.source),
            plate_path=ViewerKwargAuthority.optional(kwargs, "plate_path"),
            component_metadata=ViewerKwargAuthority.optional(kwargs, "component_metadata"),
            component_metadata_by_path=ViewerKwargAuthority.optional(
                kwargs, "component_metadata_by_path"
            ),
            images_dir=(
                ViewerKwargAuthority.optional(kwargs, "images_dir")
                if include_images_dir
                else None
            ),
        )


class ViewerKwargAuthority:
    """Named access policy for viewer backend kwargs."""

    @staticmethod
    def required(kwargs: Mapping[str, Any], name: str) -> Any:
        if name not in kwargs:
            raise ValueError(f"Viewer streaming kwargs missing required field '{name}'")
        return kwargs[name]

    @staticmethod
    def optional(kwargs: Mapping[str, Any], name: str) -> Any:
        if name in kwargs:
            return kwargs[name]
        return None

    @staticmethod
    def value_or_default(kwargs: Mapping[str, Any], name: str, default: Any) -> Any:
        if name in kwargs:
            return kwargs[name]
        return default


class ViewerTransportConfigAuthority:
    """Resolve the concrete transport config without implicit truthiness fallback."""

    @staticmethod
    def resolve(transport_config: Any, default_transport_config: Any) -> Any:
        if transport_config is None:
            return default_transport_config
        return transport_config


@dataclass(frozen=True)
class ViewerAckPolicy:
    """REQ/REP ack contract for a streaming viewer."""

    viewer_name: str
    timeout_ms: int

    def apply_socket_options(self, socket: zmq.Socket) -> None:
        socket.setsockopt(zmq.LINGER, 0)
        socket.setsockopt(zmq.SNDTIMEO, self.timeout_ms)
        socket.setsockopt(zmq.RCVTIMEO, self.timeout_ms)

    def receive(self, socket: zmq.Socket, cleanup, *, port: int) -> dict[str, Any]:
        try:
            return socket.recv_json()
        except zmq.Again as exc:
            cleanup()
            raise TimeoutError(
                f"Timed out waiting {self.timeout_ms}ms for {self.viewer_name} ack on port {port}"
            ) from exc

    def status(self, ack_response: Mapping[str, Any]) -> str:
        if "status" not in ack_response:
            raise ValueError(
                f"{self.viewer_name} ack response missing required 'status': {ack_response}"
            )
        return ack_response["status"]

"""Polystore-specific ZMQ configuration defaults (env-overridable)."""

import os

from zmqruntime import ZMQConfig


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, default))
    except ValueError:
        return default


POLYSTORE_ZMQ_CONFIG = ZMQConfig(
    control_port_offset=_env_int("POLYSTORE_ZMQ_CONTROL_OFFSET", 1000),
    default_port=_env_int("POLYSTORE_ZMQ_DEFAULT_PORT", 7777),
    ipc_socket_dir=os.getenv("POLYSTORE_ZMQ_IPC_DIR", "ipc"),
    ipc_socket_prefix=os.getenv("POLYSTORE_ZMQ_IPC_PREFIX", "polystore-zmq"),
    ipc_socket_extension=os.getenv("POLYSTORE_ZMQ_IPC_EXT", ".sock"),
    shared_ack_port=_env_int("POLYSTORE_ZMQ_ACK_PORT", 0),
    app_name=os.getenv("POLYSTORE_ZMQ_APP_NAME", "polystore"),
)

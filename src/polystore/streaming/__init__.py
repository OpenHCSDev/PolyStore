"""
Streaming package for polystore.

Package-root imports stay lightweight: submodule exports load on first
attribute access instead of at import time, so declaration-only consumers
(viewer transport contracts) do not pay the NumPy/zmq streaming cost.
"""

_LAZY_EXPORTS: dict[str, str] = {
    "FilePath": "._streaming_backend",
    "RoiStreamPayload": "._streaming_backend",
    "StreamablePayload": "._streaming_backend",
    "StreamingBackend": "._streaming_backend",
    "StreamingBatchItemPreparationAuthority": "._streaming_backend",
    "StreamingBatchMessageBuilder": "._streaming_backend",
    "StreamingBatchMessageRequest": "._streaming_backend",
    "StreamingBuiltBatch": "._streaming_backend",
    "StreamingComponentNamesRequest": "._streaming_backend",
    "StreamingItemPreparationRequest": "._streaming_backend",
    "StreamingPreparedBatchItems": "._streaming_backend",
    "StreamingSharedMemoryAuthority": "._streaming_backend",
    "ViewerDisplayPayloadExtra": "._streaming_backend",
}

__all__ = list(_LAZY_EXPORTS)


def __getattr__(name: str):
    module_name = _LAZY_EXPORTS.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    import importlib

    value = getattr(importlib.import_module(module_name, __name__), name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(_LAZY_EXPORTS))

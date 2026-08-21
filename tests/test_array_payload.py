"""Storage-boundary array normalization tests."""

from types import SimpleNamespace

import numpy as np

from polystore import array_payload


def test_storage_numpy_array_preserves_numpy_identity() -> None:
    data = np.arange(4, dtype=np.uint16)

    assert array_payload.storage_numpy_array(data) is data


def test_storage_numpy_array_delegates_framework_semantics_to_arraybridge(
    monkeypatch,
) -> None:
    data = SimpleNamespace()
    converted = np.arange(4, dtype=np.float32)
    calls = []
    monkeypatch.setattr(array_payload, "detect_memory_type", lambda value: "tensor")
    monkeypatch.setattr(
        array_payload,
        "convert_memory",
        lambda value, **kwargs: calls.append((value, kwargs)) or converted,
    )

    result = array_payload.storage_numpy_array(data)

    assert result is converted
    assert calls == [
        (
            data,
            {
                "source_type": "tensor",
                "target_type": array_payload.MemoryType.NUMPY,
                "gpu_id": 0,
            },
        )
    ]

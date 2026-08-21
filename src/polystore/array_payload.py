"""ArrayBridge-backed normalization at persistent storage boundaries."""

from typing import Any

import numpy as np
from arraybridge import convert_memory, detect_memory_type
from arraybridge.types import MemoryType


def storage_numpy_array(data: Any) -> np.ndarray:
    """Return one supported array payload as a host NumPy array."""

    if isinstance(data, np.ndarray):
        return data
    source_type = detect_memory_type(data)
    converted = convert_memory(
        data,
        source_type=source_type,
        target_type=MemoryType.NUMPY,
        gpu_id=0,
    )
    return np.asarray(converted)

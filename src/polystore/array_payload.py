"""ArrayBridge-backed normalization at persistent storage boundaries."""

import numpy as np
from arraybridge import ArrayPayload
from arraybridge.libraries import ArrayLibrary


def storage_numpy_array(data: object) -> np.ndarray:
    """Return one supported array payload as a host NumPy array."""

    if isinstance(data, ArrayPayload):
        data = data.array_payload_data()
    if isinstance(data, np.ndarray):
        return data
    return ArrayLibrary.of(data).to_numpy(data)

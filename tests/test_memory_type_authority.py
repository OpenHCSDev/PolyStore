"""Authority and compatibility proofs for PolyStore memory types."""

import pickle

from arraybridge import MemoryType as ArrayBridgeMemoryType

from polystore import MemoryType
from polystore.constants import MemoryType as ConstantsMemoryType


def test_polystore_memory_type_is_arraybridge_compatibility_export() -> None:
    assert MemoryType is ArrayBridgeMemoryType
    assert ConstantsMemoryType is ArrayBridgeMemoryType


def test_legacy_polystore_memory_type_pickle_resolves_to_owner_identity() -> None:
    legacy_payload = b"cpolystore.constants\nMemoryType\np0\n(Vnumpy\np1\ntp2\nRp3\n."

    assert pickle.loads(legacy_payload) is ArrayBridgeMemoryType.NUMPY

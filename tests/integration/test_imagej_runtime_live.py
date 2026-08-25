"""Live validation of the declared Fiji runtime and Bio-Formats bridge."""

from __future__ import annotations

import os

import pytest

from polystore.imagej_runtime import FIJI_IMAGEJ_RUNTIME

pytestmark = [
    pytest.mark.live_imagej,
    pytest.mark.skipif(
        os.environ.get("POLYSTORE_LIVE_IMAGEJ_TEST") != "1",
        reason="set POLYSTORE_LIVE_IMAGEJ_TEST=1 to resolve the managed Fiji runtime",
    ),
]


def test_declared_fiji_runtime_initializes_with_bioformats() -> None:
    import imagej
    import scyjava

    gateway = None
    try:
        assert imagej.__version__ == "1.7.0"
        gateway = FIJI_IMAGEJ_RUNTIME.initialize(imagej, scyjava, mode="headless")

        assert str(gateway.getVersion()).startswith("2.18.0/")
        assert gateway.legacy.isActive()
        assert str(scyjava.jimport("java.lang.System").getProperty("java.version")).startswith(
            "21."
        )
        assert scyjava.jimport("loci.formats.ImageReader") is not None
        assert scyjava.jimport("loci.formats.MetadataTools") is not None
    finally:
        if gateway is not None or scyjava.jvm_started():
            FIJI_IMAGEJ_RUNTIME.shutdown(gateway, scyjava)

    assert not scyjava.jvm_started()

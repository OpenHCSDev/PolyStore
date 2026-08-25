"""Live validation of the declared Fiji runtime and Bio-Formats bridge."""

from __future__ import annotations

import os
import urllib.request
from importlib.metadata import version

import pytest

from polystore.imagej_runtime import FIJI_IMAGEJ_RUNTIME

pytestmark = [
    pytest.mark.live_imagej,
    pytest.mark.skipif(
        os.environ.get("POLYSTORE_LIVE_IMAGEJ_TEST") != "1",
        reason="set POLYSTORE_LIVE_IMAGEJ_TEST=1 to resolve the managed Fiji runtime",
    ),
]


def test_declared_fiji_runtime_initializes_with_bioformats(monkeypatch) -> None:
    import imagej
    import requests
    import scyjava

    launch = FIJI_IMAGEJ_RUNTIME.distribution.materialize()

    assert (launch.imagej_directory / "jars/imglib2-imglyb-1.1.0.jar").is_file()
    assert (launch.imagej_directory / "jars/imglib2-unsafe-1.0.0.jar").is_file()
    assert (launch.imagej_directory / "licenses/imglib2-imglyb-LICENSE.txt").is_file()
    assert (launch.imagej_directory / "licenses/imglib2-unsafe-LICENSE.txt").is_file()

    def reject_runtime_http(*_args, **_kwargs):
        raise AssertionError("The materialized Fiji runtime must initialize without HTTP")

    monkeypatch.setattr(requests.sessions.Session, "request", reject_runtime_http)
    monkeypatch.setattr(urllib.request, "urlopen", reject_runtime_http)
    gateway = None
    try:
        assert imagej.__version__ == "1.7.0"
        assert version("imglyb") == "2.1.0"
        assert version("scyjava") == "1.12.5"
        gateway = FIJI_IMAGEJ_RUNTIME.initialize(imagej, scyjava, mode="headless")

        assert str(gateway.getVersion()).startswith("2.18.0/")
        assert gateway.legacy.isActive()
        assert str(scyjava.jimport("java.lang.System").getProperty("java.version")).startswith(
            "21."
        )
        assert scyjava.config.endpoints == []
        assert scyjava.jimport("loci.formats.ImageReader") is not None
        assert scyjava.jimport("loci.formats.MetadataTools") is not None
    finally:
        if gateway is not None or scyjava.jvm_started():
            FIJI_IMAGEJ_RUNTIME.shutdown(gateway, scyjava)

    assert not scyjava.jvm_started()

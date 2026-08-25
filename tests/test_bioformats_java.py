from __future__ import annotations

from dataclasses import dataclass

import pytest

import polystore.bioformats_java as bioformats_java
from polystore.backend_registry import cleanup_backend_connections
from polystore.bioformats_java import (
    BioFormatsJavaContext,
    BioFormatsJavaUnavailableError,
)


@dataclass
class _Gateway:
    dispose_count: int = 0

    def dispose(self) -> None:
        self.dispose_count += 1


class _Runtime:
    def __init__(self) -> None:
        self.gateways: list[_Gateway] = []
        self.shutdown_gateways: list[_Gateway | None] = []

    def initialize(self, imagej_module, scyjava_module, *, mode: str) -> _Gateway:
        assert imagej_module == "imagej"
        assert scyjava_module is not None
        assert mode == "headless"
        gateway = _Gateway()
        self.gateways.append(gateway)
        return gateway

    def shutdown(self, gateway: _Gateway | None, scyjava_module) -> None:
        self.shutdown_gateways.append(gateway)
        if gateway is not None:
            gateway.dispose()
        scyjava_module.shutdown_jvm()


class _ScyJava:
    def __init__(self, *, failing_import: str | None = None) -> None:
        self.failing_import = failing_import
        self.imports: list[str] = []
        self.shutdown_count = 0

    def jimport(self, name: str) -> object:
        self.imports.append(name)
        if name == self.failing_import:
            raise RuntimeError(f"could not import {name}")
        return object()

    def shutdown_jvm(self) -> None:
        self.shutdown_count += 1


def test_bioformats_context_disposes_and_can_reinitialize(monkeypatch) -> None:
    runtime = _Runtime()
    scyjava = _ScyJava()
    monkeypatch.setattr(bioformats_java, "FIJI_IMAGEJ_RUNTIME", runtime)
    context = BioFormatsJavaContext("imagej", scyjava)

    context.ensure_initialized()
    first_gateway = context.ij
    context.ensure_initialized()

    assert len(runtime.gateways) == 1
    context.dispose()
    context.dispose()
    assert first_gateway.dispose_count == 1
    assert context.ij is None
    assert context.ImageReader is None
    assert context.MetadataTools is None
    assert context.FormatTools is None

    context.ensure_initialized()
    assert len(runtime.gateways) == 2
    assert context.ij is runtime.gateways[-1]


def test_bioformats_context_disposes_partial_gateway_on_import_failure(
    monkeypatch,
) -> None:
    runtime = _Runtime()
    scyjava = _ScyJava(failing_import="loci.formats.MetadataTools")
    monkeypatch.setattr(bioformats_java, "FIJI_IMAGEJ_RUNTIME", runtime)
    context = BioFormatsJavaContext("imagej", scyjava)

    with pytest.raises(
        BioFormatsJavaUnavailableError,
        match="Could not initialize Fiji/Bio-Formats through pyimagej",
    ):
        context.ensure_initialized()

    assert runtime.gateways[0].dispose_count == 1
    assert context.ij is None
    assert context.ImageReader is None
    assert context.MetadataTools is None
    assert context.FormatTools is None


def test_dispose_instance_does_not_create_process_context(monkeypatch) -> None:
    monkeypatch.setattr(BioFormatsJavaContext, "_instance", None)

    BioFormatsJavaContext.dispose_instance()

    assert BioFormatsJavaContext._instance is None


def test_test_runtime_cleanup_stops_process_context(monkeypatch) -> None:
    runtime = _Runtime()
    scyjava = _ScyJava()
    monkeypatch.setattr(bioformats_java, "FIJI_IMAGEJ_RUNTIME", runtime)
    context = BioFormatsJavaContext("imagej", scyjava)
    gateway = _Gateway()
    context.ij = gateway
    monkeypatch.setattr(BioFormatsJavaContext, "_instance", context)

    cleanup_backend_connections(include_process_resources=True)

    assert gateway.dispose_count == 1
    assert context.ij is None
    assert runtime.shutdown_gateways == [gateway]
    assert scyjava.shutdown_count == 1
    assert BioFormatsJavaContext._instance is None

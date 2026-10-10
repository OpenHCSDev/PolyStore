from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path

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


class _ProbeReader:
    def __init__(self, result):
        self.result = result
        self.paths = []
        self.close_count = 0

    def _evaluate(self, path):
        self.paths.append(path)
        if isinstance(self.result, Exception):
            raise self.result
        return self.result

    isThisType = _evaluate
    isSingleFile = _evaluate

    def setId(self, path):
        raise AssertionError("A format probe must not open container metadata")

    def close(self):
        self.close_count += 1


class _ProbeScyJava(_ScyJava):
    def __init__(self, result):
        super().__init__()
        self.result = result
        self.readers = []

    def _reader(self):
        reader = _ProbeReader(self.result)
        self.readers.append(reader)
        return reader

    def jimport(self, name):
        declaration = super().jimport(name)
        return self._reader if name == "loci.formats.ImageReader" else declaration


@pytest.mark.parametrize("operation", ("declares_path", "is_single_file"))
@pytest.mark.parametrize("result", (True, False))
def test_format_probe_owns_initialization_and_reader_lifetime(monkeypatch, operation, result):
    runtime = _Runtime()
    scyjava = _ProbeScyJava(result)
    monkeypatch.setattr(bioformats_java, "FIJI_IMAGEJ_RUNTIME", runtime)
    context = BioFormatsJavaContext("imagej", scyjava)
    probe = getattr(context, operation)

    assert probe(Path("first.czi")) is result
    assert probe("second.czi") is result

    assert len(runtime.gateways) == 1
    assert len(scyjava.imports) == 3
    assert [reader.paths for reader in scyjava.readers] == [["first.czi"], ["second.czi"]]
    assert [reader.close_count for reader in scyjava.readers] == [1, 1]
    context.dispose()
    assert runtime.gateways[0].dispose_count == 1


@pytest.mark.parametrize("operation", ("declares_path", "is_single_file"))
def test_format_probe_closes_failed_reader_and_can_retry(monkeypatch, operation):
    runtime = _Runtime()
    scyjava = _ProbeScyJava(RuntimeError("decoder probe failed"))
    monkeypatch.setattr(bioformats_java, "FIJI_IMAGEJ_RUNTIME", runtime)
    context = BioFormatsJavaContext("imagej", scyjava)
    probe = getattr(context, operation)

    with pytest.raises(RuntimeError, match="decoder probe failed"):
        probe("bad.czi")
    assert scyjava.readers[0].close_count == 1
    scyjava.result = True
    assert probe("good.czi")
    assert scyjava.readers[1].close_count == 1
    assert len(runtime.gateways) == 1
    context.dispose()


def test_format_probe_operations_share_the_context_reader_lifetime():
    tree = ast.parse(Path(bioformats_java.__file__).read_text(encoding="utf-8"))
    context = next(
        node
        for node in tree.body
        if isinstance(node, ast.ClassDef) and node.name == "BioFormatsJavaContext"
    )
    for operation in ("declares_path", "is_single_file"):
        method = next(
            node
            for node in context.body
            if isinstance(node, ast.FunctionDef) and node.name == operation
        )
        attributes = {node.attr for node in ast.walk(method) if isinstance(node, ast.Attribute)}
        assert "_probe_reader" in attributes
        assert not attributes & {"ImageReader", "ensure_initialized", "close", "setId"}

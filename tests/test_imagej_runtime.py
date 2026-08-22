"""Tests for declaration-owned ImageJ runtime selection."""

from types import SimpleNamespace

import pytest

from polystore.imagej_runtime import (
    FIJI_IMAGEJ_RUNTIME,
    ImageJRuntimePolicy,
    ImageJRuntimeUnavailableError,
)


class _SystemProperties:
    def __init__(self, java_version: str) -> None:
        self._java_version = java_version

    def getProperty(self, name: str) -> str:
        assert name == "java.version"
        return self._java_version


class _ScyJava:
    def __init__(self, *, started: bool, java_version: str) -> None:
        self._started = started
        self.constraints: list[dict[str, str]] = []
        self.config = SimpleNamespace(
            set_java_constraints=lambda **constraints: self.constraints.append(constraints)
        )
        self._system = _SystemProperties(java_version)

    def jvm_started(self) -> bool:
        return self._started

    def jimport(self, name: str) -> _SystemProperties:
        assert name == "java.lang.System"
        return self._system


class _ImageJ:
    def __init__(self, scyjava: _ScyJava) -> None:
        self._scyjava = scyjava
        self.calls: list[tuple[str, str]] = []
        self.gateway = object()

    def init(self, endpoint: str, *, mode: str) -> object:
        self.calls.append((endpoint, mode))
        self._scyjava._started = True
        return self.gateway


def test_fiji_runtime_selects_managed_java_before_initialization() -> None:
    scyjava = _ScyJava(started=False, java_version="21.0.8")
    imagej = _ImageJ(scyjava)

    gateway = FIJI_IMAGEJ_RUNTIME.initialize(imagej, scyjava, mode="headless")

    assert gateway is imagej.gateway
    assert scyjava.constraints == [{"fetch": "always", "vendor": "zulu-jre", "version": "21"}]
    assert imagej.calls == [("sc.fiji:fiji", "headless")]


def test_runtime_accepts_compatible_active_java_without_reconfiguration() -> None:
    scyjava = _ScyJava(started=True, java_version="21")
    imagej = _ImageJ(scyjava)

    FIJI_IMAGEJ_RUNTIME.initialize(imagej, scyjava, mode="interactive")

    assert scyjava.constraints == []
    assert imagej.calls == [("sc.fiji:fiji", "interactive")]


@pytest.mark.parametrize("java_version", ("1.8.0_452", "11.0.28", "26"))
def test_runtime_rejects_an_incompatible_active_jvm(java_version: str) -> None:
    scyjava = _ScyJava(started=True, java_version=java_version)
    imagej = _ImageJ(scyjava)

    with pytest.raises(
        ImageJRuntimeUnavailableError,
        match=rf"requires Java 21; the active JVM is Java {java_version}",
    ):
        FIJI_IMAGEJ_RUNTIME.initialize(imagej, scyjava, mode="headless")

    assert imagej.calls == []


def test_runtime_reports_an_unparseable_active_java_version() -> None:
    runtime = ImageJRuntimePolicy(
        endpoint="example:imagej",
        java_fetch="always",
        java_vendor="example-jre",
        java_version="21",
    )
    scyjava = _ScyJava(started=True, java_version="unknown")

    with pytest.raises(
        ImageJRuntimeUnavailableError,
        match="Could not determine the active Java feature version",
    ):
        runtime.configure_java(scyjava)


def test_runtime_wraps_imagej_initialization_failure() -> None:
    scyjava = _ScyJava(started=False, java_version="21")

    def fail_initialization(endpoint: str, *, mode: str) -> None:
        raise RuntimeError(f"failed {endpoint} in {mode}")

    imagej = SimpleNamespace(init=fail_initialization)

    with pytest.raises(
        ImageJRuntimeUnavailableError,
        match="Could not initialize sc.fiji:fiji with managed Java 21",
    ) as exc_info:
        FIJI_IMAGEJ_RUNTIME.initialize(imagej, scyjava, mode="headless")

    assert isinstance(exc_info.value.__cause__, RuntimeError)

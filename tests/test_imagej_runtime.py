"""Tests for declaration-owned ImageJ runtime selection."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any

import pytest

from polystore.imagej_distribution import (
    FIJI_IMAGEJ_DISTRIBUTION,
    ImageJDistributionABC,
    ImageJRuntimeLaunch,
)
from polystore.imagej_runtime import (
    FIJI_IMAGEJ_RUNTIME,
    ImageJRuntimePolicy,
    ImageJRuntimeUnavailableError,
)


@pytest.fixture(autouse=True)
def _isolated_jpype_config(monkeypatch) -> None:
    """Provide the lazy optional module without adding it to base test installs."""

    jpype_module = ModuleType("jpype")
    config_module = ModuleType("jpype.config")
    config_module.destroy_jvm = False
    jpype_module.config = config_module
    monkeypatch.setitem(sys.modules, "jpype", jpype_module)
    monkeypatch.setitem(sys.modules, "jpype.config", config_module)


class _SystemProperties:
    def __init__(self, java_version: str) -> None:
        self._java_version = java_version

    def getProperty(self, name: str) -> str:
        assert name == "java.version"
        return self._java_version


class _ScyJava:
    def __init__(
        self,
        *,
        started: bool,
        java_version: str,
        endpoints: tuple[str, ...] = (),
    ) -> None:
        self._started = started
        self.constraints: list[dict[str, str]] = []
        self.shutdown_count = 0
        self.config = SimpleNamespace(
            endpoints=list(endpoints),
            set_java_constraints=lambda **constraints: self.constraints.append(constraints),
        )
        self._system = _SystemProperties(java_version)

    def jvm_started(self) -> bool:
        return self._started

    def jimport(self, name: str) -> _SystemProperties:
        assert name == "java.lang.System"
        return self._system

    def shutdown_jvm(self) -> None:
        self.shutdown_count += 1
        self._started = False


class _Gateway:
    def __init__(self, events: list[str] | None = None) -> None:
        self.dispose_count = 0
        self.events = events

    def dispose(self) -> None:
        self.dispose_count += 1
        if self.events is not None:
            self.events.append("gateway_disposed")


class _Distribution(ImageJDistributionABC):
    def __init__(self) -> None:
        self.materialization_count = 0
        self.compatibility_checks: list[Any] = []

    @property
    def label(self) -> str:
        return "test ImageJ"

    def materialize(self) -> ImageJRuntimeLaunch:
        self.materialization_count += 1
        return ImageJRuntimeLaunch(
            imagej_directory=Path("/test/imagej"),
            java_home=Path("/test/java"),
        )

    def require_compatible_gateway(self, gateway: Any) -> None:
        self.compatibility_checks.append(gateway)


class _ImageJ:
    def __init__(self, scyjava: _ScyJava) -> None:
        self._scyjava = scyjava
        self.calls: list[tuple[str | None, str]] = []
        self.java_homes: list[str | None] = []
        self.gateway = _Gateway()

    def init(self, target: str | None, *, mode: str) -> object:
        self.calls.append((target, mode))
        self.java_homes.append(os.environ.get("JAVA_HOME"))
        self._scyjava._started = True
        return self.gateway


def _runtime(
    distribution: ImageJDistributionABC,
    *,
    retry_delays: tuple[float, ...] = (),
) -> ImageJRuntimePolicy:
    return ImageJRuntimePolicy(
        distribution=distribution,
        java_version="21",
        initialization_retry_delays_seconds=retry_delays,
    )


def test_fiji_runtime_owns_one_archived_distribution() -> None:
    assert FIJI_IMAGEJ_RUNTIME.distribution is FIJI_IMAGEJ_DISTRIBUTION
    assert FIJI_IMAGEJ_RUNTIME.java_version == "21"


def test_runtime_selects_bundled_java_before_initialization(monkeypatch) -> None:
    import jpype.config

    monkeypatch.setattr(jpype.config, "destroy_jvm", False)
    distribution = _Distribution()
    runtime = _runtime(distribution)
    scyjava = _ScyJava(
        started=False,
        java_version="21.0.7",
        endpoints=("example:external-runtime",),
    )
    imagej = _ImageJ(scyjava)

    gateway = runtime.initialize(imagej, scyjava, mode="headless")

    assert gateway is imagej.gateway
    assert scyjava.constraints == [{"fetch": "never", "version": "21"}]
    assert scyjava.config.endpoints == []
    assert imagej.calls == [(str(Path("/test/imagej")), "headless")]
    assert imagej.java_homes == [str(Path("/test/java"))]
    assert distribution.materialization_count == 1
    assert distribution.compatibility_checks == [gateway]
    assert jpype.config.destroy_jvm is True


def test_fiji_runtime_shuts_down_gateway_before_jvm(monkeypatch) -> None:
    import jpype.config

    events: list[str] = []
    gateway = _Gateway(events)
    scyjava = _ScyJava(started=True, java_version="21")
    original_shutdown = scyjava.shutdown_jvm

    def record_shutdown() -> None:
        events.append("jvm_shutdown")
        original_shutdown()

    scyjava.shutdown_jvm = record_shutdown
    monkeypatch.setattr(jpype.config, "destroy_jvm", False)

    FIJI_IMAGEJ_RUNTIME.shutdown(gateway, scyjava)

    assert events == ["gateway_disposed", "jvm_shutdown"]
    assert gateway.dispose_count == 1
    assert scyjava.shutdown_count == 1
    assert jpype.config.destroy_jvm is True


def test_fiji_runtime_stops_jvm_after_gateway_disposal_failure() -> None:
    class _FailingGateway:
        def dispose(self) -> None:
            raise RuntimeError("gateway disposal failed")

    scyjava = _ScyJava(started=True, java_version="21")

    with pytest.raises(RuntimeError, match="gateway disposal failed"):
        FIJI_IMAGEJ_RUNTIME.shutdown(_FailingGateway(), scyjava)

    assert scyjava.shutdown_count == 1


def test_runtime_accepts_compatible_active_java_without_materialization() -> None:
    distribution = _Distribution()
    runtime = _runtime(distribution)
    scyjava = _ScyJava(started=True, java_version="21")
    imagej = _ImageJ(scyjava)

    runtime.initialize(imagej, scyjava, mode="interactive")

    assert scyjava.constraints == []
    assert imagej.calls == [(None, "interactive")]
    assert distribution.materialization_count == 0
    assert distribution.compatibility_checks == [imagej.gateway]


@pytest.mark.parametrize("java_version", ("1.8.0_452", "11.0.28", "17.0.16", "26"))
def test_runtime_rejects_an_incompatible_active_jvm(java_version: str) -> None:
    runtime = _runtime(_Distribution())
    scyjava = _ScyJava(started=True, java_version=java_version)
    imagej = _ImageJ(scyjava)

    with pytest.raises(
        ImageJRuntimeUnavailableError,
        match=rf"requires Java 21; the active JVM is Java {java_version}",
    ):
        runtime.initialize(imagej, scyjava, mode="headless")

    assert imagej.calls == []


def test_runtime_reports_an_unparseable_active_java_version() -> None:
    runtime = _runtime(_Distribution())
    scyjava = _ScyJava(started=True, java_version="unknown")

    with pytest.raises(
        ImageJRuntimeUnavailableError,
        match="Could not determine the active Java feature version",
    ):
        runtime.configure_java(scyjava)


def test_runtime_wraps_imagej_initialization_failure() -> None:
    runtime = _runtime(_Distribution())
    scyjava = _ScyJava(started=False, java_version="21")
    calls = 0

    def fail_initialization(target: str, *, mode: str) -> None:
        nonlocal calls
        calls += 1
        raise RuntimeError(f"failed {target} in {mode}")

    imagej = SimpleNamespace(init=fail_initialization)

    with pytest.raises(
        ImageJRuntimeUnavailableError,
        match="Could not initialize test ImageJ with bundled Java 21",
    ) as exc_info:
        runtime.initialize(imagej, scyjava, mode="headless")

    assert isinstance(exc_info.value.__cause__, RuntimeError)
    assert calls == 1


def test_runtime_retries_initialization_only_before_jvm_start(monkeypatch) -> None:
    observed_delays: list[float] = []
    monkeypatch.setattr(
        "polystore.imagej_runtime.time.sleep",
        observed_delays.append,
    )
    runtime = _runtime(_Distribution(), retry_delays=(0.1, 0.2))
    scyjava = _ScyJava(started=False, java_version="21")
    calls = 0

    def initialize_after_transient_failures(target: str, *, mode: str) -> object:
        nonlocal calls
        calls += 1
        if calls < 3:
            raise TimeoutError(f"transient failure for {target} in {mode}")
        scyjava._started = True
        return object()

    gateway = runtime.initialize(
        SimpleNamespace(init=initialize_after_transient_failures),
        scyjava,
        mode="headless",
    )

    assert gateway is not None
    assert calls == 3
    assert observed_delays == [0.1, 0.2]


def test_runtime_preserves_final_failure_after_retry_schedule() -> None:
    runtime = _runtime(_Distribution(), retry_delays=(0.0, 0.0))
    scyjava = _ScyJava(started=False, java_version="21")
    calls = 0

    def fail_initialization(target: str, *, mode: str) -> None:
        nonlocal calls
        calls += 1
        raise TimeoutError(f"transient failure for {target} in {mode}")

    with pytest.raises(
        ImageJRuntimeUnavailableError,
        match="Could not initialize test ImageJ with bundled Java 21",
    ) as exc_info:
        runtime.initialize(
            SimpleNamespace(init=fail_initialization),
            scyjava,
            mode="headless",
        )

    assert calls == 3
    assert isinstance(exc_info.value.__cause__, TimeoutError)

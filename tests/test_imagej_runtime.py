"""Tests for declaration-owned ImageJ runtime selection."""

from types import SimpleNamespace

import pytest

from polystore.imagej_runtime import (
    FIJI_IMAGEJ_RUNTIME,
    ImageJJvmTeardown,
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


def test_fiji_runtime_selects_managed_java_before_initialization(monkeypatch) -> None:
    import jpype.config

    monkeypatch.setattr(jpype.config, "destroy_jvm", True)
    scyjava = _ScyJava(started=False, java_version="21.0.8")
    imagej = _ImageJ(scyjava)

    gateway = FIJI_IMAGEJ_RUNTIME.initialize(imagej, scyjava, mode="headless")

    assert gateway is imagej.gateway
    assert scyjava.constraints == [{"fetch": "always", "vendor": "zulu-jre", "version": "21"}]
    assert imagej.calls == [("sc.fiji:fiji", "headless")]
    assert jpype.config.destroy_jvm is False


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
        jvm_teardown=ImageJJvmTeardown.PROCESS_EXIT,
    )
    scyjava = _ScyJava(started=True, java_version="unknown")

    with pytest.raises(
        ImageJRuntimeUnavailableError,
        match="Could not determine the active Java feature version",
    ):
        runtime.configure_java(scyjava)


def test_runtime_wraps_imagej_initialization_failure() -> None:
    scyjava = _ScyJava(started=True, java_version="21")
    calls = 0

    def fail_initialization(endpoint: str, *, mode: str) -> None:
        nonlocal calls
        calls += 1
        raise RuntimeError(f"failed {endpoint} in {mode}")

    imagej = SimpleNamespace(init=fail_initialization)

    with pytest.raises(
        ImageJRuntimeUnavailableError,
        match="Could not initialize sc.fiji:fiji with managed Java 21",
    ) as exc_info:
        FIJI_IMAGEJ_RUNTIME.initialize(imagej, scyjava, mode="headless")

    assert isinstance(exc_info.value.__cause__, RuntimeError)
    assert calls == 1


def test_runtime_retries_initialization_only_before_jvm_start(monkeypatch) -> None:
    observed_delays: list[float] = []
    monkeypatch.setattr(
        "polystore.imagej_runtime.time.sleep",
        observed_delays.append,
    )
    runtime = ImageJRuntimePolicy(
        endpoint="example:imagej",
        java_fetch="always",
        java_vendor="example-jre",
        java_version="21",
        jvm_teardown=ImageJJvmTeardown.PROCESS_EXIT,
        initialization_retry_delays_seconds=(0.1, 0.2),
    )
    scyjava = _ScyJava(started=False, java_version="21")
    calls = 0

    def initialize_after_transient_failures(endpoint: str, *, mode: str) -> object:
        nonlocal calls
        calls += 1
        if calls < 3:
            raise TimeoutError(f"transient failure for {endpoint} in {mode}")
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
    runtime = ImageJRuntimePolicy(
        endpoint="example:imagej",
        java_fetch="always",
        java_vendor="example-jre",
        java_version="21",
        jvm_teardown=ImageJJvmTeardown.PROCESS_EXIT,
        initialization_retry_delays_seconds=(0.0, 0.0),
    )
    scyjava = _ScyJava(started=False, java_version="21")
    calls = 0

    def fail_initialization(endpoint: str, *, mode: str) -> None:
        nonlocal calls
        calls += 1
        raise TimeoutError(f"transient failure for {endpoint} in {mode}")

    with pytest.raises(
        ImageJRuntimeUnavailableError,
        match="Could not initialize example:imagej with managed Java 21",
    ) as exc_info:
        runtime.initialize(
            SimpleNamespace(init=fail_initialization),
            scyjava,
            mode="headless",
        )

    assert calls == 3
    assert isinstance(exc_info.value.__cause__, TimeoutError)

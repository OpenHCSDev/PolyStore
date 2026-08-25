"""Declared runtime requirements for ImageJ distributions."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any

from .imagej_distribution import FIJI_IMAGEJ_DISTRIBUTION, ImageJDistributionABC

logger = logging.getLogger(__name__)


class ImageJRuntimeUnavailableError(RuntimeError):
    """Raised when an ImageJ distribution cannot use its declared Java runtime."""


@dataclass(frozen=True, slots=True)
class ImageJRuntimePolicy:
    """Own an ImageJ distribution, bundled Java requirement, and process lifecycle."""

    distribution: ImageJDistributionABC
    java_version: str
    initialization_retry_delays_seconds: tuple[float, ...] = ()

    @property
    def java_major_version(self) -> int:
        """Return the declared Java feature version."""

        return int(self.java_version.partition(".")[0])

    def initialize(
        self,
        imagej_module: Any,
        scyjava_module: Any,
        *,
        mode: str,
    ) -> Any:
        """Initialize the declared distribution with its compatible bundled JVM."""

        if scyjava_module.jvm_started():
            self.require_compatible_active_java(scyjava_module)
            gateway = imagej_module.init(None, mode=mode)
            self.distribution.require_compatible_gateway(gateway)
            return gateway

        launch = self.distribution.materialize()
        self.configure_java(scyjava_module)
        retry_delays = iter(self.initialization_retry_delays_seconds)
        with launch.activated_environment() as imagej_target:
            while True:
                try:
                    gateway = imagej_module.init(imagej_target, mode=mode)
                    break
                except Exception as exc:
                    retry_delay = next(retry_delays, None)
                    if retry_delay is None or scyjava_module.jvm_started():
                        raise ImageJRuntimeUnavailableError(
                            f"Could not initialize {self.distribution.label} with "
                            f"bundled Java {self.java_version}."
                        ) from exc
                    logger.warning(
                        "ImageJ initialization failed before JVM startup; retrying %s "
                        "in %.1f seconds: %s",
                        self.distribution.label,
                        retry_delay,
                        exc,
                    )
                    time.sleep(retry_delay)
        self.require_compatible_active_java(scyjava_module)
        self.distribution.require_compatible_gateway(gateway)
        return gateway

    def configure_java(self, scyjava_module: Any) -> None:
        """Make the bundle the sole fresh-JVM classpath and Java authority."""

        self._configure_controlled_jvm_shutdown()
        if scyjava_module.jvm_started():
            self.require_compatible_active_java(scyjava_module)
            return
        scyjava_module.config.endpoints.clear()
        scyjava_module.config.set_java_constraints(
            fetch="never",
            version=self.java_version,
        )

    def shutdown(self, gateway: Any | None, scyjava_module: Any) -> None:
        """Dispose the ImageJ gateway and stop its JVM before Python finalization."""

        self._configure_controlled_jvm_shutdown()
        try:
            if gateway is not None:
                gateway.dispose()
        finally:
            scyjava_module.shutdown_jvm()

    @staticmethod
    def _configure_controlled_jvm_shutdown() -> None:
        """Require explicit JVM destruction while Python runtime state is valid."""

        import jpype.config

        jpype.config.destroy_jvm = True

    def require_compatible_active_java(self, scyjava_module: Any) -> None:
        """Reject a JVM whose feature version differs from this declaration."""

        active_version = str(scyjava_module.jimport("java.lang.System").getProperty("java.version"))
        active_major = _java_major_version(active_version)
        if active_major != self.java_major_version:
            raise ImageJRuntimeUnavailableError(
                f"{self.distribution.label} requires Java {self.java_version}; the "
                f"active JVM is Java {active_version}. Start this runtime in a fresh "
                "process."
            )


def _java_major_version(version: str) -> int:
    """Project legacy and modern Java version strings onto their feature version."""

    components = version.split(".")
    try:
        feature_component = components[1] if components[0] == "1" else components[0]
        return int(feature_component)
    except (IndexError, ValueError) as exc:
        raise ImageJRuntimeUnavailableError(
            f"Could not determine the active Java feature version from {version!r}."
        ) from exc


FIJI_IMAGEJ_RUNTIME = ImageJRuntimePolicy(
    distribution=FIJI_IMAGEJ_DISTRIBUTION,
    java_version="21",
    initialization_retry_delays_seconds=(1.0, 2.0),
)

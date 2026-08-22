"""Declared runtime requirements for ImageJ distributions."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


class ImageJRuntimeUnavailableError(RuntimeError):
    """Raised when an ImageJ distribution cannot use its declared Java runtime."""


@dataclass(frozen=True, slots=True)
class ImageJRuntimePolicy:
    """Own the endpoint and managed Java requirement for an ImageJ runtime."""

    endpoint: str
    java_fetch: str
    java_vendor: str
    java_version: str

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
        """Initialize the declared endpoint with its compatible managed JVM."""

        self.configure_java(scyjava_module)
        try:
            gateway = imagej_module.init(self.endpoint, mode=mode)
        except Exception as exc:
            raise ImageJRuntimeUnavailableError(
                f"Could not initialize {self.endpoint} with managed Java " f"{self.java_version}."
            ) from exc
        self.require_compatible_active_java(scyjava_module)
        return gateway

    def configure_java(self, scyjava_module: Any) -> None:
        """Select managed Java before startup or validate the active JVM."""

        if scyjava_module.jvm_started():
            self.require_compatible_active_java(scyjava_module)
            return
        scyjava_module.config.set_java_constraints(
            fetch=self.java_fetch,
            vendor=self.java_vendor,
            version=self.java_version,
        )

    def require_compatible_active_java(self, scyjava_module: Any) -> None:
        """Reject a JVM whose feature version differs from this declaration."""

        active_version = str(scyjava_module.jimport("java.lang.System").getProperty("java.version"))
        active_major = _java_major_version(active_version)
        if active_major != self.java_major_version:
            raise ImageJRuntimeUnavailableError(
                f"{self.endpoint} requires Java {self.java_version}; the active JVM "
                f"is Java {active_version}. Start this runtime in a fresh process."
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
    endpoint="sc.fiji:fiji",
    java_fetch="always",
    java_vendor="zulu-jre",
    java_version="21",
)

"""Versioned ImageJ distributions and their local runtime materialization."""

from __future__ import annotations

import hashlib
import http.client
import logging
import os
import platform
import shutil
import stat
import tempfile
import time
import urllib.request
import zipfile
from abc import ABC, abstractmethod
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum
from pathlib import Path, PurePosixPath
from typing import Any

from platformdirs import user_cache_path

from .atomic import file_lock

logger = logging.getLogger(__name__)


class ImageJDistributionUnavailableError(RuntimeError):
    """Raised when a declared ImageJ distribution cannot be materialized."""


@dataclass(frozen=True, slots=True)
class ImageJRuntimeArchive:
    """Immutable remote archive that owns its verification and extraction."""

    label: str
    url: str
    sha256: str

    def download_verified_once(
        self,
        *,
        target_directory: Path,
        timeout_seconds: float,
    ) -> Path:
        """Stream and verify this archive once without retry policy."""

        temporary_file = tempfile.NamedTemporaryFile(
            prefix=".imagej-download.",
            suffix=Path(self.url).suffix,
            dir=target_directory,
            delete=False,
        )
        archive_path = Path(temporary_file.name)
        digest = hashlib.sha256()
        try:
            with (
                temporary_file,
                urllib.request.urlopen(
                    self.url,
                    timeout=timeout_seconds,
                ) as response,
            ):
                total_bytes = int(response.headers.get("Content-Length", "0"))
                logger.info(
                    "Downloading %s%s.",
                    self.label,
                    f" ({total_bytes / 1024 / 1024:.0f} MiB)" if total_bytes else "",
                )
                copied_bytes = 0
                reported_decile = 0
                while chunk := response.read(1024 * 1024):
                    temporary_file.write(chunk)
                    digest.update(chunk)
                    copied_bytes += len(chunk)
                    if total_bytes:
                        decile = min(10, copied_bytes * 10 // total_bytes)
                        if decile > reported_decile:
                            logger.info(
                                "Downloading %s: %d%%",
                                self.label,
                                decile * 10,
                            )
                            reported_decile = decile
            actual_sha256 = digest.hexdigest()
            if actual_sha256 != self.sha256:
                raise ImageJDistributionUnavailableError(
                    f"Downloaded {self.label} with SHA-256 {actual_sha256}, "
                    f"expected {self.sha256}."
                )
            return archive_path
        except Exception:
            archive_path.unlink(missing_ok=True)
            raise

    def extract_into(self, archive_path: Path, target_directory: Path) -> None:
        """Extract this verified archive without permitting cache traversal."""

        target_root = target_directory.resolve()
        with zipfile.ZipFile(archive_path) as archive:
            for member in archive.infolist():
                member_path = PurePosixPath(member.filename)
                if (
                    member_path.is_absolute()
                    or ".." in member_path.parts
                    or "\\" in member.filename
                ):
                    raise ImageJDistributionUnavailableError(
                        f"{self.label} contains unsafe member {member.filename!r}."
                    )
                parts = tuple(part for part in member_path.parts if part not in {"", "."})
                if not parts:
                    continue
                output_path = target_directory.joinpath(*parts)
                output_path.parent.mkdir(parents=True, exist_ok=True)
                if not output_path.parent.resolve().is_relative_to(target_root):
                    raise ImageJDistributionUnavailableError(
                        f"{self.label} member escapes the cache: {member.filename!r}."
                    )
                if member.is_dir():
                    output_path.mkdir(parents=True, exist_ok=True)
                    continue
                mode = member.external_attr >> 16
                if stat.S_ISLNK(mode):
                    link_target = archive.read(member).decode()
                    resolved_target = (output_path.parent / link_target).resolve()
                    if not resolved_target.is_relative_to(target_root):
                        raise ImageJDistributionUnavailableError(
                            f"{self.label} symlink escapes the cache: " f"{member.filename!r}."
                        )
                    os.symlink(link_target, output_path)
                    continue
                with archive.open(member) as source, output_path.open("wb") as destination:
                    shutil.copyfileobj(source, destination)
                if mode:
                    output_path.chmod(mode & 0o777)


@dataclass(frozen=True, slots=True)
class ImageJArchiveDownloadPolicy:
    """Retry and timeout policy shared by declared ImageJ archives."""

    timeout_seconds: float = 60.0
    retry_delays_seconds: tuple[float, ...] = (1.0, 2.0, 4.0)

    def download(
        self,
        archive: ImageJRuntimeArchive,
        *,
        target_directory: Path,
    ) -> Path:
        """Download a verified archive under this retry policy."""

        retry_delays = iter(self.retry_delays_seconds)
        while True:
            try:
                return archive.download_verified_once(
                    target_directory=target_directory,
                    timeout_seconds=self.timeout_seconds,
                )
            except (
                http.client.HTTPException,
                ImageJDistributionUnavailableError,
                OSError,
            ) as exc:
                retry_delay = next(retry_delays, None)
                if retry_delay is None:
                    raise ImageJDistributionUnavailableError(
                        f"Could not download {archive.label} from {archive.url}."
                    ) from exc
                logger.warning(
                    "Runtime artifact download failed; retrying %s in %.1f seconds.",
                    archive.label,
                    retry_delay,
                    exc_info=True,
                )
                time.sleep(retry_delay)


@dataclass(frozen=True, slots=True)
class ImageJRuntimeLaunch:
    """Materialized ImageJ directory and the Java runtime shipped with it."""

    imagej_directory: Path
    java_home: Path

    @contextmanager
    def activated_environment(self) -> Iterator[str]:
        """Expose the bundled Java installation only while its JVM starts."""

        previous_java_home = os.environ.get("JAVA_HOME")
        previous_path = os.environ.get("PATH")
        os.environ["JAVA_HOME"] = str(self.java_home)
        java_binary_directory = str(self.java_home / "bin")
        os.environ["PATH"] = (
            os.pathsep.join((java_binary_directory, previous_path))
            if previous_path
            else java_binary_directory
        )
        try:
            yield str(self.imagej_directory)
        finally:
            if previous_java_home is None:
                os.environ.pop("JAVA_HOME", None)
            else:
                os.environ["JAVA_HOME"] = previous_java_home
            if previous_path is None:
                os.environ.pop("PATH", None)
            else:
                os.environ["PATH"] = previous_path


class ImageJDistributionABC(ABC):
    """Nominal owner of one materializable ImageJ distribution."""

    @property
    @abstractmethod
    def label(self) -> str:
        """Return the distribution label used in diagnostics."""

    @abstractmethod
    def materialize(self) -> ImageJRuntimeLaunch:
        """Return a verified local launch of this distribution."""

    @abstractmethod
    def require_compatible_gateway(self, gateway: Any) -> None:
        """Reject a gateway created from a different distribution."""


@dataclass(frozen=True, slots=True)
class _FijiBundleAssetSpec:
    systems: frozenset[str]
    machines: frozenset[str]
    filename: str
    sha256: str


class FijiBundleAsset(Enum):
    """Official Fiji bundle selected by its own host-platform declaration."""

    LINUX_X86_64 = _FijiBundleAssetSpec(
        systems=frozenset({"linux"}),
        machines=frozenset({"x86_64", "amd64"}),
        filename="fiji-latest-linux64-jdk.zip",
        sha256="3330b1b9a6e5dfd0606297cb2e9c4774ff4708782bc185e34d409358ec1faa5e",
    )
    LINUX_ARM64 = _FijiBundleAssetSpec(
        systems=frozenset({"linux"}),
        machines=frozenset({"aarch64", "arm64"}),
        filename="fiji-latest-linux-arm64-jdk.zip",
        sha256="00d24ea9e3268150d0bebba22aa4635dbaabb325252511ddb546c07def45e73a",
    )
    MACOS_X86_64 = _FijiBundleAssetSpec(
        systems=frozenset({"darwin"}),
        machines=frozenset({"x86_64", "amd64"}),
        filename="fiji-latest-macos64-jdk.zip",
        sha256="b82046df1ac8d270c8ff5b358c7d58e5eace9bbfca836a4ce681459e5fc543b6",
    )
    MACOS_ARM64 = _FijiBundleAssetSpec(
        systems=frozenset({"darwin"}),
        machines=frozenset({"aarch64", "arm64"}),
        filename="fiji-latest-macos-arm64-jdk.zip",
        sha256="e66a395160b5affc0c2328accb4782918703918c4b7391a79cfc7300299fea72",
    )
    WINDOWS_X86_64 = _FijiBundleAssetSpec(
        systems=frozenset({"windows"}),
        machines=frozenset({"x86_64", "amd64"}),
        filename="fiji-latest-win64-jdk.zip",
        sha256="5267fc8470c4f87cb948848898cc0dc62f74b69a78f3abf2a8643fe3a4af6c4a",
    )
    WINDOWS_ARM64 = _FijiBundleAssetSpec(
        systems=frozenset({"windows"}),
        machines=frozenset({"aarch64", "arm64"}),
        filename="fiji-latest-win-arm64-jdk.zip",
        sha256="76746beb534408e6e3ce88ff0d8cd86b98f702e6bbf10e780740214937acb2fd",
    )

    @property
    def filename(self) -> str:
        """Return this platform's archive filename."""

        return self.value.filename

    @property
    def sha256(self) -> str:
        """Return this platform's authoritative archive digest."""

        return self.value.sha256

    def archive(
        self,
        *,
        archive_base_url: str,
        label: str,
    ) -> ImageJRuntimeArchive:
        """Project this member into its immutable remote archive."""

        return ImageJRuntimeArchive(
            label=label,
            url=f"{archive_base_url.rstrip('/')}/{self.filename}",
            sha256=self.sha256,
        )

    def matches(self, system: str, machine: str) -> bool:
        """Return whether this member owns the normalized host pair."""

        return system.casefold() in self.value.systems and machine.casefold() in self.value.machines

    @classmethod
    def for_host(cls, system: str, machine: str) -> FijiBundleAsset:
        """Resolve one declared bundle for a host without caller-side dispatch."""

        matches = tuple(member for member in cls if member.matches(system, machine))
        if len(matches) != 1:
            raise ImageJDistributionUnavailableError(
                f"No Fiji bundle is declared for {system!r} on {machine!r}."
            )
        return matches[0]

    @classmethod
    def for_current_host(cls) -> FijiBundleAsset:
        """Resolve the bundle owned by the current host platform."""

        return cls.for_host(platform.system(), platform.machine())


@dataclass(frozen=True, slots=True)
class ImageJRuntimeOverlay(ImageJRuntimeArchive):
    """Checksummed archive that completes one ImageJ runtime classpath."""

    def apply(
        self,
        *,
        imagej_directory: Path,
        download_directory: Path,
        download_policy: ImageJArchiveDownloadPolicy,
    ) -> None:
        """Download, verify, and safely overlay this archive onto ImageJ."""

        archive_path = download_policy.download(
            self,
            target_directory=download_directory,
        )
        try:
            self.extract_into(archive_path, imagej_directory)
        finally:
            archive_path.unlink(missing_ok=True)


@dataclass(frozen=True, slots=True)
class FijiArchiveDistribution(ImageJDistributionABC):
    """Checksummed Fiji application bundle from one immutable official archive."""

    release_id: str
    imagej_version: str
    archive_base_url: str
    runtime_overlays: tuple[ImageJRuntimeOverlay, ...] = ()
    cache_root: Path | None = None
    lock_timeout_seconds: float = 1800.0
    download_policy: ImageJArchiveDownloadPolicy = ImageJArchiveDownloadPolicy()

    @property
    def label(self) -> str:
        """Return the declared Fiji version and immutable archive revision."""

        return f"Fiji {self.imagej_version} archive {self.release_id}"

    def materialize(self) -> ImageJRuntimeLaunch:
        """Download, verify, and atomically cache this host's Fiji bundle."""

        asset = FijiBundleAsset.for_current_host()
        cache_parent = (
            Path(self.cache_root)
            if self.cache_root is not None
            else user_cache_path("polystore") / "imagej"
        )
        cache_parent.mkdir(parents=True, exist_ok=True)
        runtime_digest = hashlib.sha256(
            "".join((asset.sha256, *(overlay.sha256 for overlay in self.runtime_overlays))).encode(
                "ascii"
            )
        ).hexdigest()
        runtime_directory = cache_parent / (
            f"fiji-{self.release_id}-{asset.name.casefold()}-{runtime_digest[:12]}"
        )
        materialization = self._discover_runtime(runtime_directory)
        if materialization is not None:
            return materialization

        lock_path = cache_parent / f".{runtime_directory.name}.lock"
        with file_lock(lock_path, timeout=self.lock_timeout_seconds):
            materialization = self._discover_runtime(runtime_directory)
            if materialization is not None:
                return materialization
            if runtime_directory.exists():
                shutil.rmtree(runtime_directory)
            return self._materialize_locked(asset, cache_parent, runtime_directory)

    def _materialize_locked(
        self,
        asset: FijiBundleAsset,
        cache_parent: Path,
        runtime_directory: Path,
    ) -> ImageJRuntimeLaunch:
        staging_directory = Path(
            tempfile.mkdtemp(prefix=f".{runtime_directory.name}.", dir=cache_parent)
        )
        archive_path: Path | None = None
        try:
            archive = asset.archive(
                archive_base_url=self.archive_base_url,
                label=self.label,
            )
            archive_path = self.download_policy.download(
                archive,
                target_directory=cache_parent,
            )
            logger.info("Extracting %s into the PolyStore runtime cache.", self.label)
            archive.extract_into(archive_path, staging_directory)
            materialization = self._require_runtime(staging_directory)
            for overlay in self.runtime_overlays:
                logger.info("Applying %s to %s.", overlay.label, self.label)
                overlay.apply(
                    imagej_directory=materialization.imagej_directory,
                    download_directory=cache_parent,
                    download_policy=self.download_policy,
                )
            staging_directory.replace(runtime_directory)
            return ImageJRuntimeLaunch(
                imagej_directory=runtime_directory
                / materialization.imagej_directory.relative_to(staging_directory),
                java_home=runtime_directory
                / materialization.java_home.relative_to(staging_directory),
            )
        finally:
            if archive_path is not None:
                archive_path.unlink(missing_ok=True)
            if staging_directory.exists():
                shutil.rmtree(staging_directory)

    def _discover_runtime(self, runtime_directory: Path) -> ImageJRuntimeLaunch | None:
        if not runtime_directory.is_dir():
            return None
        try:
            return self._require_runtime(runtime_directory)
        except ImageJDistributionUnavailableError:
            return None

    def _require_runtime(self, runtime_directory: Path) -> ImageJRuntimeLaunch:
        imagej_directories = tuple(
            candidate
            for candidate in runtime_directory.iterdir()
            if candidate.is_dir()
            and (candidate / "jars").is_dir()
            and (candidate / "plugins").is_dir()
        )
        if len(imagej_directories) != 1:
            raise ImageJDistributionUnavailableError(
                f"Expected one Fiji application in {runtime_directory}, found "
                f"{len(imagej_directories)}."
            )
        imagej_directory = imagej_directories[0]
        java_homes = {
            java_binary.parent.parent.resolve()
            for executable_name in ("java", "java.exe")
            for java_binary in (imagej_directory / "java").rglob(executable_name)
            if java_binary.parent.name == "bin" and java_binary.is_file()
        }
        if len(java_homes) != 1:
            raise ImageJDistributionUnavailableError(
                f"Expected one bundled Java home in {imagej_directory}, found "
                f"{len(java_homes)}."
            )
        return ImageJRuntimeLaunch(
            imagej_directory=imagej_directory.resolve(),
            java_home=java_homes.pop(),
        )

    def require_compatible_gateway(self, gateway: Any) -> None:
        """Reject a live gateway created from a different Fiji release."""

        active_version = str(gateway.getVersion()).partition("/")[0]
        if active_version != self.imagej_version:
            raise ImageJDistributionUnavailableError(
                f"{self.label} is required; the active gateway is ImageJ "
                f"{active_version}. Start this runtime in a fresh process."
            )


FIJI_IMAGEJ_DISTRIBUTION = FijiArchiveDistribution(
    release_id="20260718-0417",
    imagej_version="2.18.0",
    archive_base_url=("https://downloads.imagej.net/fiji/archive/latest/20260718-0417"),
    runtime_overlays=(
        ImageJRuntimeOverlay(
            label="PyImageJ 1.7.0 Fiji bridge overlay",
            url=(
                "https://github.com/OpenHCSDev/PolyStore/releases/download/"
                "imagej-runtime-20260718-0417-pyimagej-1.7.0/"
                "pyimagej-1.7.0-fiji-overlay.zip"
            ),
            sha256="210d1ebb2c1f1ede85d0e94e741a772e07cfb9233992ae4e85cd245b06890345",
        ),
    ),
)

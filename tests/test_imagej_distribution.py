"""Tests for checksummed, host-owned ImageJ distribution materialization."""

from __future__ import annotations

import os
import shutil
import stat
import zipfile
from pathlib import Path
from types import SimpleNamespace

import pytest

from polystore.imagej_distribution import (
    FijiArchiveDistribution,
    FijiBundleAsset,
    ImageJArchiveDownloadPolicy,
    ImageJDistributionUnavailableError,
    ImageJRuntimeArchive,
    ImageJRuntimeLaunch,
    ImageJRuntimeOverlay,
)


@pytest.mark.parametrize(
    ("system", "machine", "expected"),
    (
        ("Linux", "x86_64", FijiBundleAsset.LINUX_X86_64),
        ("Linux", "aarch64", FijiBundleAsset.LINUX_ARM64),
        ("Darwin", "x86_64", FijiBundleAsset.MACOS_X86_64),
        ("Darwin", "arm64", FijiBundleAsset.MACOS_ARM64),
        ("Windows", "AMD64", FijiBundleAsset.WINDOWS_X86_64),
        ("Windows", "ARM64", FijiBundleAsset.WINDOWS_ARM64),
    ),
)
def test_fiji_bundle_member_owns_host_selection(
    system: str,
    machine: str,
    expected: FijiBundleAsset,
) -> None:
    assert FijiBundleAsset.for_host(system, machine) is expected


def test_fiji_bundle_rejects_an_undeclared_host() -> None:
    with pytest.raises(ImageJDistributionUnavailableError, match="No Fiji bundle"):
        FijiBundleAsset.for_host("Plan9", "mips")


def test_runtime_launch_scopes_bundled_java_environment(monkeypatch, tmp_path) -> None:
    launch = ImageJRuntimeLaunch(
        imagej_directory=tmp_path / "Fiji",
        java_home=tmp_path / "jdk",
    )
    monkeypatch.setenv("JAVA_HOME", "/existing/java")
    monkeypatch.setenv("PATH", "/existing/bin")

    with launch.activated_environment() as target:
        assert target == str(tmp_path / "Fiji")
        assert os.environ["JAVA_HOME"] == str(tmp_path / "jdk")
        assert os.environ["PATH"].split(os.pathsep) == [
            str(tmp_path / "jdk" / "bin"),
            "/existing/bin",
        ]

    assert os.environ["JAVA_HOME"] == "/existing/java"
    assert os.environ["PATH"] == "/existing/bin"


def test_runtime_launch_does_not_add_an_empty_path_entry(monkeypatch, tmp_path) -> None:
    launch = ImageJRuntimeLaunch(
        imagej_directory=tmp_path / "Fiji",
        java_home=tmp_path / "jdk",
    )
    monkeypatch.delenv("JAVA_HOME", raising=False)
    monkeypatch.delenv("PATH", raising=False)

    with launch.activated_environment():
        assert os.environ["PATH"] == str(tmp_path / "jdk" / "bin")

    assert "JAVA_HOME" not in os.environ
    assert "PATH" not in os.environ


def test_verified_download_rejects_wrong_content(tmp_path) -> None:
    source = tmp_path / "source.zip"
    source.write_bytes(b"not the declared archive")
    archive = ImageJRuntimeArchive(
        label="test Fiji",
        url=source.as_uri(),
        sha256="0" * 64,
    )

    with pytest.raises(ImageJDistributionUnavailableError, match="SHA-256"):
        archive.download_verified_once(
            target_directory=tmp_path,
            timeout_seconds=1.0,
        )

    assert not tuple(tmp_path.glob(".imagej-download.*"))


def test_runtime_overlay_owns_verified_materialization(monkeypatch, tmp_path) -> None:
    overlay_archive = tmp_path / "overlay.zip"
    with zipfile.ZipFile(overlay_archive, "w") as archive:
        archive.writestr("jars/bridge.jar", b"bridge")
        archive.writestr("licenses/bridge.txt", b"license")
    imagej_directory = tmp_path / "Fiji"
    imagej_directory.mkdir()

    def provide_overlay(_policy, _archive, *, target_directory: Path) -> Path:
        target = tmp_path / "download.zip"
        shutil.copyfile(overlay_archive, target)
        return target

    monkeypatch.setattr(
        ImageJArchiveDownloadPolicy,
        "download",
        provide_overlay,
    )
    overlay = ImageJRuntimeOverlay(
        label="test overlay",
        url="https://example.invalid/overlay.zip",
        sha256="1" * 64,
    )

    overlay.apply(
        imagej_directory=imagej_directory,
        download_directory=tmp_path,
        download_policy=ImageJArchiveDownloadPolicy(retry_delays_seconds=()),
    )

    assert (imagej_directory / "jars/bridge.jar").read_bytes() == b"bridge"
    assert (imagej_directory / "licenses/bridge.txt").read_bytes() == b"license"
    assert not (tmp_path / "download.zip").exists()


def test_safe_zip_extraction_preserves_relative_symlinks(tmp_path) -> None:
    archive_path = tmp_path / "runtime.zip"
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr("Fiji/jars/core.jar", b"jar")
        link = zipfile.ZipInfo("Fiji/jars/core-link.jar")
        link.create_system = 3
        link.external_attr = (stat.S_IFLNK | 0o777) << 16
        archive.writestr(link, "core.jar")

    target = tmp_path / "target"
    target.mkdir()
    runtime_archive = ImageJRuntimeArchive(
        label="test runtime",
        url=archive_path.as_uri(),
        sha256="0" * 64,
    )
    runtime_archive.extract_into(archive_path, target)

    assert (target / "Fiji/jars/core-link.jar").is_symlink()
    assert (target / "Fiji/jars/core-link.jar").read_bytes() == b"jar"


def test_safe_zip_extraction_rejects_escaping_members(tmp_path) -> None:
    archive_path = tmp_path / "unsafe.zip"
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr("../escape", b"unsafe")

    target = tmp_path / "target"
    target.mkdir()
    runtime_archive = ImageJRuntimeArchive(
        label="test runtime",
        url=archive_path.as_uri(),
        sha256="0" * 64,
    )
    with pytest.raises(ImageJDistributionUnavailableError, match="unsafe member"):
        runtime_archive.extract_into(archive_path, target)


def test_safe_zip_extraction_rejects_backslash_traversal(tmp_path) -> None:
    archive_path = tmp_path / "unsafe.zip"
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr(r"..\escape", b"unsafe")

    target = tmp_path / "target"
    target.mkdir()
    runtime_archive = ImageJRuntimeArchive(
        label="test runtime",
        url=archive_path.as_uri(),
        sha256="0" * 64,
    )
    with pytest.raises(ImageJDistributionUnavailableError, match="unsafe member"):
        runtime_archive.extract_into(archive_path, target)


def test_runtime_layout_discovers_one_fiji_and_java_home(tmp_path) -> None:
    imagej_directory = tmp_path / "Fiji"
    (imagej_directory / "jars").mkdir(parents=True)
    (imagej_directory / "plugins").mkdir()
    java_binary = imagej_directory / "java/linux64/jdk/bin/java"
    java_binary.parent.mkdir(parents=True)
    java_binary.write_bytes(b"java")

    distribution = FijiArchiveDistribution(
        release_id="test-release",
        imagej_version="2.18.0",
        archive_base_url="https://example.invalid/fiji",
    )
    launch = distribution._require_runtime(tmp_path)

    assert launch.imagej_directory == imagej_directory.resolve()
    assert launch.java_home == java_binary.parent.parent.resolve()


def test_distribution_materializes_once_into_digest_addressed_cache(
    monkeypatch,
    tmp_path,
) -> None:
    source_archive = tmp_path / "source.zip"
    with zipfile.ZipFile(source_archive, "w") as archive:
        archive.writestr("Fiji/jars/core.jar", b"jar")
        archive.writestr("Fiji/plugins/plugin.jar", b"plugin")
        archive.writestr("Fiji/java/linux64/jdk/bin/java", b"java")
    download_count = 0

    def provide_archive(_policy, _archive, *, target_directory: Path) -> Path:
        nonlocal download_count
        download_count += 1
        target = target_directory / f"download-{download_count}.zip"
        shutil.copyfile(source_archive, target)
        return target

    monkeypatch.setattr(
        ImageJArchiveDownloadPolicy,
        "download",
        provide_archive,
    )
    monkeypatch.setattr(
        "polystore.imagej_distribution.platform.system",
        lambda: "Linux",
    )
    monkeypatch.setattr(
        "polystore.imagej_distribution.platform.machine",
        lambda: "x86_64",
    )
    distribution = FijiArchiveDistribution(
        release_id="test-release",
        imagej_version="2.18.0",
        archive_base_url="https://example.invalid/fiji",
        cache_root=tmp_path / "cache",
        download_policy=ImageJArchiveDownloadPolicy(retry_delays_seconds=()),
    )

    first = distribution.materialize()
    second = distribution.materialize()

    assert first == second
    assert download_count == 1
    assert first.imagej_directory.name == "Fiji"
    assert first.java_home.name == "jdk"


def test_distribution_rejects_a_different_live_imagej_version() -> None:
    distribution = FijiArchiveDistribution(
        release_id="test-release",
        imagej_version="2.18.0",
        archive_base_url="https://example.invalid/fiji",
    )

    with pytest.raises(
        ImageJDistributionUnavailableError,
        match="active gateway is ImageJ 2.17.0",
    ):
        distribution.require_compatible_gateway(SimpleNamespace(getVersion=lambda: "2.17.0/1.54p"))

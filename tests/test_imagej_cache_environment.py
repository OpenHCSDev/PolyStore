"""Bundle-only configuration survives independent process cache policies."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import zipfile
from pathlib import Path

import pytest

from polystore.imagej_distribution import (
    FijiArchiveDistribution,
    ImageJArchiveDownloadPolicy,
    ImageJDistributionUnavailableError,
    ImageJRuntimeArchive,
)


@pytest.mark.parametrize("value, allowed", ((None, True), ("true", True), (" FALSE ", False)))
def test_download_policy_decodes_permission_without_copying_retry_defaults(value, allowed):
    environment = {} if value is None else {
        ImageJArchiveDownloadPolicy.allow_download_environment_key: value
    }
    policy = ImageJArchiveDownloadPolicy.from_environment(environment)
    assert policy.allow_download is allowed
    default = ImageJArchiveDownloadPolicy()
    assert policy.timeout_seconds == default.timeout_seconds
    assert policy.retry_delays_seconds == default.retry_delays_seconds


@pytest.mark.parametrize("value", ("", "yes", "0", "relative/cache"))
def test_invalid_download_permission_fails(value):
    with pytest.raises(ImageJDistributionUnavailableError, match="true or false"):
        ImageJArchiveDownloadPolicy.from_environment({
            ImageJArchiveDownloadPolicy.allow_download_environment_key: value
        })


def test_forbidden_download_has_no_archive_or_cache_side_effects(monkeypatch, tmp_path):
    def forbidden_io(*args, **kwargs):
        pytest.fail("Forbidden policy reached archive IO")

    monkeypatch.setattr(ImageJRuntimeArchive, "download_verified_once", forbidden_io)
    root = tmp_path / "absent-cache"
    policy = ImageJArchiveDownloadPolicy(allow_download=False)
    with pytest.raises(ImageJDistributionUnavailableError, match="forbidden"):
        policy.download(
            ImageJRuntimeArchive("fixture", "https://invalid.example/fixture.zip", "0" * 64),
            target_directory=root,
        )
    distribution = FijiArchiveDistribution(
        release_id="fixture", imagej_version="fixture", archive_base_url="https://invalid.example",
        cache_root=root, download_policy=policy,
    )
    with pytest.raises(ImageJDistributionUnavailableError, match="forbidden"):
        distribution.materialize()
    assert not root.exists()


def test_unset_bundle_root_retains_distribution_default():
    assert FijiArchiveDistribution.cache_root_from_environment({}) is None


@pytest.mark.parametrize("value", ("", " ", "relative/cache"))
def test_invalid_explicit_bundle_root_fails_without_a_fallback(value):
    with pytest.raises(ImageJDistributionUnavailableError, match="absolute bundle-cache"):
        FijiArchiveDistribution.cache_root_from_environment(
            {FijiArchiveDistribution.cache_root_environment_key: value}
        )


def test_bundle_root_decode_is_independent_of_xdg_cache(tmp_path):
    root = tmp_path / "bundles"
    for run in ("first", "second"):
        assert (
            FijiArchiveDistribution.cache_root_from_environment(
                {
                    FijiArchiveDistribution.cache_root_environment_key: str(root),
                    "XDG_CACHE_HOME": str(tmp_path / run),
                }
            )
            == root
        )


def test_fresh_processes_share_one_verified_bundle_not_their_xdg_caches(tmp_path):
    archive = tmp_path / "tiny-fiji.zip"
    with zipfile.ZipFile(archive, "w") as fixture:
        fixture.writestr("Fiji/jars/core.jar", b"verified fixture")
        fixture.writestr("Fiji/plugins/plugin.jar", b"fixture")
        fixture.writestr("Fiji/java/linux64/jdk/bin/java", b"no JVM execution")
    digest = hashlib.sha256(archive.read_bytes()).hexdigest()
    root = tmp_path / "shared-bundles"
    helper = Path(__file__).with_name("imagej_cache_process_fixture.py")
    results = []
    for run in ("first", "second"):
        isolated = tmp_path / run
        isolated.mkdir()
        (isolated / "run.log").write_text(run)
        environment = dict(os.environ)
        environment[FijiArchiveDistribution.cache_root_environment_key] = str(root)
        environment["XDG_CACHE_HOME"] = str(isolated)
        child = subprocess.run(
            [sys.executable, str(helper), str(archive), digest],
            env=environment,
            capture_output=True,
            text=True,
            check=True,
            timeout=10,
        )
        result = json.loads(child.stdout)
        assert result["cache_root"] == str(root)
        assert result["xdg_cache_home"] == str(isolated)
        assert result["source"] == str(
            Path(__file__).resolve().parents[1] / "src/polystore/imagej_distribution.py"
        )
        assert (isolated / "run.log").read_text() == run
        assert not (isolated / "polystore/imagej").exists()
        results.append(result)
    assert [result["downloads"] for result in results] == [1, 0]
    assert results[0]["imagej_directory"] == results[1]["imagej_directory"]
    assert results[0]["java_home"] == results[1]["java_home"]
    assert len(tuple(root.glob("fiji-*"))) == 1
    assert not tuple(root.glob(".imagej-download.*"))

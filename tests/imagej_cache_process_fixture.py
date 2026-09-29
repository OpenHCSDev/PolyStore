"""Tiny verified artifact response for fresh-process bundle-cache tests; no JVM."""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass, field, replace
from pathlib import Path
from unittest.mock import patch

import polystore.imagej_distribution as distribution_module
from polystore.imagej_distribution import (
    FIJI_IMAGEJ_DISTRIBUTION,
    FijiBundleAsset,
    ImageJArchiveDownloadPolicy,
    ImageJRuntimeArchive,
)
from polystore.imagej_runtime import FIJI_IMAGEJ_RUNTIME


@dataclass(frozen=True)
class FixtureAsset:
    source: Path
    sha256: str
    name: str = "LOCAL_FIXTURE"

    def archive(self, *, archive_base_url, label):
        return ImageJRuntimeArchive(label, self.source.as_uri(), self.sha256)


@dataclass(frozen=True)
class CountedDownloadPolicy(ImageJArchiveDownloadPolicy):
    downloaded: list[str] = field(default_factory=list)

    def download(self, archive, *, target_directory):
        self.downloaded.append(archive.sha256)
        return super().download(archive, target_directory=target_directory)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("archive", type=Path)
    parser.add_argument("sha256")
    args = parser.parse_args()
    assert FIJI_IMAGEJ_RUNTIME.distribution is FIJI_IMAGEJ_DISTRIBUTION
    policy = CountedDownloadPolicy(retry_delays_seconds=())
    # Only the external artifact/host response is controlled. Canonical root,
    # checksum verification, cache locking/staging/discovery remain production.
    distribution = replace(
        FIJI_IMAGEJ_RUNTIME.distribution, runtime_overlays=(), download_policy=policy
    )
    with patch.object(
        FijiBundleAsset, "for_current_host", return_value=FixtureAsset(args.archive, args.sha256)
    ):
        launch = distribution.materialize()
    print(
        json.dumps(
            {
                "cache_root": str(FIJI_IMAGEJ_DISTRIBUTION.cache_root),
                "imagej_directory": str(launch.imagej_directory),
                "java_home": str(launch.java_home),
                "downloads": len(policy.downloaded),
                "xdg_cache_home": os.environ["XDG_CACHE_HOME"],
                "source": str(Path(distribution_module.__file__).resolve()),
            }
        )
    )


if __name__ == "__main__":
    main()

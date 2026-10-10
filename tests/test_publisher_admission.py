"""Release tests use the checkout; the built distribution owns its metadata."""

import shlex
import tomllib
import zipfile
from email.parser import Parser
from pathlib import Path

from packaging.requirements import Requirement
from setuptools import build_meta


ROOT = Path(__file__).resolve().parents[1]


def test_publisher_installs_the_dev_candidate_editably():
    workflow = (ROOT / ".github/workflows/publish.yml").read_text(encoding="utf-8")
    commands = [shlex.split(line.strip()) for line in workflow.splitlines()]
    (install,) = [command for command in commands if ".[dev]" in command]
    assert install[:4] == ["python", "-m", "pip", "install"]
    assert install[install.index("-e") + 1] == ".[dev]"
    assert "run: python -m pytest -q" in workflow


def test_editable_artifact_owns_checkout_path_and_roi_dev_requirement(tmp_path, monkeypatch):
    monkeypatch.chdir(ROOT)
    wheel = build_meta.build_editable(str(tmp_path))
    with zipfile.ZipFile(tmp_path / wheel) as archive:
        (pth,) = [name for name in archive.namelist() if name.endswith(".pth")]
        assert Path(archive.read(pth).decode().strip()).resolve() == ROOT / "src"
        (metadata_name,) = [name for name in archive.namelist() if name.endswith("/METADATA")]
        metadata = Parser().parsestr(archive.read(metadata_name).decode())
    project = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))["project"]
    assert metadata["Name"] == project["name"]
    assert metadata["Version"] == project["version"]
    requirements = [Requirement(value) for value in metadata.get_all("Requires-Dist")]
    (roi,) = [requirement for requirement in requirements if requirement.name == "roifile"]
    assert roi.marker.evaluate({"extra": "dev"})
    assert not roi.marker.evaluate({"extra": ""})

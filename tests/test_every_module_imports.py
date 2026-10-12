"""Every PolyStore module imports against the installed first-party libraries.

The imports run in a child interpreter: importing every module in the test
process would leave process-wide state (ImageJ environment, registries) behind
for the tests that follow.
"""

import json
import subprocess
import sys

FIRST_PARTY = ("polystore", "arraybridge", "metaclass_registry", "zmqruntime")

IMPORT_EVERY_MODULE = """
import importlib, json, pkgutil, traceback
import polystore

report = {"imported": [], "optional_missing": {}, "failed": {}}
for module in pkgutil.walk_packages(polystore.__path__, prefix="polystore."):
    try:
        importlib.import_module(module.name)
    except ModuleNotFoundError as error:
        missing = (error.name or "").split(".")[0]
        kind = "failed" if missing in FIRST_PARTY else "optional_missing"
        report[kind][module.name] = traceback.format_exc() if kind == "failed" else missing
    except Exception:
        report["failed"][module.name] = traceback.format_exc()
    else:
        report["imported"].append(module.name)
print(json.dumps(report))
"""


def test_every_module_imports() -> None:
    completed = subprocess.run(
        [sys.executable, "-c", f"FIRST_PARTY = {FIRST_PARTY!r}\n{IMPORT_EVERY_MODULE}"],
        capture_output=True,
        text=True,
        check=True,
    )
    report = json.loads(completed.stdout.splitlines()[-1])

    # A missing optional third-party package is not a PolyStore fault; a
    # missing first-party module or name, or any other error, always is.
    assert report["failed"] == {}
    assert {"polystore.constants", "polystore.array_payload"} <= set(report["imported"])

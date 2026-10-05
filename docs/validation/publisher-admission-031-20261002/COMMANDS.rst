Focused qualification command
=============================

Run from the isolated PolyStore worktree::

  systemd-run --user --scope --quiet \
    -p MemoryMax=512M -p MemorySwapMax=0 -p CPUQuota=100% \
    timeout 60s taskset -c 0 env \
    PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 PYTHONDONTWRITEBYTECODE=1 \
    OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 \
    PYTHONPATH=/home/ts/wt/polystore-publisher-admission-031-20261002/src \
    /home/ts/wt/openhcs-generated-inputs-installed-parent-20261001/.venv/bin/python -B \
    /home/ts/wt/openhcs-prior-measurement-role-projection-20261001/docs/validation/selected_plane_stream_materialization_20261001/run_bounded_source.py \
    /home/ts/wt/openhcs-generated-inputs-installed-parent-20261001/.venv/bin/python -B -m pytest \
    -o addopts= \
    --basetemp=docs/validation/publisher-admission-031-20261002/qualified-test-temp \
    --junitxml=docs/validation/publisher-admission-031-20261002/qualified.xml \
    tests/test_publisher_admission.py tests/test_basic.py \
    tests/test_imagej_cache_environment.py tests/test_metadata_namespace.py \
    tests/test_roi_region_properties.py tests/test_roi.py \
    tests/test_lazy_package_exports.py -q

The default coverage/report addopts are disabled for this focused resource-bound
source run, not for the publisher. All selected tests/assertions are unchanged.
Source fixtures prohibit actual Fiji downloads/JVM execution.

Offline lock attempt (terminal failure)::

  systemd-run --user --scope --quiet \
    -p MemoryMax=512M -p MemorySwapMax=0 -p CPUQuota=100% \
    timeout 60s taskset -c 0 uv lock --offline --no-python-downloads

No solution for python_full_version >=3.14 / win32: imglyb not found in cache;
polystore[all] requires imglyb==2.1.0. No lock file was changed.

Owned disposable output is confined to the three source-test-temp,
admission-test-temp and qualified-test-temp directories below this receipt root.
Original log, test XML and qualification notes are durable evidence, not caches.

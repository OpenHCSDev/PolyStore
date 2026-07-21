# Test coverage improvement plan

Goal: raise coverage from ~24% to 85%+ by adding small, high-leverage, parameterized tests that exercise the FileManager router and backend implementations.

Scope and strategy
- Focus first on the low-friction, high-return surface:
  - `FileManager` (router) — exercises many backend code paths with little test code.
  - `DiskBackend` and `MemoryBackend` — implement core IO, listing, formats.
  - Add `Zarr` tests later when `zarr`/ome-zarr deps are available.

Approach
- Keep tests small and DRY using pytest fixtures and parameterization.
- Use `registry` fixture that supplies a minimal backend mapping (`disk`, `memory`).
- Provide a `FileManager` fixture that uses that registry so tests exercise the router.
- For each backend run the same set of assertions: save/load, batch ops, exists/ensure_directory, list_files, natural sorting for images, and error propagation for unknown backends.

Files to add (first wave)
- `tests/conftest.py` — fixtures: `registry`, `file_manager`, `sample_payloads`.
- `tests/test_filemanager_backends.py` — parameterized tests for `disk` and `memory`.

Next steps (after this wave)
- Expand disk-specific tests for CSV/JSON/Text handlers and symlink behavior.
- Add gated `zarr` tests using `pytest.importorskip('zarr')` and small synthetic stores.
- Add registry and cleanup tests to exercise lazy instantiation and cleanup paths.

How I'll validate
- Run the new tests and iteratively fix test issues.
- Run `pytest --cov=polystore --cov-report=term-missing` to monitor coverage improvements.

Notes
- Tests intentionally avoid heavy external deps (ome-zarr, tifffile) in the initial wave. These will be added behind `importorskip` so CI remains lightweight.

Author: automated scaffolding
Date: 2025-11-02

# Polystore Extraction Status

## ✅ Completed

### Repository Setup
- [x] Created local git repository at `/home/ts/code/projects/polystore`
- [x] Initialized with `main` branch
- [x] Created directory structure following arraybridge/metaclass-registry pattern

### Files Copied from OpenHCS
- [x] `atomic.py` - Cross-platform atomic file operations
- [x] `exceptions.py` - Storage exceptions
- [x] `base.py` - Abstract interfaces (DataSink, DataSource, StorageBackend)
- [x] `memory.py` - In-memory backend
- [x] `disk.py` - Disk storage backend with multi-format support
- [x] `zarr.py` - Zarr/OME-Zarr backend
- [x] `filemanager.py` - High-level file manager API
- [x] `streaming.py` - ZeroMQ streaming backend base class
- [x] `backend_registry.py` - Auto-registration system

### CI/CD Setup
- [x] Copied `coverage-pages.yml` workflow
- [x] Copied `publish.yml` workflow
- [x] Updated workflows for polystore (changed openhcs → polystore)
- [x] Configured GitHub Pages deployment
- [x] Configured PyPI publishing on version tags

### Documentation Setup
- [x] Copied Sphinx configuration from OpenHCS
- [x] Updated `conf.py` for polystore
- [x] Created `index.rst` with basic documentation
- [x] Copied static assets

### Package Configuration
- [x] Created `pyproject.toml` with:
  - Core dependencies: numpy, portalocker
  - Optional dependencies: zarr, torch, jax, tensorflow, cupy, streaming
  - Dev dependencies: pytest, coverage, black, ruff
  - Docs dependencies: sphinx, sphinx-rtd-theme
- [x] Created `__init__.py` with exports
- [x] Created `README.md` with features and examples
- [x] Created `LICENSE` (MIT)
- [x] Created `.gitignore`

### Testing
- [x] Created `tests/test_basic.py` with basic smoke tests
- [x] Configured pytest in `pyproject.toml`

### Initial Commit
- [x] Committed all files with descriptive message

## 🚧 Next Steps (Refactoring Required)

### 1. Remove OpenHCS Dependencies

The following files have imports from `openhcs.*` that need to be refactored:

#### `base.py`
- [ ] Replace `from openhcs.constants.constants import Backend` → Use string literals or create generic enum
- [ ] Replace `from openhcs.core.auto_register_meta import AutoRegisterMeta` → Copy to polystore or depend on metaclass-registry

#### `disk.py`
- [ ] Replace `from openhcs.constants.constants import FileFormat, Backend` → Create generic format registry
- [ ] Replace `from openhcs.core.lazy_gpu_imports import ...` → Move to polystore

#### `zarr.py`
- [ ] Replace `from openhcs.core.config import ZarrConfig, TransportMode` → Move to polystore.config
- [ ] Replace `from openhcs.constants.constants import Backend` → Use string literal

#### `filemanager.py`
- [ ] Replace `from openhcs.constants.constants import DEFAULT_IMAGE_EXTENSIONS` → Make parameter with default
- [ ] Replace `from openhcs.core.utils import natural_sort` → Move to polystore.utils
- [ ] Remove `_materialization_context` (OpenHCS-specific)

#### `streaming.py`
- [ ] Replace `from openhcs.runtime.zmq_base import get_zmq_transport_url` → Move to polystore
- [ ] Replace `from openhcs.core.config import TransportMode` → Move to polystore.config
- [ ] Replace `from openhcs.core.roi import ROI` → Make optional (OpenHCS-specific)
- [ ] Replace `from openhcs.runtime.queue_tracker import ...` → Make optional (OpenHCS-specific)
- [ ] Replace `from openhcs.constants.streaming import StreamingDataType` → Move to polystore

#### `backend_registry.py`
- [ ] Replace `from openhcs.core.auto_register_meta import AutoRegisterMeta` → Copy to polystore

### 2. Create New Files

- [ ] `src/polystore/config.py` - ZarrConfig, TransportMode
- [ ] `src/polystore/utils.py` - natural_sort, get_zmq_transport_url
- [ ] `src/polystore/formats.py` - Generic FileFormat registry
- [ ] `src/polystore/registry.py` - AutoRegisterMeta (or depend on metaclass-registry)
- [ ] `src/polystore/lazy_imports.py` - Lazy GPU framework imports

### 3. GitHub Repository

- [ ] Create GitHub repo: `trissim/polystore`
- [ ] Add remote: `git remote add origin https://github.com/trissim/polystore.git`
- [ ] Push initial commit: `git push -u origin main`
- [ ] Enable GitHub Pages in repo settings (Source: GitHub Actions)
- [ ] Add PyPI API token to GitHub secrets (`PYPI_API_TOKEN`)

### 4. Documentation

- [ ] Create `docs/source/installation.rst`
- [ ] Create `docs/source/quickstart.rst`
- [ ] Create `docs/source/api/index.rst`
- [ ] Create `docs/source/api/backends.rst`
- [ ] Create `docs/source/api/filemanager.rst`
- [ ] Set up Read the Docs integration

### 5. Testing

- [ ] Write comprehensive unit tests for each backend
- [ ] Write integration tests for FileManager
- [ ] Write tests for atomic operations
- [ ] Write tests for format detection
- [ ] Set up CI to run tests on multiple Python versions

### 6. OpenHCS Integration

- [ ] Update OpenHCS `pyproject.toml` to depend on `polystore>=0.1.0`
- [ ] Update OpenHCS `openhcs/io/__init__.py` to re-export polystore
- [ ] Keep OpenHCS-specific backends in `openhcs/io/`:
  - `omero_local.py`
  - `napari_stream.py`
  - `fiji_stream.py`
  - `virtual_workspace.py`
  - `metadata_writer.py`
  - `metadata_migration.py`
  - `pipeline_migration.py`

### 7. Addon Packages (Future)

- [ ] Create `polystore-napari` package (extends StreamingBackend)
- [ ] Create `polystore-fiji` package (extends StreamingBackend)
- [ ] Create `polystore-omero` package (extends ReadOnlyBackend)

## 📊 File Status

| File | Copied | Needs Refactoring | Notes |
|------|--------|-------------------|-------|
| `atomic.py` | ✅ | ❌ | Zero dependencies - ready to use |
| `exceptions.py` | ✅ | ❌ | Zero dependencies - ready to use |
| `base.py` | ✅ | ✅ | Replace Backend enum, AutoRegisterMeta |
| `memory.py` | ✅ | ✅ | Replace Backend enum |
| `disk.py` | ✅ | ✅ | Replace FileFormat, Backend, lazy imports |
| `zarr.py` | ✅ | ✅ | Move ZarrConfig, replace Backend |
| `filemanager.py` | ✅ | ✅ | Move natural_sort, remove _materialization_context |
| `streaming.py` | ✅ | ✅ | Move zmq utils, make ROI/queue_tracker optional |
| `backend_registry.py` | ✅ | ✅ | Copy AutoRegisterMeta |

## 🎯 Immediate Next Action

**You need to:**
1. Create the GitHub repository manually at https://github.com/new
   - Name: `polystore`
   - Description: "Framework-agnostic multi-backend storage abstraction for ML and scientific computing"
   - Public
   - No README/LICENSE/gitignore (we already have them)

2. Then run:
   ```bash
   cd /home/ts/code/projects/polystore
   git remote add origin https://github.com/trissim/polystore.git
   git push -u origin main
   ```

3. After that, we'll tackle the refactoring to remove OpenHCS dependencies.

## 📝 Notes

- **streaming.py included**: As you suggested, streaming.py is included as a generic base class. The OpenHCS-specific parts (ROI, queue_tracker) will be made optional.
- **virtual_workspace.py excluded**: This is OpenHCS-specific (uses openhcs_metadata.json format) and stays in OpenHCS.
- **Addon strategy**: napari/fiji/omero can be separate addon packages that extend polystore backends.


# Test Coverage Expansion Plan

## Summary
Added comprehensive test suites to improve coverage for `disk.py` and `memory.py` backends. Two new test files were created with deep coverage of edge cases, error conditions, and advanced operations.

## New Test Files

### 1. `tests/test_disk_coverage.py`
Comprehensive tests for DiskBackend with ~450+ lines covering:

**Test Classes:**
- **TestDiskBatchOperations** - Batch save/load error handling
  - Length mismatch detection
  - Multiple file operations
  - Various data format handling

- **TestDiskListingOperations** - File listing with filters
  - Non-existent directory handling
  - Pattern matching with glob
  - Extension filtering (case-insensitive)
  - Recursive vs non-recursive with breadth-first ordering
  - Empty directory handling

- **TestDiskDeleteOperations** - Delete and delete_all
  - File deletion
  - Empty/non-empty directory handling
  - Recursive deletion
  - Error cases

- **TestDiskPathOperations** - Path existence and type checking
  - exists() on various path types
  - is_file() with proper error handling
  - is_dir() with proper error handling
  - Error boundaries

- **TestDiskStatOperation** - Metadata retrieval
  - File metadata
  - Directory metadata
  - Symlink metadata (including broken links)
  - Missing path handling

- **TestDiskCopyAndMove** - File system operations
  - Basic copy/move
  - Cross-directory operations
  - Overwrite prevention
  - Directory tree copying
  - Error cases

- **TestDiskSymlinkAdvanced** - Advanced symlink operations
  - Directory symlinks
  - Non-existent source handling
  - Overwrite behavior
  - Regular file vs symlink detection

- **TestDiskErrorHandling** - Error conditions
  - Unregistered extensions
  - Directory creation idempotence
  - Nested directory creation

### 2. `tests/test_memory_coverage.py`
Comprehensive tests for MemoryBackend with ~600+ lines covering:

**Test Classes:**
- **TestMemoryCopyAndMove** - In-memory copy/move operations
  - Basic file operations
  - Cross-directory moves
  - Error conditions (nonexistent, existing dest)
  - Directory operations

- **TestMemorySymlink** - Symlink operations
  - Basic symlink creation
  - Non-existent target handling
  - Overwrite behavior
  - Type detection

- **TestMemoryStat** - Metadata for memory paths
  - File, directory, symlink, missing path metadata
  - Broken symlink handling
  - Type classification

- **TestMemoryDeleteOperations** - Delete operations
  - File deletion
  - Empty/non-empty directory handling
  - Recursive deletion (delete_all)
  - Error cases

- **TestMemoryClearFilesOnly** - Selective clearing
  - Directory preservation
  - File removal
  - Symlink removal
  - GPU object cleanup

- **TestMemoryIsGPUObject** - GPU detection
  - PyTorch CUDA tensors
  - CuPy arrays
  - CPU arrays
  - Regular objects

- **TestMemoryListingOperations** - Directory listing
  - Extension filtering
  - Pattern matching
  - Recursive vs non-recursive
  - Direct children vs nested
  - Error cases

- **TestMemoryEdgeCases** - Edge cases and boundaries
  - Parent directory validation
  - Existing path overwrite prevention
  - Path normalization consistency
  - File vs directory boundaries
  - Invalid intermediate paths

## Coverage Improvements

### disk.py Areas Now Covered:
- ✅ Batch operations with validation
- ✅ Listing with multiple filter types (pattern, extensions, recursive)
- ✅ Breadth-first traversal ordering
- ✅ Delete operations (single and recursive)
- ✅ Path metadata (stat)
- ✅ Copy and move operations
- ✅ Symlink creation and detection
- ✅ Error handling for various edge cases
- ✅ Directory operations and creation
- ✅ Extension validation and format registry

### memory.py Areas Now Covered:
- ✅ Move operations (basic and cross-directory)
- ✅ Copy operations (files and directories with deep copy)
- ✅ Symlink operations (creation, detection, metadata)
- ✅ stat() method for all path types
- ✅ Delete operations (single and recursive)
- ✅ clear_files_only() with GPU handling
- ✅ GPU object detection and cleanup
- ✅ Listing operations with filters
- ✅ Path normalization and consistency
- ✅ Boundary conditions between file/directory operations
- ✅ Error cases and edge conditions

## Key Testing Patterns Used

1. **Setup Method Pattern** - Each class uses `setup_method()` for test isolation
2. **Error Boundary Testing** - Explicit tests for error conditions with `pytest.raises()`
3. **Mock Usage** - GPU object detection tests use mocks to simulate GPU tensors
4. **Deep Assertions** - NumPy arrays compared with `np.testing.assert_array_equal()`
5. **Isolation** - Files use temporary directories to avoid interference

## Running the Tests

```bash
# Run all new coverage tests
pytest tests/test_disk_coverage.py tests/test_memory_coverage.py -v

# Run with coverage report
pytest tests/test_disk_coverage.py tests/test_memory_coverage.py --cov=src/polystore --cov-report=html

# Run specific test class
pytest tests/test_disk_coverage.py::TestDiskBatchOperations -v
```

## Next Steps

1. Review test results to identify any failures
2. Fix any issues in the implementation based on test results
3. Move to zarr.py coverage expansion (deferred per user request)
4. Ensure all edge cases are covered before production release

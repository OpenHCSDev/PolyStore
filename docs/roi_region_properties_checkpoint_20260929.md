# ROI region-properties checkpoint

Owner: Lovelace. Integration: OpenHCS coordinator.
Authorized paired work for OpenHCSDev/openhcs #132 and draft PR #206.
Branch: `fix/fragmented-roi-properties-20260929`.
Isolated checkout: `/home/ts/wt/openhcs-input-preparation-20260929/external/PolyStore`.
Fetched/merged base: `origin/main` at `f94bbbe8631a4c78f7ceaec72fb99671b58c9a18`.
Only open PR at orientation: Dependabot #10; no overlapping feature PR.

## Independent PR206 review: dependency-owned Java probe

Latest owner explicitly extended this existing paired PR to correct the Java
lifecycle finding in
https://github.com/OpenHCSDev/openhcs/pull/206#issuecomment-5897315214.
No overlapping feature PR found: only this draft16 and Dependabot10 are open.
Source commit `0aa057bc3b4241d77f5f3905e10e31efe2ee87e4`, paired OpenHCS
`32a2b27e22220b28f4a75db1977973aa61384a10`.

IMPL-13 / BOUND-2: existing `BioFormatsJavaContext.is_single_file` at
`src/polystore/bioformats_java.py:158` now owns the decoder's capability. Existing
`declares_path:153` and this operation share `_probe_reader:168`, which initializes,
constructs and unconditionally closes a metadata-free Java reader. Neither probe
calls setId/OME metadata initialization. Delete the OpenHCS helper that imitated
this lifecycle; its adapter now invokes this public context operation. OpenHCS
still owns path-selection/compound-entrypoint policy. No alternate context,
extractor, cache, registry or filename/companion roster. ROI source remains the
already-reviewed in-place RegionProperties implementation unchanged.

Light check: **28 passed in 0.50s**, peak Python RSS **72.8 MiB**. Eleven child
cases (four existing context checks, six new true/false/failure/retry cases, one
AST ownership guard) plus seventeen parent binding-path behavior/guard cases.
Probe tests use controlled external responses but actual context initialization
and lifetime; zero JVM/ImageJ/MCP/GUI processes. A new probe reuses one lifetime
instead of reconstructing construction/cleanup in consumers. AST guard prevents
the two public operations from bypassing `_probe_reader`; parent guard prevents
its adapter from constructing Java readers or reauthoring binding policy.
New tests and touched child source pass Ruff; both diffs pass whitespace checks.
Current archive SKILL plus complete implementation/boundaries/membership chapters
read; scope source/AST/contract evidence, not full NRA/native proof.

Recorded interpreter/explicit worktree and all required submodule src paths.
Actual imports verified before testing. Bootstrap OpenHCS first, disable plugin
autoload, `pytest.main` with `-q --tb=short --noconftest --import-mode=importlib
-p no:cacheprovider -o addopts=` on the child Java context and parent path-admission
files; shell bound45s. Two unused asyncio config warnings with plugins disabled.
Initial26-case check passed before adding ownership guards (0.23s/67.1 MiB).
Guard reports11.4 GiB available/historical swap11.9 GiB, explicit8 GiB admission;
final11.2 GiB available. No continuous peak-RAM claim. Tiny owned92 KiB scratch
at the recorded path removed after results retained. No validation-lock takeover,
frozen installation/source/skill change, merge/install or blind-data inspection.

Original **141 passed / 26 failed** remains unresolved by these narrow passes;
the profile **did not run**. Separate-context preservation/affected source-control
rerun, bounded ROI after-profile and real Java/installed acceptance remain gated
on the next finite runtime handoff. Parent recorded gitlink stays `f94bbbe`;
integration now requires this paired public capability too. Coordinator owns
validated adoption/installation, not this worker. No closure/readiness claim.

## Owned change and deletion

`TwoDimensionalLabeledMaskROIExtractor.extract` stays the only 2D behavior
owner in the existing `LabeledMaskROIExtractor` registered family. The installed
scikit-image `RegionProperties.image` already caches the exact binary bbox crop;
area/perimeter evaluation uses it. `regionprops` itself calls `find_objects`.
The extractor previously called `find_objects` again, re-indexed its slices,
re-cropped the source, recomputed equality and cast that crop to uint8.

Remove that scan, import, indexed-slice guard, second crop/equality construction
and cast. Pad the existing unfilled `region.image`, and translate contours using
the already-owned bbox. Keep area thresholds, labels, order, contour level,
origin, metadata, full-canvas MaskShape representation and archive codecs.
No alternate extractor, cache, format, registry or source-pruning change.

## Pattern review and new cases

- BOUND-2: consume public `RegionProperties.bbox`/`image`, not a separately
  reconstructed representation of facts that object already models. The 2D
  method no longer imports or calls `find_objects`.
- IMPL-12/13: delete the repeated scan/crop procedure in place; do not copy it
  into an OpenHCS extractor or benchmark cache. Numeric dimensional acceptance
  and contour options remain existing geometric contracts, not string dispatch.
- IDEN-2/6: parents retain original sparse integer labels (7, 42, 188).
  Disconnected components and hole rings are shapes/members, not new identities.
- MEMB-1/2: the existing extractor and shape/converter registries remain the only
  family authorities. A new parent value or topology needs no roster/dispatch edit.

New-case witnesses in `tests/test_roi_region_properties.py`: a ring surrounding
islands with another parent label; objects touching all four image borders;
one-pixel necks, diagonal contact and disconnected border pixels. Zero/nonzero
spatial origins exercise bbox translation. Tests compare every contour vertex
and ordering with independent full-canvas scikit-image tracing, inspect parent
area/centroid/bbox, save/reopen the actual ImageJ ZIP, and compare every member's
coordinates and parent metadata. A work-count guard expects one whole-mask bbox
scan; min-area filtering is checked for contour and mask paths.

The existing full-canvas MaskShape path reconstructs exact labels, including
holes/topology. OpenHCS adds an actual raster-only TIFF materialization witness
with holes/borders and a guard against invoking its unselected ROI writer.

## Fidelity boundary

The polygon contract represents independent contours. It does not declare a
hole-subtraction relationship. Archive reopen preserves rings/parent metadata,
but that alone does not prove a viewer or third-party consumer rasterizes holes
correctly. MaskShape is explicitly unsupported by the ImageJ ROI codec. No
ZIP-to-label-raster algorithm or equivalence claim is invented here. Exact mask
and label-TIFF preservation are separate evidence. No biological data/output,
held-out files, JVM, GUI, installed package or shared source is modified.

## Validation and resource receipt

Earlier nonblocking validation-lock probe returned 75 (busy); no tests started
then. The coordinator handed off a finite slot on 2026-09-29, after installing
the independent #151/#212 harness checkpoint. Tested source: OpenHCS
`006361ff28528c0159bed0999149c71b8befda94` plus PolyStore
`8c886e79267f72ad88bda4143845ed1721325dd8`, with all nine package import locations
verified under this isolated worktree and explicit submodule PYTHONPATH.

Retained first attempt: 29 passed / 138 setup errors in 8.49s because the cleaned
scratch parent did not exist. Create only the owned scratch parent and retain the
same assertions. The combined retry produced 141 passed / 26 failed in 37.12s.
All sixteen new ROI fidelity/work-count cases and the existing ROI/fragmented
materialization cases passed, including hole/border/topology contour and archive
reopen fidelity, exact masks/TIFFs, original parent identities and one bbox scan.
Four actual fresh owned MCP generate/inspect/sample journeys passed as well.

The 26 source-workspace failures looked for `polystore_metadata.json` where the
application wrote `openhcs_metadata.json`. Source witness:
`polystore.metadata_writer.MetadataConfig` reads the generic environment/default
at module initialization; `openhcs/__init__.py:20` sets the application filename.
Combined dependency/application collection can initialize PolyStore first. The
retry runner had not imported OpenHCS before starting pytest, unlike the original
path-verification runner. This is a source-supported import-order explanation,
not an executed preservation proof. Lovelace owns the next separate-context
application regression check; no metadata API/source rewrite is authorized here.
Next driver uses separate dependency/application processes and imports the
existing `openhcs` entrypoint before application pytest collection, so its
declared initialization owns the filename. It will not monkeypatch metadata,
copy companion files, add a filename fallback or change a failed assertion.

The profile did not execute because the test subprocess exited nonzero. No
after-change timing/speedup claim. Run it and the application regressions in their
proper separately bootstrapped contexts at the next handed-off slot; do not race
the newly authorized fresh author.

The finite commands are terminal and released flock. A tagged-environment
process audit found zero owned handles, including pytest/MCP children, after
each terminal command. No restart on observation timeout, no JVM or GUI. Entry
RAM guard: 13.4 GiB available, historical swap warning 11.9 GiB; 8 GiB minimum
was checked before starting the run. No continuously sampled peak-RAM claim.
No source merge, installation or frozen-harness mutation in this validation.

Light evidence: new test passes Ruff; source/test AST and `git diff --check`
are the scoped guards. No full NRA scan or broad proof. The before-change bounded
128x128 OpenHCS profile observed two bbox scans, 3 parents and 902 members;
single-fixture timings remain in the parent receipt, not a speedup claim.

Executed command used `/home/ts/code/projects/openhcs/.venv/bin/python -m pytest
-q --tb=short --import-mode=importlib -o addopts=` with the new/existing ROI,
fragmented materialization, preparation selection, Bio-Formats, source workspace,
plane-store, zero-geometry and real MCP journey files. CPU-only mode; exact source
paths; automatic coverage disabled. The tests' assertions were not weakened.

Scratch owner: Lovelace. Purpose: tiny pytest archives and profile output.
Path: `/home/ts/.cache/agent-scratch/openhcs-issue-input-20260929`.
The previous 8.1 MiB fixtures were removed. The finite check regenerated 4.2 MiB
of disposable synthetic fixtures; that directory was also removed after retaining
this receipt. No source/saved session/private biological data was removed.
Receipts, code and PR body files remain on persistent worktree storage.

Parent recorded gitlink remains at the base until a published paired commit is
ready for coordinator review. No merge/install performed.

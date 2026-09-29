# ROI region-properties checkpoint

Owner: Lovelace. Integration: OpenHCS coordinator.
Authorized paired work for OpenHCSDev/openhcs #132 and draft PR #206.
Branch: `fix/fragmented-roi-properties-20260929`.
Isolated checkout: `/home/ts/wt/openhcs-input-preparation-20260929/external/PolyStore`.
Fetched/merged base: `origin/main` at `f94bbbe8631a4c78f7ceaec72fb99671b58c9a18`.
Only open PR at orientation: Dependabot #10; no overlapping feature PR.

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

Implementation and tests authored; behavioral validation pending Euler's shared
slot. Nonblocking validation-lock probe returned 75 (busy), so no tests/JVM/GUI
started and no polling/sleeping. Resource guard: 13.2 GiB available RAM,
historical swap warning 11.5 GiB. Maintain at least 8 GiB available.

Light evidence: new test passes Ruff; source/test AST and `git diff --check`
are the scoped guards. No full NRA scan or broad proof. The before-change bounded
128x128 OpenHCS profile observed two bbox scans, 3 parents and 902 members;
single-fixture timings remain in the parent receipt, not a speedup claim.

When the slot is released, run the new/existing ROI tests and OpenHCS fragmented
materialization tests with `/home/ts/code/projects/openhcs/.venv/bin/python`,
CPU-only mode and verified worktree/submodule source paths. Disable automatic
coverage output for this focused run. Re-run the bounded phase profile; do not
start a large CZI, GUI or JVM journey.

Scratch owner: Lovelace. Purpose: tiny pytest archives and profile output.
Planned path: `/home/ts/.cache/agent-scratch/openhcs-issue-input-20260929`.
The previous 8.1 MiB fixtures were removed; no new generated output yet.
Receipts, code and PR body files remain on persistent worktree storage.

Parent recorded gitlink remains at the base until a published paired commit is
ready for coordinator review. No merge/install performed.

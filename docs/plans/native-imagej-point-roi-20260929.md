# Native ImageJ point archive repair checkpoint

Implemented native codec repair. Dependency integration and live viewer validation
remain separate from native archive validation.

Base and source audited: `0efe67fdd14985bf90cee6e0f0c4735d41d265d1`,
OpenHCSDev/PolyStore main. Issue: OpenHCSDev/PolyStore#12.
Dependent work: OpenHCSDev/openhcs#134 and draft OpenHCSDev/openhcs#153.
Branch: `fix/native-imagej-point-roi-20260929`.
Own worktree: `/home/ts/wt/polystore-point-roi-20260929`.

## Current delivery decision

The user explicitly authorized bounded Python inspection and the lightweight
refactor-audit scripts so the FULL NRA memory defect does not block OpenHCS.
Main owns this codec repair; the previous worker is no longer active. The
existing archive reader and `ImageJROIShapeConverter` family are the bounded
production corpus. All 53 original classes across these two modules were
inspected through NRA's canonical syntax index and joined to its 53 eligible
class-family projections. Declared local codec bases resolve to the existing
family. External base execution/MRO proof and full global/R1 coverage remain
OPEN and are not claimed. The bounded projection took 1.02 seconds and 63,800 KiB
peak RSS.

The admitted external-format obligation is POINT bytes to PointShape, preserving
all XY coordinates and existing per-member metadata. Polygon/polyline/oval
members must keep their own geometry, not use a silent polygon fallback.
Extend the existing registered converter family with decoding behavior and
derive native-kind lookup from its leaf declarations. The archive reader keeps
ownership of ZIP bytes and metadata sidecar assembly. Delete its geometry
switch. Unsupported native kinds must fail explicitly. This closes IMPL-4 and
the concrete BOUND-2 bypass within this boundary; it is not a proof of global
architecture or unrelated external ROI subtype support.

Native encoding/decoding additions are behavior changes validated by actual
roifile bytes and the disk entrypoint, not asserted equivalent from syntax.
New geometry requires one codec declaration, not a ZIP-reader branch. Existing
scientific archives and native Z/C/T metadata are not rewritten or guessed.
Owned small test output/cache: `/home/ts/.cache/agent-scratch/openhcs-point-codec-20260929`.

## Implemented behavior and validation

- POINT encoding sets the actual native POINT kind. Decode retains every
  point, exact fractional YX, and existing logical metadata.
- Polygon, polyline, and oval decoding belongs to their existing codec leaves.
  Oval writing now uses the native subpixel bounding rectangle rather than a
  two-point FREEHAND payload relabeled OVAL. Independent integer oval input is
  tested too.
- Native kinds resolve from the existing registered leaf declarations. There
  is no added registry or archive-reader geometry switch. Unsupported kinds,
  specialized subtypes, malformed/empty/nonfinite coordinates, missing sidecar
  metadata, and mask encoding fail explicitly.
- Old one-point FREEHAND archives stay invalid. No legacy reader is added and
  no frozen scientific archive is rewritten.

The realistic disk save/load entrypoint, native roifile bytes, Napari projection,
existing disk tests, and streaming identity/metadata tests passed together:
114 passed in 0.88 seconds; 1.32 seconds wall time; peak RSS 104,084 KiB.
Command: existing OpenHCS CPython 3.12 environment, this worktree's `src` first
on `PYTHONPATH`, recorded parent dependency source roots, plugin autoload off,
`python -m pytest -o addopts= -q tests/test_roi.py tests/test_disk_backend.py
tests/test_disk_more.py tests/test_disk_coverage.py tests/test_streaming_metadata.py
tests/test_streaming_identity.py`, bounded by a 60-second shell timeout.
This is source-override native validation, not installed or rendered-viewer proof.
An earlier tiny CPython 3.14 environment lacked portalocker during conftest
import; that setup failure was retained separately and was not a codec test.

Before-change lightweight census and overlay at committed HEAD took 0.48 and
1.61 seconds respectively, with 21,776 and 39,628 KiB peak RSS. They read Git
revisions, not uncommitted files. Candidate `7c7f681` versus the recorded main
adds 80 production code lines, removes one None probe, and changes none of the
other census debt counts. The native external-kind switch has been deleted,
as independently confirmed in the combined source diff. Census: 0.14 seconds,
20,212 KiB peak RSS. Candidate overlay: 1.59 seconds, 40,748 KiB peak RSS; its
category totals match the baseline, with no parse warnings. Existing unrelated
switches and record-shape leads remain, so neither metric is a claim of a clean
package or completed global NRA coverage.

## Native evidence

The synthetic codec diagnostic imports only the worker's pinned dependency
sources. The original completed run used CPython 3.14.6, NumPy 2.5.1,
PolyStore 0.2.19, roifile 2026.2.10, ZMQRuntime 0.2.24, and numcodecs 0.17.0.
Three cases: polygon control passed, two point cases failed. Runtime 0.39 s,
peak RSS 53168 KiB. The canonical-metadata repeat had the same outcomes:
0.28 s, peak RSS 51808 KiB. These are native codec diagnostics, not transport,
installed OpenHCS, managed viewer or scientific acceptance.

Receipt root: `/tmp/openhcs-pr153.LeLQct`. Original completed evidence:
`scan-recovery-point-native-prerequisite-768.{json,stderr,guard}`.
Canonical repeat: `scan-recovery-point-native-canonical-768.{json,stderr,guard}`.
The earlier missing-numcodecs import failure remains separate and is not a
codec failure. The canonical comparison derives logical metadata from public
`roi_zip_metadata_payload` and JSON decoding; native coordinate checks are
independent of tuple/list wire representation.

- `PointImageJROIShapeConverter`, `src/polystore/roi_converters.py:547`, emits
  FREEHAND through `ImagejRoi.frompoints`, not POINT. The archive preserves
  exact XY `(3.5, 1.25)`, metadata label 7, object/source names, and zero-based
  `plane_indices=(2,)`. Native position/C/Z/T fields are all 0 (unset).
- The writer's integer extents top/left/bottom/right are `(1,4,2,5)` while its
  subpixel XY coordinates remain exact. Do not treat rounded extents as loss
  of the subpixel coordinates or invent an image extent.
- `load_rois_from_zip`, `src/polystore/roi.py:581`, maps non-POLYLINE shapes to
  PolygonShape. Both the production point archive and a standard synthetic
  POINT archive fail with `Polygon must have at least 3 vertices, got 1`.
- The standard POINT fixture preserves exact XY and native one-based Z=3
  (zero-based Z=2) through roifile before the PolyStore reader fails.
- `DiskStorageBackend._save_rois`, `src/polystore/disk.py:814`, owns archive
  assembly and the existing per-member metadata. The polygon control passes
  exact native XY plus logical label/object/source/plane metadata identity.

## Owner-level closure proposal

Pattern leads: IMPL-4, an existing nominal family owns encoding while decoding
is outside it; IMPL-2, external ROI enum cases are recovered in a consumer;
BOUND-2, decoding should consume the existing shape/codec authority. These
source-checked leads are not completed NRA claims.

Extend the existing nominal ImageJ geometry/codec owners. Shared archive
loading calls the owned decoding contract; leaf declarations own external
ImageJ kind acceptance and geometry construction. Compose actual encode/decode
capabilities through MI/MRO where needed. Derive any lookup from those
declarations, never maintain a second shape roster, string dispatch ladder or
OpenHCS-specific decoder. A typed carrier or relocation alone does not close
the ownership gap.

External ImageJ bytes and existing archive metadata remain the format
authority. POINT encoding uses POINT, and decoding preserves exact YX and
source/object/plane identity. Native Z/C/T projection requires declared axis
meaning; generic leading plane indices are not automatically a Z axis. Reject
ambiguous or malformed identity instead of guessing. Polygon, polyline and
ellipse behaviour remain protected by the same family contract.

Proposed production write scope: existing `src/polystore/roi.py`,
`src/polystore/roi_converters.py`, and existing native codec tests. Change
`src/polystore/disk.py` only if archive-owner closure genuinely requires it and
coordinate the extension first. Production changes stay within the two recorded
ROI owners; the disk/archive metadata owner is unchanged.

New-case experiment: a supported geometry declares its own external kind and
encode/decode behaviour once; neither the archive reader nor OpenHCS adds a
dispatch branch. Guards exclude duplicate codec maps, silent polygon fallback,
raw-string ladders and weakened validation. Unsupported geometry remains an
explicit failure, not an alias.

## Historical common audit gate and integration boundary

The common complete-context audit retains OpenHCS plus all eight recorded
dependency source roots and all 79 detectors. It is not complete. The first
compact attempt exceeded 768 MiB; the all-root compact attempt stayed bounded
but hit the 160-second deadline (`complete=false`). Required full/raw R1,
omitted-detector coverage and source-export evidence remain unavailable.
That earlier implementation hold is superseded by the user's delivery decision
above; partial cache evidence is still not a complete global audit.

The old complete-coverage hold no longer controls this native repair. No
revision-checked NRA DSL transaction or completed behavior-equivalence proof
is claimed for these authored codec additions. Native tests cover
POINT/standard POINT, exact fractional YX, explicit native Z and sidecar plane
identity, label/object/source, polygon/polyline/ellipse, malformed and unsupported
types. Test shards are bounded to 60 seconds with CPU-only/offscreen settings
where applicable and owned cache roots.

This dependency worktree is independent of the common scan's pinned clone.
The OpenHCS parent gitlink stays at the recorded base. Integration requires a
validated/merged dependency PR and integration owner's recorded-SHA selection.
User authorization permits locally validated checkpoints without waiting for
optional hosted CI. Before claiming this fixes the blind-review workflow,
integrate the dependency and verify the actual OpenHCS/MCP/viewer route on the
isolated display. Counts and biological acceptance remain frozen/ambiguous.

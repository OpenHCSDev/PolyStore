PolyStore 0.3.1 publisher admission
================================

Source base: 84f322e46871de5ed47e7fd20976ad03e533c1c4 (remote main).
Owner: Dewey, isolated polystore-publisher-admission-031-20261002 worktree.
Parent owns integration, the sole new 0.3.1 tag, and hosted release acceptance.
The immutable failed v0.3.0 tag is untouched.

Original failure
----------------

Run https://github.com/OpenHCSDev/PolyStore/actions/runs/36959840684,
job 110690882405: 9 failed, 545 passed, 19 skipped in 15.91s.
Three failures require child processes to import checkout source; the publisher
installed a regular distribution. Six ROI topology/archive cases fail because
the existing dev extra omitted roifile. No original test or skip is changed.

The full job log was fetched through the GitHub SDK job-log tool and archived
as original-job-110690882405.log.gz (lossless gzip), preserving UTF-8 text including
its original BOM and final newline: 142020 bytes, SHA256
542d75ef00bcd8677526131a1afe195cea6fa63edca7eca5aa8e0b1b4e4b703c.

Ownership and repair
--------------------

Replace the existing publisher install with editable .[dev] admission, matching
the existing ordinary test workflow. The later package build and immutable
declaration/publish steps are unchanged. Declare roifile in the original dev
extra, not a workflow-only dependency roster or mandatory runtime requirement.
Advance the sole project version declaration to 0.3.1; the existing package
version and release scripts derive it from distribution/project metadata.

Current NRA/refactor-audit skills and packaged pattern catalog were read.
MEMB-1/MEMB-2: dependency membership remains in the original extras declaration.
TIME-8: no duplicated version or metadata authority. No Python runtime mechanism
is introduced, so inheritance/MI would be ornamental here. Independent future
dev dependencies need only their own extra entry, not publisher consumer edits.
The real editable-build test reads emitted metadata and the actual source .pth
from the existing setuptools backend rather than mirroring its implementation.

Open-PR check found only Dependabot PR10, whose publisher-file changes are action
pins; this patch leaves those pins unchanged. Parent's release worktree is not
edited. No environment installation, external download, Fiji/native launch,
science job, tag or release operation was performed.

Focused source qualification
----------------------------

68 PASS, zero skips: publisher admission/editable metadata, basic package,
ImageJ cache policy and fresh-process source, namespace handoff, ROI geometry
and archive roundtrip, and lazy package exports. These include all nine original
failed cases. The initial original-only run was 63 PASS; new admission controls
were 2 PASS. Final combined run: 8.43s pytest, 9.089s monitored wall,
322700 KiB aggregate RSS (315.14 MiB), one CPU, 512 MiB / 60s limits.
See qualified.xml and qualified-source.txt. The command is in COMMANDS.rst.

Source is explicitly this worktree's src; dependencies/interpreter are readonly
openhcs-generated-inputs-installed-parent-20261001/.venv/bin/python.
The two publisher controls exercise the real offline editable build without
installing its artifact. They verify checkout binding, unfiltered publisher
pytest invocation, derived version, and dev-only ROI dependency metadata.
No installed editable-candidate or hosted 0.3.1 acceptance is claimed.

Lock limitation
---------------

uv lock --offline --no-python-downloads failed because imglyb==2.1.0 is absent
from the cache for the all/bioformats extras. Original uv.lock already records
PolyStore 0.2.18 and ZMQRuntime 0.2.21, inconsistent with base declarations.
The lock is unchanged: no fabricated entries, constrained platforms, dependency
weakening or downloads. Publisher pip does not consume uv.lock. The existing
separate distribution CI lock-check remains unqualified until authorized lock
regeneration; this is not represented as publisher or runtime acceptance.

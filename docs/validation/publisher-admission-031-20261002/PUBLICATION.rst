Original 0.3.1 publication attempt
=================================

Parent merged PR21 normally at f07da0971947e7b7938503961a7014909e560307.
The original release.py session36811 was resumed once by the parent and
completed. No release invocation, retag, resubmission, rerun, environment install
or scientific operation was performed by this source worker.

Exact publisher handle:
https://github.com/OpenHCSDev/PolyStore/actions/runs/36963152726
Job 110701039079, run_attempt 1, event push, created 2026-10-02T04:07:22Z.
The native gh run watch observes this handle only.

Verified remote tag objects:

* v0.3.1 annotated tag 57740a8200d27633da81a9e7a7320d5f4ebb1147,
  peels to f07da0971947e7b7938503961a7014909e560307.
* Immutable failed v0.3.0 tag 282c13750e40eb96135f6c7072d2b0c540db4309,
  still peels to 84f322e46871de5ed47e7fd20976ad03e533c1c4.

Initial observation: run queued, no GitHub release and PyPI 0.3.1 JSON HTTP404.
This is pending publication, not failure or artifact acceptance.

Terminal publication acceptance
-------------------------------

The SAME attempt completed SUCCESS, job start 2026-10-02T04:18:47Z,
job completion 2026-10-02T04:20:22Z. Every build/publish step passed:
editable installation, full release-candidate tests, ordinary distribution build,
immutable declaration validation, twine check, trusted PyPI publication, GitHub
release and original post-job cleanup. No attempt was resubmitted or replayed.

Actual hosted suite: 570 PASS, 5 SKIP, zero failures in 17.57s. These are the
original full publisher selection plus the two new admission tests. No source
test, assertion or skip was weakened or added to avoid the original nine reds.
Do not substitute the earlier local 572/1skip count for this hosted result.

PyPI JSON confirms version0.3.1, requires-python >=3.11, and roifile declared
only for the dev extra. Both PyPI distributions are non-yanked. GitHub release
401537625 is public/non-draft/non-prerelease, published 2026-10-02T04:20:19Z:
https://github.com/OpenHCSDev/PolyStore/releases/tag/v0.3.1
PyPI release: https://pypi.org/project/polystore/0.3.1/

Programmatic artifact metadata comparison found BOTH filename/size/SHA256
identities equal between PyPI JSON and the GitHub release assets. The original
publisher DSSE subject digests in its retained log match these same digests:

* polystore-0.3.1-py3-none-any.whl, 157342 bytes,
  d1c81414f9b5ec1373a68a41bf704ec76c4b1f60a1ea0f00bb031bc8ae264e50.
* polystore-0.3.1.tar.gz, 193442 bytes,
  0d1e78468581ead9c1cb13fcea0bc208ce52ba4e909bbcedf1662604be928dc7.

GitHub also retains both publish.attestation assets. Workflow artifact inventory
is empty; these are actual GitHub RELEASE assets, not nonexistent Actions
artifact uploads. The initial publication check established metadata agreement
only. The subsequent bounded actual-byte verification below extends that proof;
neither check establishes fresh installed execution or biological acceptance.

Full successful job log fetched through original GitHub SDK and archived
losslessly in original-job-110701039079.log.gz. Decoded original bytes169009,
including BOM/final newline, SHA256
cd1723ac96587309e23dec9144476c0219ae2b2124b0e3c084fe701a8fedfa49.
Raw terminal-run, GitHub-release, PyPI and workflow-artifact JSON replies are
archived alongside it. The v0.3.0/v0.3.1 tag object identities were checked
again after successful publication and remained unchanged.

Publication blocker is closed. The previously recorded offline uv.lock
limitation remains separate and unchanged; this publisher does not use it.

Independent public-wheel byte verification
-----------------------------------------

After the owner authorised one157KB public fetch, no retained exact public wheel
was found under the worktrees, agent scratch or uv/pip caches. Existing local
editable wheels were not substituted for the published artifact. One HTTPS fetch
of the original PyPI wheel URL completed HTTP200 in0.086730s,157342bytes.
An independent local file read and SHA256 calculation asserted:

* Actual length157342bytes.
* Actual digest
  d1c81414f9b5ec1373a68a41bf704ec76c4b1f60a1ea0f00bb031bc8ae264e50.

Both expected identities matched, exit0. PUBLIC-WHEEL-BYTE-PROOF.txt retains
the original command/results and bounds: oneCPU/512MiB, fetch20s(max25s wall),
response maximum157342B, hash10s. No package installation/import, environment
creation, source change, native/science run or dependency download occurred.
The sdist remains metadata-agreement-only; its bytes were not independently
downloaded/hashed. The wheel byte check does not imply installed acceptance.

The completed original synthetic-test scratch and single public-wheel scratch
are closed after proof archival; CLEANUP.rst records the exact targets/results.
Both original publisher logs, compressed evidence, source, branch history and
native journal remain preserved.

The original append-only tool-call authority is linked at
/home/ts/wt/openhcs-issue-batch-20260929/agent-audit-20261002/Dewey/native-session.jsonl.
The existing link resolves to the original Codex session journal; it has not
been copied, edited or replaced. Source/publication work has no microscopy
screenshot claim. Original failed 0.3.0 publisher logs remain preserved.

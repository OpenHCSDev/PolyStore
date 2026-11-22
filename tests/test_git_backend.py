"""GitBackend: versioned, content-addressed text through git plumbing."""

from __future__ import annotations

import pickle
import subprocess

import pytest

from polystore import FileManager, GitBackend, MemoryBackend
from polystore.backend_registry import create_storage_registry
from polystore.exceptions import StorageResolutionError


@pytest.fixture
def backend(tmp_path):
    return GitBackend(tmp_path / "store.git")


def test_saved_files_are_read_back_and_listed_like_a_filesystem(backend):
    backend.save("turn one", "ctx/0001-user.txt")
    backend.save(b"\x00raw", "ctx/0002-tool.bin")
    backend.save("a note", "ctx/notes/plan.md")

    assert backend.load_text("ctx/0001-user.txt") == "turn one"
    assert backend.load("ctx/0002-tool.bin") == b"\x00raw"
    assert backend.list_files("ctx") == ["ctx/0001-user.txt", "ctx/0002-tool.bin"]
    assert backend.list_files("ctx", recursive=True, extensions={".md"}) == ["ctx/notes/plan.md"]
    assert backend.list_files("", pattern="*.txt", recursive=True) == ["ctx/0001-user.txt"]
    assert backend.list_dir("ctx") == ["0001-user.txt", "0002-tool.bin", "notes"]
    assert backend.exists("ctx/notes") and backend.exists("ctx/0001-user.txt")
    assert not backend.exists("ctx/missing")
    with pytest.raises(IsADirectoryError):
        backend.is_file("ctx")
    with pytest.raises(NotADirectoryError):
        backend.is_dir("ctx/0001-user.txt")
    with pytest.raises(FileNotFoundError):
        backend.load("ctx/missing")


def test_a_checkpoint_is_a_commit_any_git_reads_and_a_reopened_branch_resumes_from(tmp_path):
    first = GitBackend(tmp_path / "store.git")
    assert first.head() is None
    first.save("v1", "doc.txt")
    one = first.checkpoint("write v1")
    first.save("v2", "doc.txt")
    two = first.checkpoint("write v2")

    assert first.checkpoint("nothing changed") == two
    assert [commit.message for commit in first.history()] == ["write v2", "write v1"]
    assert first.load_text("doc.txt", ref=one.id) == "v1"
    shown = subprocess.run(
        ["git", f"--git-dir={tmp_path / 'store.git'}", "show", "refs/heads/main:doc.txt"],
        capture_output=True, check=True,
    )
    assert shown.stdout == b"v2"
    reopened = GitBackend(tmp_path / "store.git")
    assert reopened.head() == two
    assert reopened.load_text("doc.txt") == "v2"


def test_writes_that_were_never_checkpointed_are_not_on_the_branch(tmp_path):
    first = GitBackend(tmp_path / "store.git")
    first.save("kept", "kept.txt")
    first.checkpoint("keep")
    first.save("staged only", "lost.txt")

    assert GitBackend(tmp_path / "store.git").list_files("") == ["kept.txt"]


def test_history_of_one_path_names_only_the_checkpoints_that_changed_it(backend):
    backend.save("a1", "a.txt")
    backend.checkpoint("a1")
    backend.save("b1", "b.txt")
    backend.checkpoint("b1")
    backend.save("a2", "a.txt")
    backend.checkpoint("a2")

    assert [commit.message for commit in backend.history("a.txt")] == ["a2", "a1"]
    assert [commit.message for commit in backend.history(limit=1)] == ["a2"]


def test_delete_move_and_copy_change_the_staged_tree_only(backend):
    backend.save("one", "ctx/0001.txt")
    backend.save("two", "ctx/0002.txt")
    before = backend.checkpoint("two turns")

    backend.move("ctx/0002.txt", "store/0002.txt")
    backend.copy("ctx", "backup")
    backend.delete("ctx/0001.txt")

    assert backend.list_files("", recursive=True) == ["backup/0001.txt", "store/0002.txt"]
    assert backend.load_text("ctx/0001.txt", ref=before.id) == "one"
    backend.delete_all("backup")
    assert backend.list_dir("") == ["store"]
    with pytest.raises(FileNotFoundError):
        backend.delete("ctx/0001.txt")


def test_content_ids_are_equal_exactly_when_content_is_equal(backend):
    backend.save("same", "a/x.txt")
    backend.save("same", "b/x.txt")
    backend.save("other", "c/x.txt")

    assert backend.blob_id("a/x.txt") == backend.blob_id("b/x.txt") != backend.blob_id("c/x.txt")
    assert backend.tree_id("a") == backend.tree_id("b") != backend.tree_id("c")
    whole = backend.tree_id()
    backend.save("changed", "c/x.txt")
    assert backend.tree_id("a") == backend.tree_id("b")
    assert backend.tree_id() != whole


def test_restore_stages_a_past_tree_and_records_the_return_as_a_new_commit(backend):
    backend.save("v1", "doc.txt")
    one = backend.checkpoint("v1")
    backend.save("v2", "doc.txt")
    backend.save("extra", "extra.txt")
    backend.checkpoint("v2")

    backend.restore(one.id)
    assert backend.list_files("") == ["doc.txt"]
    back = backend.checkpoint("back to v1")
    assert back.tree == one.tree and back.id != one.id
    assert [commit.message for commit in backend.history()] == ["back to v1", "v2", "v1"]


def test_a_fork_starts_at_the_head_and_diverges(backend):
    backend.save("shared", "doc.txt")
    backend.checkpoint("shared")
    branch = backend.fork("refs/heads/branch")
    branch.save("branch only", "doc.txt")
    branch.checkpoint("diverge")

    assert backend.load_text("doc.txt") == "shared"
    assert [commit.message for commit in branch.history()] == ["diverge", "shared"]
    with pytest.raises(StorageResolutionError):
        backend.fork("refs/heads/branch")


def test_a_checkpoint_never_overwrites_another_writers(tmp_path):
    first = GitBackend(tmp_path / "store.git")
    first.save("base", "doc.txt")
    first.checkpoint("base")
    second = GitBackend(tmp_path / "store.git")
    first.save("first", "doc.txt")
    second.save("second", "doc.txt")
    second.checkpoint("second wins the race")

    with pytest.raises(StorageResolutionError):
        first.checkpoint("first lost")
    assert GitBackend(tmp_path / "store.git").load_text("doc.txt") == "second"


def test_paths_stay_inside_the_repository_and_payloads_are_bytes_or_text(backend):
    for escape in ("/abs.txt", "../up.txt", "a/../../up.txt"):
        with pytest.raises(StorageResolutionError):
            backend.save("x", escape)
    with pytest.raises(TypeError):
        backend.save([1, 2, 3], "list.txt")
    assert backend.accepts_payload("text", "a.txt") and not backend.accepts_payload(3, "a.txt")


def test_many_revisions_of_one_document_pack_to_about_one_copy(backend):
    lines = [f"line {number}: " + "x" * 60 for number in range(400)]
    for revision in range(40):
        lines[revision] = f"edited in revision {revision}"
        backend.save("\n".join(lines), "ctx/LIVE.txt")
        backend.checkpoint(f"revision {revision}")
    backend.compact()

    packed = sum(file.stat().st_size for file in (backend.repository / "objects").rglob("*") if file.is_file())
    assert packed < 2 * len("\n".join(lines))
    assert backend.load_text("ctx/LIVE.txt", ref=backend.history()[-1].id).startswith("edited in revision 0\nline 1:")


def test_it_routes_through_a_file_manager_and_survives_pickling(tmp_path):
    backend = GitBackend(tmp_path / "store.git")
    files = FileManager({"git": backend, "memory": MemoryBackend()})
    files.save("routed", "ctx/a.txt", backend="git")
    assert files.load("ctx/a.txt", backend="git") == b"routed"
    backend.checkpoint("routed")

    clone = pickle.loads(pickle.dumps(backend.get_connection_params()))
    assert GitBackend.from_connection_params(clone).load_text("ctx/a.txt") == "routed"


def test_the_default_registry_builds_no_backend_that_needs_context():
    registry = create_storage_registry()
    assert "git" not in registry and "virtual_workspace" not in registry
    assert {"disk", "memory", "zarr"} <= set(registry)


def test_a_batch_is_stored_by_two_git_processes_whatever_its_size(tmp_path, monkeypatch):
    import subprocess

    backend = GitBackend(tmp_path / "store.git")
    started: list[list[str]] = []
    run = subprocess.run

    def counted(command, *arguments, **keywords):
        started.append(command)
        return run(command, *arguments, **keywords)

    monkeypatch.setattr(subprocess, "run", counted)
    names = [f"ctx/{number:04d} turn.txt" for number in range(60)]
    backend.save_batch([f"text {number}\n" for number in range(59)] + [b"\x00bytes"], names)
    assert len(started) == 2
    monkeypatch.setattr(subprocess, "run", run)

    assert backend.list_files("ctx") == names
    assert backend.load_text(names[7]) == "text 7\n" and backend.load(names[-1]) == b"\x00bytes"
    backend.save("replaced\n", names[7])
    backend.checkpoint("batch")
    assert GitBackend(tmp_path / "store.git").load_text(names[7]) == "replaced\n"

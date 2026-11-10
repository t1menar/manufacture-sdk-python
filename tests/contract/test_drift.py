"""Drift test: the vendored contract must equal the pinned core contract release.

Offline checks (always): every vendored file matches the sha256 in the lock and nothing
untracked hides in the vendored directories. Online check (``MF_CONTRACT_ONLINE=1``, on in
CI): download the pinned release archive, verify it against the release ``SHA256SUMS`` and
the checksum in the lock, and compare every included file byte by byte.
"""

from __future__ import annotations

import os
import re

import pytest

from contract_sync import ROOT, Lock, diff, fetch_release, included, sha256
from manufacture import CONTRACT_VERSION
from manufacture._contract import CONTRACT_COMMIT

LOCK = Lock.load()


def test_lock_pins_a_release_and_its_commit() -> None:
    assert re.fullmatch(r"contract-v\d+\.\d+\.\d+", LOCK.release)
    assert LOCK.release == f"contract-v{LOCK.version}"
    assert re.fullmatch(r"[0-9a-f]{64}", LOCK.archive_sha256)
    assert re.fullmatch(r"[0-9a-f]{40}", LOCK.commit)
    assert LOCK.commit == CONTRACT_COMMIT


def test_lock_version_matches_vendored_version_file() -> None:
    assert LOCK.version == CONTRACT_VERSION


@pytest.mark.parametrize("path", sorted(LOCK.files))
def test_vendored_file_matches_lock(path: str) -> None:
    local = LOCK.local_path(path)
    assert local.is_file(), f"{path} is in the lock but not vendored"
    assert sha256(local.read_bytes()) == LOCK.files[path], (
        f"{local.relative_to(ROOT)} was edited locally; run scripts/contract_sync.py instead"
    )
    assert LOCK.is_included(path), f"{path} is vendored but not in include"


def test_no_untracked_files_in_vendored_directories() -> None:
    tracked = {LOCK.local_path(p) for p in LOCK.files}
    for prefix, dest in LOCK.mappings.items():
        if not prefix.endswith("/"):
            continue
        for path in (ROOT / dest).rglob("*"):
            if path.is_file() and "__pycache__" not in path.parts:
                assert path in tracked, f"{path.relative_to(ROOT)} is not part of the contract lock"


@pytest.mark.contract_online
@pytest.mark.skipif(
    os.environ.get("MF_CONTRACT_ONLINE") != "1", reason="set MF_CONTRACT_ONLINE=1 to fetch GitHub"
)
def test_vendored_copy_matches_pinned_release() -> None:
    digest, files = fetch_release(LOCK.repository, LOCK.release)
    assert digest == LOCK.archive_sha256, "the release archive was re-published"
    upstream = included(LOCK, files)
    assert diff(LOCK, upstream) == []
    assert {p: sha256(d) for p, d in upstream.items()} == LOCK.files

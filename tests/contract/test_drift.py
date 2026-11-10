"""Drift test: the vendored contract must be byte-identical to the pinned core commit.

Offline checks (always): every vendored file matches the sha256 in the lock and nothing
untracked hides in the vendored directories. Online check (``MF_CONTRACT_ONLINE=1``, on in
CI): download the pinned commit from GitHub and compare byte by byte, including the file
list, so a file added upstream under an included directory is reported as drift.
"""

from __future__ import annotations

import os
import re

import pytest

from contract_sync import ROOT, Lock, diff, fetch_contract, sha256
from manufacture import CONTRACT_VERSION
from manufacture._contract import CONTRACT_COMMIT

LOCK = Lock.load()


def test_lock_pins_full_commit_sha() -> None:
    assert re.fullmatch(r"[0-9a-f]{40}", LOCK.commit), "pin a full commit SHA, not a branch"
    assert LOCK.commit == CONTRACT_COMMIT


def test_lock_version_matches_vendored_version_file() -> None:
    assert re.fullmatch(r"\d+\.\d+\.\d+", CONTRACT_VERSION)
    assert LOCK.version == CONTRACT_VERSION


@pytest.mark.parametrize("core_path", sorted(LOCK.files))
def test_vendored_file_matches_lock(core_path: str) -> None:
    local = LOCK.local_path(core_path)
    assert local.is_file(), f"{core_path} is in the lock but not vendored"
    assert sha256(local.read_bytes()) == LOCK.files[core_path], (
        f"{local.relative_to(ROOT)} was edited locally; run scripts/contract_sync.py instead"
    )
    assert LOCK.is_included(core_path), f"{core_path} is vendored but not in include"


def test_no_untracked_files_in_vendored_directories() -> None:
    tracked = {LOCK.local_path(p) for p in LOCK.files}
    for dest in LOCK.mappings.values():
        for path in (ROOT / dest).rglob("*"):
            if path.is_file() and "__pycache__" not in path.parts:
                assert path in tracked, f"{path.relative_to(ROOT)} is not part of the contract lock"


@pytest.mark.contract_online
@pytest.mark.skipif(
    os.environ.get("MF_CONTRACT_ONLINE") != "1", reason="set MF_CONTRACT_ONLINE=1 to fetch GitHub"
)
def test_vendored_copy_matches_pinned_core_commit() -> None:
    upstream = fetch_contract(LOCK, LOCK.commit)
    assert diff(LOCK, upstream) == []
    assert {p: sha256(d) for p, d in upstream.items()} == LOCK.files

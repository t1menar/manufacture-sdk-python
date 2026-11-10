#!/usr/bin/env python3
"""Vendor the Manufacture contract from ``t1menar/manufacture-core`` at a pinned commit.

The lock file ``src/manufacture/_contract/contract.lock.json`` says *what* is vendored
(``include``: core paths, a trailing ``/`` means a whole directory), *where* it goes
(``mappings``: core prefix -> path in this repo) and *which* commit it came from
(``commit`` + per-file ``sha256``). To track a new schema directory (for example the
envelope schemas of core PR C2) add it to ``include`` and run the sync.

Usage::

    python scripts/contract_sync.py --ref <sha|branch|tag>   # vendor, rewrite the lock
    python scripts/contract_sync.py --check                  # compare with the pinned commit
    python scripts/contract_sync.py --check --ref main       # report drift against core main

Only the standard library is used, so the script runs before ``uv sync``. Set
``GITHUB_TOKEN`` to raise the GitHub API rate limit.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

ROOT = Path(__file__).resolve().parent.parent
LOCK_PATH = ROOT / "src" / "manufacture" / "_contract" / "contract.lock.json"
API = "https://api.github.com"
RAW = "https://raw.githubusercontent.com"
TIMEOUT_S = 30


@dataclass(frozen=True)
class Lock:
    """Parsed ``contract.lock.json``."""

    repository: str
    commit: str
    version: str
    include: tuple[str, ...]
    mappings: dict[str, str]
    files: dict[str, str]

    @classmethod
    def load(cls, path: Path = LOCK_PATH) -> Lock:
        raw = cast("dict[str, Any]", json.loads(path.read_text(encoding="utf-8")))
        return cls(
            repository=str(raw["repository"]),
            commit=str(raw["commit"]),
            version=str(raw["version"]),
            include=tuple(str(p) for p in raw["include"]),
            mappings={str(k): str(v) for k, v in raw["mappings"].items()},
            files={str(k): str(v) for k, v in raw["files"].items()},
        )

    def dump(self, path: Path = LOCK_PATH) -> None:
        doc = {
            "repository": self.repository,
            "commit": self.commit,
            "version": self.version,
            "include": list(self.include),
            "mappings": self.mappings,
            "files": dict(sorted(self.files.items())),
        }
        path.write_text(json.dumps(doc, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    def local_path(self, core_path: str) -> Path:
        """Where a core file is vendored in this repository."""
        for prefix, dest in sorted(self.mappings.items(), key=lambda kv: -len(kv[0])):
            if core_path.startswith(prefix):
                return ROOT / dest / core_path.removeprefix(prefix)
        raise ValueError(f"no mapping for core path {core_path!r}")

    def is_included(self, core_path: str) -> bool:
        return any(
            core_path.startswith(p) if p.endswith("/") else core_path == p for p in self.include
        )


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _get(url: str) -> bytes:
    if urllib.parse.urlsplit(url).scheme != "https":
        raise ValueError(f"refusing non-https URL {url!r}")
    req = urllib.request.Request(url, headers={"User-Agent": "manufacture-sdk-contract-sync"})
    token = os.environ.get("GITHUB_TOKEN")
    if token and url.startswith(API):
        req.add_header("Authorization", f"Bearer {token}")
    with urllib.request.urlopen(req, timeout=TIMEOUT_S) as resp:
        return cast("bytes", resp.read())


def resolve_commit(repository: str, ref: str) -> str:
    """Resolve a branch, tag or short SHA to a full commit SHA."""
    doc = json.loads(_get(f"{API}/repos/{repository}/commits/{urllib.parse.quote(ref)}"))
    return str(doc["sha"])


def list_files(repository: str, commit: str) -> list[str]:
    """All blob paths of the core repository at ``commit``."""
    doc = json.loads(_get(f"{API}/repos/{repository}/git/trees/{commit}?recursive=1"))
    if doc.get("truncated"):
        raise RuntimeError("GitHub returned a truncated tree; sync from a smaller repository")
    return [str(e["path"]) for e in doc["tree"] if e["type"] == "blob"]


def fetch_raw(repository: str, commit: str, path: str) -> bytes:
    return _get(f"{RAW}/{repository}/{commit}/{urllib.parse.quote(path)}")


def fetch_contract(lock: Lock, commit: str) -> dict[str, bytes]:
    """Download every included file of the core at ``commit``."""
    paths = sorted(p for p in list_files(lock.repository, commit) if lock.is_included(p))
    if not paths:
        raise RuntimeError(f"nothing matches include={list(lock.include)} at {commit}")
    return {p: fetch_raw(lock.repository, commit, p) for p in paths}


def diff(lock: Lock, upstream: dict[str, bytes]) -> list[str]:
    """Human-readable differences between the vendored copy and ``upstream``."""
    problems: list[str] = []
    for path, data in upstream.items():
        local = lock.local_path(path)
        if not local.is_file():
            problems.append(f"missing locally: {path}")
        elif local.read_bytes() != data:
            problems.append(f"content differs: {path}")
    problems.extend(f"removed upstream: {p}" for p in sorted(set(lock.files) - set(upstream)))
    return problems


def write(lock: Lock, commit: str, upstream: dict[str, bytes]) -> Lock:
    for path in set(lock.files) - set(upstream):
        lock.local_path(path).unlink(missing_ok=True)
    for path, data in upstream.items():
        target = lock.local_path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)
    version = upstream["schemas/VERSION"].decode().strip()
    new = Lock(
        repository=lock.repository,
        commit=commit,
        version=version,
        include=lock.include,
        mappings=lock.mappings,
        files={p: sha256(d) for p, d in upstream.items()},
    )
    new.dump()
    return new


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0] if __doc__ else None)
    parser.add_argument("--ref", help="core commit, branch or tag (default: pinned commit)")
    parser.add_argument("--check", action="store_true", help="only compare, do not write")
    args = parser.parse_args(argv)

    lock = Lock.load()
    commit = resolve_commit(lock.repository, args.ref) if args.ref else lock.commit
    upstream = fetch_contract(lock, commit)

    if args.check:
        problems = diff(lock, upstream)
        label = f"{lock.repository}@{commit[:12]}"
        if problems:
            print(f"contract drift against {label} (pinned {lock.commit[:12]}):")
            for p in problems:
                print(f"  - {p}")
            return 1
        print(f"vendored contract matches {label} ({len(upstream)} files)")
        return 0

    new = write(lock, commit, upstream)
    print(
        f"vendored {len(new.files)} files from {lock.repository}@{commit} (contract {new.version})"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())

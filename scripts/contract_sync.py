#!/usr/bin/env python3
"""Vendor the Manufacture contract from a ``manufacture-core`` contract release.

The core publishes ``contract-vX.Y.Z.tar.gz`` and ``SHA256SUMS`` on the GitHub release
``contract-vX.Y.Z``. The lock file ``src/manufacture/_contract/contract.lock.json`` pins
that release and the archive checksum, says *what* is vendored (``include``: paths inside
the archive, a trailing ``/`` means a whole directory) and *where* it goes (``mappings``:
archive prefix -> path in this repository), and records the sha256 of every file.

Usage::

    python scripts/contract_sync.py --release contract-v0.2.1   # vendor, rewrite the lock
    python scripts/contract_sync.py --check                     # compare with the pinned release
    python scripts/contract_sync.py --check --ref main          # report drift against core main

``--ref`` compares with a branch or commit of the core repository instead of a release
(``repo_paths`` maps archive paths to repository paths where they differ). Only the
standard library is used. ``GITHUB_TOKEN`` raises the GitHub API rate limit.
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import sys
import tarfile
import urllib.parse
import urllib.request
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, cast

ROOT = Path(__file__).resolve().parent.parent
LOCK_PATH = ROOT / "src" / "manufacture" / "_contract" / "contract.lock.json"
API = "https://api.github.com"
RAW = "https://raw.githubusercontent.com"
DOWNLOAD = "https://github.com"
TIMEOUT_S = 60


@dataclass(frozen=True)
class Lock:
    """Parsed ``contract.lock.json``."""

    repository: str
    release: str
    archive_sha256: str
    commit: str
    version: str
    include: tuple[str, ...]
    mappings: dict[str, str]
    repo_paths: dict[str, str]
    files: dict[str, str]

    @classmethod
    def load(cls, path: Path = LOCK_PATH) -> Lock:
        raw = cast("dict[str, Any]", json.loads(path.read_text(encoding="utf-8")))
        return cls(
            repository=str(raw["repository"]),
            release=str(raw["release"]),
            archive_sha256=str(raw["archive_sha256"]),
            commit=str(raw["commit"]),
            version=str(raw["version"]),
            include=tuple(str(p) for p in raw["include"]),
            mappings={str(k): str(v) for k, v in raw["mappings"].items()},
            repo_paths={str(k): str(v) for k, v in raw.get("repo_paths", {}).items()},
            files={str(k): str(v) for k, v in raw["files"].items()},
        )

    def dump(self, path: Path = LOCK_PATH) -> None:
        doc = {
            "repository": self.repository,
            "release": self.release,
            "archive_sha256": self.archive_sha256,
            "commit": self.commit,
            "version": self.version,
            "include": list(self.include),
            "mappings": self.mappings,
            "repo_paths": self.repo_paths,
            "files": dict(sorted(self.files.items())),
        }
        path.write_text(json.dumps(doc, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    def local_path(self, path: str) -> Path:
        """Where an archive file is vendored in this repository."""
        for prefix, dest in sorted(self.mappings.items(), key=lambda kv: -len(kv[0])):
            if path == prefix or (prefix.endswith("/") and path.startswith(prefix)):
                return ROOT / (dest if path == prefix else dest + path.removeprefix(prefix))
        raise ValueError(f"no mapping for contract path {path!r}")

    def is_included(self, path: str) -> bool:
        return any(path.startswith(p) if p.endswith("/") else path == p for p in self.include)

    def repo_path(self, path: str) -> str:
        """Path of an archive file inside the core repository."""
        return self.repo_paths.get(path, path)


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


def fetch_release(repository: str, release: str) -> tuple[str, dict[str, bytes]]:
    """Download the release archive, verify it against ``SHA256SUMS``, return its files.

    Returns the archive checksum and ``{path inside the archive: content}``.
    """
    base = f"{DOWNLOAD}/{repository}/releases/download/{release}"
    archive_name = f"{release}.tar.gz"
    archive = _get(f"{base}/{archive_name}")
    sums = _get(f"{base}/SHA256SUMS").decode()
    expected = {
        name.lstrip("*"): digest
        for digest, name in (ln.split() for ln in sums.splitlines() if ln.strip())
    }
    digest = sha256(archive)
    if expected.get(archive_name) != digest:
        raise RuntimeError(f"{archive_name}: sha256 {digest} does not match SHA256SUMS")
    files: dict[str, bytes] = {}
    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as tar:
        for member in tar.getmembers():
            if not member.isfile():
                continue
            top, _, rel = member.name.partition("/")
            if top != release or not rel or ".." in rel.split("/"):
                raise RuntimeError(f"unexpected archive member {member.name!r}")
            data = tar.extractfile(member)
            if data is not None:
                files[rel] = data.read()
    return digest, files


def fetch_ref(lock: Lock, commit: str) -> dict[str, bytes]:
    """Files of the core repository at ``commit``, keyed by archive path."""
    doc = json.loads(_get(f"{API}/repos/{lock.repository}/git/trees/{commit}?recursive=1"))
    if doc.get("truncated"):
        raise RuntimeError("GitHub returned a truncated tree")
    repo_files = {str(e["path"]) for e in doc["tree"] if e["type"] == "blob"}
    reverse = {v: k for k, v in lock.repo_paths.items()}
    out: dict[str, bytes] = {}
    for repo_path in sorted(repo_files):
        path = reverse.get(repo_path, repo_path)
        if lock.is_included(path) and not path.endswith((".go", "README.md")):  # not in archives
            out[path] = _get(f"{RAW}/{lock.repository}/{commit}/{urllib.parse.quote(repo_path)}")
    return out


def included(lock: Lock, files: dict[str, bytes]) -> dict[str, bytes]:
    picked = {p: d for p, d in files.items() if lock.is_included(p)}
    if not picked:
        raise RuntimeError(f"nothing matches include={list(lock.include)}")
    return picked


def diff(lock: Lock, upstream: dict[str, bytes]) -> list[str]:
    """Human-readable differences between the vendored copy and ``upstream``."""
    problems: list[str] = []
    for path, data in sorted(upstream.items()):
        local = lock.local_path(path)
        if not local.is_file():
            problems.append(f"missing locally: {path}")
        elif local.read_bytes() != data:
            problems.append(f"content differs: {path}")
    problems.extend(f"removed upstream: {p}" for p in sorted(set(lock.files) - set(upstream)))
    return problems


def write(lock: Lock, upstream: dict[str, bytes]) -> Lock:
    for path in set(lock.files) - set(upstream):
        lock.local_path(path).unlink(missing_ok=True)
    for path, data in upstream.items():
        target = lock.local_path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)
    new = replace(
        lock,
        version=upstream["schemas/VERSION"].decode().strip(),
        files={p: sha256(d) for p, d in upstream.items()},
    )
    new.dump()
    return new


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0] if __doc__ else None)
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--release", help="contract release tag, e.g. contract-v0.2.1")
    group.add_argument("--ref", help="core branch or commit (only with --check)")
    parser.add_argument("--check", action="store_true", help="only compare, do not write")
    args = parser.parse_args(argv)
    lock = Lock.load()

    if args.ref:
        if not args.check:
            parser.error("--ref is for --check only; vendor from a release with --release")
        commit = resolve_commit(lock.repository, args.ref)
        upstream, label = fetch_ref(lock, commit), f"{lock.repository}@{commit[:12]} ({args.ref})"
    else:
        release = args.release or lock.release
        digest, files = fetch_release(lock.repository, release)
        if args.check and release == lock.release and digest != lock.archive_sha256:
            print(f"{release}: archive sha256 changed ({digest}), the release was re-published")
            return 1
        upstream, label = included(lock, files), f"{lock.repository} release {release}"
        if not args.check:
            commit = resolve_commit(lock.repository, release)
            lock = replace(lock, release=release, archive_sha256=digest, commit=commit)
            new = write(lock, upstream)
            print(
                f"vendored {len(new.files)} files from {label} ({commit[:12]}), contract {new.version}"
            )
            return 0

    problems = diff(lock, upstream)
    if problems:
        print(f"contract drift against {label} (pinned {lock.release}):")
        for p in problems:
            print(f"  - {p}")
        return 1
    print(f"vendored contract matches {label} ({len(upstream)} files)")
    return 0


if __name__ == "__main__":
    sys.exit(main())

# ADR 0002: Vendored contract pinned to a core contract release, with a drift test

Status: accepted (S1), 2026-11-10; revised for contract 0.2.1 the same day

## Context
`manufacture-core` owns the contract. On every tag `contract-vX.Y.Z` its CI publishes
`contract-vX.Y.Z.tar.gz` (schemas, testdata, `messaging.md`, `openapi.yaml`, `CHANGELOG.md`)
and `SHA256SUMS` on the GitHub release (architecture doc §1.3). The first S1 draft pinned a
commit (`ce6e1f7`, contract 0.1.0) because no release existed; `contract-v0.2.1`
(commit `3299ffe`) is the first release with the envelope schemas (C2).

## Decision
- The SDK vendors a **contract release**:
  `schemas/VERSION`, `schemas/v1/`, `schemas/envelope/v2/` into
  `src/manufacture/_contract/schemas/` (shipped in the wheel), `testdata/` (resource specs
  and the envelope conformance vectors `testdata/envelope/`) into `tests/contract/testdata/`,
  and `messaging.md` into `docs/contract/messaging.md`.
- `src/manufacture/_contract/contract.lock.json` records the repository, the release tag,
  the archive sha256, the commit the tag points to, the contract version, the `include` list
  (paths inside the archive; a trailing `/` means a whole directory), `mappings`
  (archive prefix → local path), `repo_paths` (archive path → repository path where they
  differ) and the sha256 of every vendored file.
- `scripts/contract_sync.py` (stdlib only) is the single writer:
  `make contract-sync RELEASE=contract-vX.Y.Z` downloads the archive, verifies it against
  `SHA256SUMS`, extracts the included paths and rewrites the lock. Each update is its own PR.
- Drift test (`tests/contract/test_drift.py`):
  - offline, always: every vendored file matches its lock hash, nothing untracked sits in
    the vendored directories, `VERSION` equals the lock version;
  - online (`MF_CONTRACT_ONLINE=1`, the `contract-drift` CI job): downloads the pinned
    release, checks the archive checksum against `SHA256SUMS` and the lock (a re-published
    release is drift too) and compares every file byte by byte, including the file list.
- Nightly workflow `contract-nightly` compares with core `main` (`--check --ref main`) and
  only reports (job summary + warning), it never fails.
- Conformance: `tests/contract/test_testdata.py` runs the resource testdata through
  `manufacture.contract.validate_resource`; `tests/contract/test_envelope_vectors.py` runs
  `testdata/envelope/{valid,invalid}` through `validate_envelope`.

## Adding contract files
Add the archive path to `include` (and `mappings` if it is a new top-level prefix), run
`make contract-sync RELEASE=...`. Hash checks, online drift and the wheel pick it up.

## Consequences
- The SDK never edits schemas; contract changes go through the core.
- Pydantic models of `Module`/`Pipeline` resources are not part of S1: resources are
  validated by the JSON Schemas themselves, identical to the core by construction.

# ADR 0002: Vendored contract pinned to a core commit, with a drift test

Status: accepted (S1), 2026-11-10

## Context
`manufacture-core` owns the contract (`schemas/`). The architecture doc (§1.3) plans to
vendor it from the `contract-vX.Y.Z.tar.gz` release asset. The core has no releases yet
(C1 = `ce6e1f7`, contract `0.1.0`), and the message envelope schemas (C2) are not merged.

## Decision
- The SDK vendors core files at a **pinned full commit SHA**:
  `schemas/VERSION` and `schemas/v1/` into `src/manufacture/_contract/schemas/` (shipped in
  the wheel), `testdata/` into `tests/contract/testdata/` (tests only).
- `src/manufacture/_contract/contract.lock.json` records the repository, commit, contract
  version, the `include` list (core paths; a trailing `/` means the whole directory),
  `mappings` (core prefix → local path) and the sha256 of every vendored file.
- `scripts/contract_sync.py` (stdlib only) is the single writer: `make contract-sync
  REF=<sha>` downloads the included paths from GitHub (tree API + raw files), rewrites the
  vendored files and the lock. Each update is its own PR.
- Drift test (`tests/contract/test_drift.py`):
  - offline, always: every vendored file matches its lock hash, nothing untracked sits in
    the vendored directories, `VERSION` equals the lock version;
  - online (`MF_CONTRACT_ONLINE=1`, the `contract-drift` CI job): downloads the pinned
    commit and compares byte by byte, including the file list.
- Nightly workflow `contract-nightly` compares with core `main` and only reports
  (job summary + warning), it never fails.
- `tests/contract/test_testdata.py` runs core `testdata/` through the SDK validator:
  `valid/` passes, `invalid/schema/` is rejected at the paths listed in `# want:`,
  `invalid/rules/` is schema-valid (semantic rules belong to `manufacture validate`).

## Adding the envelope schemas (C2) and other contract files
Add the core path (for example `schemas/envelope/v2/`) to `include` in the lock file and
run `make contract-sync REF=<sha of core main with C2>`. Everything else (hash check,
online drift, wheel content) picks the new files up automatically.

## Switch to release archives
When the core publishes `contract-vX.Y.Z.tar.gz` with `SHA256SUMS`, the sync script gets a
`--release vX.Y.Z` mode that verifies the archive checksum; the lock keeps both the tag and
the commit it points to. Until then the commit pin is the source of truth.

## Consequences
- The SDK never edits schemas; contract changes go through the core.
- Pydantic models of `Module`/`Pipeline` resources are not part of S1: the SDK validates
  resources with the JSON Schemas themselves, which keeps it identical to the core by
  construction.

# ADR 0001: Package layout and tooling

Status: accepted (S1), 2026-11-10

## Context
The SDK is the only supported way to write Manufacture modules in Python (architecture
doc v2, D7). It must be strictly typed and cheap to keep green across Python versions.

## Decision
- Distribution `manufacture-sdk`, import package `manufacture`, src-layout
  (`src/manufacture`), `py.typed`. Build backend hatchling, environments and lock via `uv`
  (`uv.lock` is committed; CI runs `uv sync --locked`).
- Python `>=3.12`; CI tests 3.12, 3.13 and 3.14.
- Quality gates, all blocking in CI and runnable with `make check`:
  `ruff check` (broad rule set incl. bugbear, bandit, pylint, annotations),
  `ruff format --check`, `mypy --strict` over `src`, `tests` and `scripts`, `pytest`
  with `pytest-asyncio` (auto mode) and `filterwarnings = error`.
- Runtime dependencies stay minimal and are added by the PR that needs them
  (S1: `jsonschema`, `referencing` only).

## Consequences
- Warnings from dependencies fail tests; a noisy dependency upgrade needs an explicit
  filter with a reason.
- Releases (S5) are built with `uv build`; the wheel ships the vendored contract.

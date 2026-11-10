# manufacture-sdk-python

Python SDK for [Manufacture](https://github.com/t1menar/manufacture-core) pipeline modules.

> Status: pre-alpha. This repository implements the plan in the core architecture doc
> (§5.5, §11.2).

## Development

Requires [uv](https://docs.astral.sh/uv/) and Python 3.12+.

```bash
make sync        # uv sync --locked
make check       # ruff (lint + format), mypy --strict, pytest
make test-online # drift test: compares the vendored contract with GitHub
```

## Contract

The core repository owns the contract and publishes it as a release archive
(`contract-vX.Y.Z.tar.gz` + `SHA256SUMS`). The SDK keeps a byte-identical copy:

| What | Vendored to | Shipped in wheel |
|---|---|---|
| `schemas/VERSION`, `schemas/v1/*` (Module, Pipeline), `schemas/envelope/v2/*` (messages, blob ref) | `src/manufacture/_contract/schemas/` | yes |
| `testdata/` (resource specs, envelope conformance vectors) | `tests/contract/testdata/` | no |
| `messaging.md` (module protocol) | `docs/contract/messaging.md` | no |

Pinned: release [`contract-v0.2.1`](https://github.com/t1menar/manufacture-core/releases/tag/contract-v0.2.1)
(core commit `3299ffe`), see
[`contract.lock.json`](src/manufacture/_contract/contract.lock.json).

```bash
make contract-sync RELEASE=contract-vX.Y.Z   # vendor another release (separate PR)
make contract-check                          # compare with the pinned release
make contract-check REF=main                 # see what changed on core main
```

New contract files are tracked by adding their archive path to `include` in the lock file
and re-running the sync; see [ADR 0002](docs/adr/0002-vendored-contract-and-drift-test.md).

Validate a resource against the vendored schemas:

```python
import yaml
from manufacture.contract import validate_resource

for doc in yaml.safe_load_all(open("pipeline.yaml")):
    for err in validate_resource(doc):
        print(err)  # spec.stages[0].step.timeout: '1.5h' does not match ...
```

## Decisions

Architecture decisions are recorded in [`docs/adr/`](docs/adr/).

## License

MIT

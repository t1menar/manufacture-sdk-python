# manufacture-sdk-python

Python SDK for [Manufacture](https://github.com/t1menar/manufacture-core) pipeline modules.

> Status: pre-alpha. This repository implements the plan in the core architecture doc
> (§5.5, §11.2): S1 package skeleton and vendored contract.

## Development

Requires [uv](https://docs.astral.sh/uv/) and Python 3.12+.

```bash
make sync        # uv sync --locked
make check       # ruff (lint + format), mypy --strict, pytest
make test-online # drift test: compares the vendored contract with GitHub
```

## Contract

The core repository owns the contract. The SDK keeps a byte-identical copy:

| What | Vendored to | Shipped in wheel |
|---|---|---|
| `schemas/VERSION`, `schemas/v1/*.schema.json` (Module, Pipeline) | `src/manufacture/_contract/schemas/` | yes |
| `testdata/` (valid and invalid specs) | `tests/contract/testdata/` | no |

The pinned core commit, the list of tracked paths and the file hashes live in
[`src/manufacture/_contract/contract.lock.json`](src/manufacture/_contract/contract.lock.json).
Currently pinned: `t1menar/manufacture-core@ce6e1f7` (C1), contract `0.1.0`.

```bash
make contract-sync REF=<core commit sha>   # vendor another commit (separate PR)
make contract-check                        # compare with the pinned commit
make contract-check REF=main               # see what changed on core main
```

> **Envelope schemas are not vendored yet.** The message envelope (`schemas/envelope/v2`,
> core PR C2) does not exist on core `main` at the pinned commit. When it lands, add
> `"schemas/envelope/v2/"` to `include` in the lock file and run `make contract-sync`;
> the drift test covers new files automatically. See
> [ADR 0002](docs/adr/0002-vendored-contract-and-drift-test.md).

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

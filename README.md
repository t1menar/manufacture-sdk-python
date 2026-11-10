# manufacture-sdk-python

Python SDK for [Manufacture](https://github.com/t1menar/manufacture-core) pipeline modules.

> Status: pre-alpha. This repository implements the plan in the core architecture doc
> (§5.5, §11.2).

## Writing a module

```python
from pydantic import BaseModel

from manufacture import Context, Module


class Text(BaseModel):
    text: str


app = Module("hello-hello", version="0.1.0", concurrency=4)


@app.step(input=Text, output=Text)
async def handle(ctx: Context, inp: Text) -> Text:
    await ctx.progress(0.5, "halfway")  # also a heartbeat
    return Text(text=inp.text + "hello")


if __name__ == "__main__":
    app.run()  # reads MF_* from the environment
```

Sources start runs:

```python
from collections.abc import AsyncIterator

from manufacture import RunRequest, SourceContext


@app.source(pipeline="hello-world")  # on source.trigger (temporary: on_start=True / every="1h")
async def start(ctx: SourceContext) -> AsyncIterator[RunRequest]:
    yield RunRequest(data={"text": ""}, dedup_key=ctx.trigger_id)
```

Run and inspect:

```bash
MF_MODULE_NAME=hello-hello MF_AMQP_URL=amqp://user:pass@rabbitmq:5672/ mf-module run hello:app
mf-module manifest hello:app                          # what module.online announces
echo '{"text": ""}' > in.json && mf-module test hello:app --input in.json   # no broker
```

| Env | Default | Meaning |
|---|---|---|
| `MF_MODULE_NAME` | `Module(name)` | must match the module name when set |
| `MF_REPLICA_ID` | `<module>-<random>` | replica id (control queue, heartbeats) |
| `MF_AMQP_URL` | `amqp://guest:guest@localhost:5672/` | broker (secret, never logged) |
| `MF_CONCURRENCY` | `Module(concurrency=)` | parallel steps = prefetch |
| `MF_HEALTH_ADDR` | `:8081` | `/healthz`, `/readyz`; empty or `off` disables |
| `MF_LOG_LEVEL` | `info` | JSON logs on stdout |

What the runtime guarantees (late ack, idempotency by `attempt.id`, heartbeats, deadline,
cancel, graceful shutdown) and the open contract questions are in
[ADR 0003](docs/adr/0003-module-runtime.md). `on_start` and `every` run in every replica and
are temporary: schedules are moving to the core.

Unit-test handlers without a broker with `manufacture.testing`:

```python
from manufacture.testing import call_on_start, call_source, call_step

result = await call_step(app, {"text": ""})  # step.succeeded / step.failed
assert result.output == {"text": "hello"}

runs = (await call_source(init_app, {"n": 2})).runs  # one source.trigger -> run.requested payloads
runs = (await call_on_start(init_app)).runs  # what on_start requests (temporary)
```

Every message the module publishes in these helpers is validated against the contract schema.

## Development

Requires [uv](https://docs.astral.sh/uv/) and Python 3.12+.

```bash
make sync        # uv sync --locked
make check       # ruff (lint + format), mypy --strict, pytest
make test-online # drift test: compares the vendored contract with GitHub

# runtime against a real RabbitMQ (CI uses a rabbitmq:4 service container)
make test-integration MF_TEST_AMQP_URL=amqp://guest:guest@127.0.0.1:5672/
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

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

### Running a whole pipeline without the platform

`manufacture.fakecore` executes a pipeline from its spec (linear stages, timeouts, leases,
retries, optional steps, merge, `max_runs`, cancel) against your real modules, over an
in-memory broker or a real RabbitMQ. Parallel/foreach/routes/`when`/input projections
are refused, not approximated. Details and limits: [ADR 0004](docs/adr/0004-fake-core.md).

```python
from manufacture.fakecore import FakeCore

async with FakeCore("pipeline.yaml", [init.app, hello.app, world.app, exclaim.app]) as core:
    run = await core.run("hello-world", {"text": ""})
    assert run.status == "succeeded" and run.data == {"text": "hello world!"}

# the same over RabbitMQ; modules not passed as code may connect by themselves
FakeCore("pipeline.yaml", [hello.app], broker="amqp", amqp_url="amqp://...")
```

```bash
pip install 'manufacture-sdk[fakecore]'   # PyYAML for YAML specs
mf-fakecore -f pipeline.yaml modules/*/main.py --run hello-world --data '{"text": ""}'
```

## Contract 0.3.0 in the SDK

| Need | API |
|---|---|
| Wait for a human or a service (R1) | `await ctx.wait(reason="human", message=..., expected_by=...)` sends `step.waiting`, acks the delivery and stops the handler; keep `str(ctx.attempt_handle)` and `ctx.step_key`; finish later from any replica with `await app.complete(handle, output)` / `await app.fail(handle, error)` |
| Visit counter and idempotency key (R2) | `ctx.step_key`, `ctx.visit`, `ctx.labels`, `ctx.budget`, `ctx.priority` |
| Confirm before an irreversible action (R4) | `await ctx.confirm(action=...)` (needs `MF_CORE_URL`, `MF_MODULE_TOKEN`); raises `Superseded` / `Cancelled` / `DeadlineTooClose`, a refusal that escapes the handler reports nothing |
| Usage and cost (R5) | `ctx.add_usage(cost_usd, model=..., input_tokens=...)`: cumulative, sent with heartbeats, waiting and the result |
| Run lifecycle (R6) | `@app.on_run_started`, `@app.on_run_finished` (queue `mf.notify.<module>`, needs `subscribe` in the Module resource) |
| Run priority and labels | `RunRequest(priority=..., labels={...})` |
| Attention details | `NeedsAttention(msg, recommendation=..., details={...})` |

Test helpers: `call_step(..., visit=, budget=, labels=, core=<httpx handler>)` (a waiting step returns
`result.waiting`), `call_run_event(app, run_started(...) | run_finished(...))`. The in-process fake core
runs the 0.3.0 timing rules (timeout from the first heartbeat, pause while waiting) but still has no
`routes`, CEL projections, visits, budgets or notifications.

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

Pinned: release [`contract-v0.3.0`](https://github.com/t1menar/manufacture-core/releases/tag/contract-v0.3.0)
(core commit `0ca4450`), see
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

# ADR 0003: Module runtime (S2)

- Status: accepted
- Date: 2026-11-10
- Contract: 0.2.1 (`docs/contract/messaging.md`, `schemas/envelope/v2`)

## Context

A module author should write one `async def` and get the whole replica side of the
messaging protocol for free: topology, lifecycle messages, heartbeats, deadlines,
cancellation, retries classification, idempotency, and graceful shutdown.

## Decision

### Public API

```python
app = Module("hello-hello", version="0.1.0", concurrency=4)  # settings=MySettings optional


@app.step(input=In, output=Out)  # one step handler per module
async def handle(ctx: Context, inp: In) -> Out: ...


@app.source(pipeline="hello-world", on_start=False, every=None)
async def src(ctx: SourceContext) -> AsyncIterator[RunRequest]:
    yield RunRequest(data={...})


app.run()  # or: mf-module run pkg.mod:app
```

Errors: `Transient(code, retry_after_s=)`, `Fatal(code)`, `NeedsAttention(code)` map to
`step.failed.class` `transient | fatal | attention`. Any other exception is `transient`
with code `unhandled`; invalid input or config is `fatal` (`input_invalid`,
`config_invalid`); an output that fails the output model is `fatal` (`output_invalid`).

### Wire layer is schema-driven

`manufacture._wire.decode` first validates the raw JSON with the vendored
`envelope/v2` JSON Schema (`manufacture.contract.validate_envelope`), then parses it into
pydantic models with `extra="forbid"`. Everything the SDK publishes goes through the same
models; `manufacture.testing.FakeTransport` validates every published message against the
schema, so the whole unit-test suite doubles as a conformance test. The core's
`testdata/envelope` vectors are decoded (valid) and rejected (invalid) in `tests/unit/test_wire.py`.

### Topology and lifecycle (messaging.md §2–3)

- Work: quorum queue `mf.module.<name>` (`x-delivery-limit=5`, DLX `mf.dlx`), bound to
  direct `mf.commands` with key `module.<name>`; `prefetch = MF_CONCURRENCY`, manual ack.
  The SDK re-declares and re-binds it idempotently with the exact core arguments.
- Control: `mf.control.<name>.<replica>` (exclusive, auto-delete) bound to topic
  `mf.control` with `control.<name>`.
- Events: published to topic `mf.events` with routing key = `type`, persistent,
  `content_type=application/json`, `message_id`/`type` properties, publisher confirms,
  `mandatory=true` (an unroutable event is an error, not a silent drop).
- Exchanges are owned by the core and only checked passively; a missing exchange fails
  `start()`. `MF_DECLARE_TOPOLOGY=true` declares them too (local experiments without a core).
- `module.online` once after consuming starts; `module.heartbeat` every 10 s;
  `module.offline` on graceful stop.
- `/healthz` (alive) and `/readyz` (broker connected, consumer running) on `MF_HEALTH_ADDR`
  (default `:8081`, empty or `off` disables).

### Execution guarantees

- **Late ack**: a `step.requested` delivery is acked only after its result is confirmed by
  the broker. Publishing failures retry with backoff up to `publish_timeout_s`, after which
  the delivery is requeued.
- **Result id** `uuid5(NS, "<attempt_id>:<type>")`, so a re-sent result is applied once.
- **Duplicates**: a duplicate of an attempt running in this replica is acked at once without
  a second start (messaging.md §4). A duplicate of an attempt that finished here re-publishes
  the cached outcome (bounded LRU, 10 000 attempts).
- **Heartbeat** every `lease_seconds / 3` (not more often than `min_heartbeat_s`) plus each
  `ctx.progress(...)`; uuid4 message ids.
- **Deadline**: the handler is cancelled at `payload.deadline`; nothing is published (the
  core already marked the attempt `timed_out`). An already expired request is acked and dropped.
- **Cancel**: `step.cancel` sets `ctx.cancelled`, waits `cancel_grace_s`, then cancels the
  task. Nothing is published; the delivery is acked.
- **Shutdown** (SIGTERM/SIGINT): stop consuming, wait up to `shutdown_grace_s` (25 s, below
  the Kubernetes default 30 s), then cancel the rest and publish
  `transient/module_shutdown` so the core retries at once instead of waiting for the lease.
- **Sources**: `source.trigger` arrives on the work queue and is dispatched by
  `payload.pipeline`; each yielded `RunRequest` becomes `run.requested`. The delivery is acked
  after all runs are confirmed; a failing source or unknown pipeline dead-letters it.
- **Malformed messages** (invalid JSON, schema violations, unknown type) are rejected without
  requeue and land in `mf.dlx`.

### Settings

`RuntimeSettings` reads `MF_MODULE_NAME`, `MF_REPLICA_ID` (default `<module>-<random>`),
`MF_AMQP_URL` (a `SecretStr`, never logged), `MF_CONCURRENCY`, `MF_HEALTH_ADDR`,
`MF_LOG_LEVEL`, plus SDK knobs (`MF_SHUTDOWN_GRACE_S`, `MF_CANCEL_GRACE_S`, ...). If
`MF_MODULE_NAME` is set it must match `Module(name)`. `MF_S3_*` is for S3 (blob store).

## Compromises — questions for the core author

These are isolated in the runtime and easy to change once answered:

1. **`run.requested` message id.** The contract defines ids only for step results. The SDK uses
   `uuid5(NS, "run.requested:<pipeline>:<dedup_key>")` so a re-published request is
   deduplicated by message id as well as by `dedup_key`. OK, or should it be uuid4?
2. **Default `dedup_key`.** When a source does not set one, the SDK uses
   `"<trigger_id>:<index>"`. Is that what the core expects, or should it be omitted?
3. **`trigger_id` for `on_start`/`every` runs.** Those runs have no core trigger, so the SDK does
   not send `trigger_id` (it must be a uuid of a core trigger?). Confirm.
4. **`on_start`/`every` with several replicas** produce one run per replica (deduplicated only if
   the source sets a stable `dedup_key`). Should these move to core-side schedules
   (`source.trigger`) only?
5. **Unknown pipeline in `source.trigger`** is dead-lettered. Should it rather be acked and
   reported (there is no `source.failed` message)?
6. **Shutdown error**: `class=transient, code=module_shutdown`. Is a reserved code wanted?
7. **`module.online` after a reconnect.** Sent once per process start. Should it be re-sent
   when the robust connection reconnects (new control queue)?
8. **Long steps vs `consumer_timeout`.** With late ack, a step longer than RabbitMQ's
   `consumer_timeout` (30 min default) closes the channel. Does the core set a larger
   `consumer_timeout` (per queue `x-consumer-timeout`) for `mf.module.*`? The SDK cannot add
   arguments the core does not declare.
9. **`user_id` property.** The SDK sets AMQP `user_id` to the URL user (RabbitMQ validates it).
   Is the core going to rely on it to authenticate the producer?
10. **No step output patching from the module** beyond `output` (no `patch`/`metrics` in 0.2.1).
    Metrics will go to `/metrics` in S3+.
11. **Envelope validation cost.** Every inbound message is validated against the JSON Schema
    (≤ 256 KiB). Fine for now; can drop to pydantic-only if it shows up in profiles.

## Consequences

- Module code is plain async Python; correctness of the protocol lives in one place and is
  covered by unit tests on an in-memory transport and integration tests on a real RabbitMQ
  (CI service container).
- A contract bump is a vendored-schema update plus model changes in `_wire.py`; the drift and
  vector tests point at what changed.

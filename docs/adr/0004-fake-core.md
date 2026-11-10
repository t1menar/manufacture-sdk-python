# ADR 0004: Fake core for tests without the platform (S3)

- Status: proposed
- Date: 2026-11-10
- Contract: 0.2.1 (module protocol); API shapes from core `openapi.yaml`

## Context

Module and pipeline authors need to run a whole pipeline end-to-end in CI and on a
laptop while the core engine (C4) and its runtime driver (C6) are not available, and
later without Docker/Postgres/MinIO for fast feedback. `manufacture.testing` covers one
handler call; nothing covered a pipeline.

## Decision

`manufacture.fakecore` is a test-only engine that executes a pipeline **from its spec**
and speaks the real module protocol to real `Runtime`s.

```python
from manufacture.fakecore import FakeCore

async with FakeCore("pipeline.yaml", [init.app, hello.app, world.app, exclaim.app]) as core:
    run = await core.run("hello-world", {"text": ""})            # API path, 201
    trigger_id = await core.trigger("hello-world")                # source.trigger, 202
    [run] = await core.wait_for_runs("hello-world", trigger_id=trigger_id)
```

CLI: `mf-fakecore -f pipeline.yaml modules/*/main.py --run hello-world --data '{"text": ""}'`
(also `python -m manufacture.fakecore`). YAML needs `manufacture-sdk[fakecore]` (PyYAML).

### What it executes

| Spec | Fake core |
|---|---|
| linear `stages[].step` | yes, in order; default step id = stage name (as `spec.assignStepIDs`) |
| `module.spec.config` + `step.config` | merged by key into `payload.config` |
| `timeout` | from dispatch (0.2.1 `payload.deadline`); then `step.cancel{timeout}`, error class `timeout` |
| `lease` | `payload.lease_seconds`; ticks from the first heartbeat/result of the attempt; then `step.cancel{lost}`, class `lost` |
| `retry` | `max_attempts`, `fixed`/`exponential`, `initial`, `max`, `jitter` (×U(0.5,1), `jitter=False` for exact tests), `on_errors`; `retry_after_seconds` is a lower bound |
| `step.failed` | `transient` retried per policy; `fatal` fails the step; `attention` parks the run in `needs_attention` |
| `optional` | a failed optional step does not fail the run |
| `merge` | `data` (RFC 7396 JSON Merge Patch, `null` deletes) or `none` |
| `context.schema` | checked on `start_run` (raises) and `run.requested` (recorded as a rejection) |
| `concurrency.max_runs` | admission queue; FIFO, higher `priority` first |
| `trigger.source` | `trigger()` sends `source.trigger`; `run.requested` creates runs, dedup by `(pipeline, dedup_key)` |
| cancel | `cancel(run_id)` sends `step.cancel{cancelled}` |
| stale results | results of non-current attempts are dropped and listed in `stale_results` |
| `module.online/heartbeat/offline` | `replicas` registry |
| `parallel`, `foreach`, `routes`, `when`, `join`, `input` projections, `trigger.schedule` | **refused** with `UnsupportedSpecError` (no CEL in the SDK; never approximate) |

Every document is first validated with the vendored contract schemas (`SpecError`).

### Brokers

- `broker="memory"` (default): an in-process model of messaging.md §2 — one work queue
  per module with competing consumers and round-robin, prefetch, ack/requeue,
  `redelivered`, quorum `x-delivery-limit` 5 then dead letters, unacked work returned
  when a replica's transport closes, control fanout to every replica of the module.
  Every module message is validated against the envelope schema; a violation makes the
  next `wait*`/`run` raise `ProtocolViolationError`. All modules used by a pipeline must
  be passed as code.
- `broker="amqp"`: real RabbitMQ (`amqp_url` or `$MF_AMQP_URL`). The fake core declares
  what the core declares (exchanges, quorum work queues with the pinned arguments,
  `mf.core.events` bound to `#`, `mf.dlx` → `mf.dead-letters`) under a random `fc-xxxx.`
  prefix and deletes its queues on exit. In-process replicas use the normal
  `AmqpTransport`; modules not passed as code are *external* and may connect by themselves
  (another process, a container) with `MF_AMQP_PREFIX=<core.prefix>`.
- Or any object implementing the `Bus` protocol.

In-process replicas get `replica_id = <module>-<i>`, replicas from the spec (`int`, or
`min` of a range, at least 1) unless `replicas={...}`, `concurrency` from the spec,
`health_addr=off`, `min_heartbeat_s=0.05`. `kill_replica()` simulates a crash and
`start_replica()` adds one.

### Results

`Run`, `Step`, `Attempt` follow the core API (`openapi.yaml`): run states
`queued → running → succeeded | failed | needs_attention | cancelled`, attempt states
`scheduled/dispatched/running/succeeded/failed/timed_out/lost/cancelled`, `trigger{kind,by}`.
`Run.to_dict()` gives the API JSON shape. `events` lists every state change in order.

### Not breaking at 0.3.0

The public surface is already shaped for 0.3.0, so the migration adds behaviour, not
signatures:

- `Run.status` includes `waiting_human`; step/attempt states include `waiting`;
  `Run.priority`, `Run.labels`, `Run.usage`, `Step.visit`, `Step.step_key` exist now.
  `step_key` already uses the 0.3.0 formula (`STEP_NS = fa1e7bfd-…`, test vector from the
  design) and is the same for every attempt of a visit; under 0.2.1 it is not sent.
- `start_run(..., dedup_key=, priority=, labels=)` matches 0.3.0 `run.requested`/API.
- `rejections` records what 0.3.0 will send as `run.rejected` (same codes).
- `needs_attention` is not terminal, but `wait()` returns on it (as it will for
  `on_timeout: attention`).
- The 0.3.0 timeout start (first heartbeat) is one line in `_attempt`; the lease already
  ticks only after the first sign of life (0.3.0 §4.2), which also avoids false `lost` for
  work waiting behind a busy replica's prefetch in 0.2.1.

Planned for the 0.3.0 PR (after the contract release, with the vendored schemas):
`step.waiting` and `waiting_human`, `on_timeout`/`wait.timeout`/`on_cancel`, `confirm`
with its four outcomes (`ok|superseded|cancelled|deadline`) as an in-process call,
`run.started`/`run.finished` to `mf.notify.<module>`, budget/usage accounting,
`run.rejected`, routes with visits (needs a CEL subset or is refused as today).

## Consequences

- `manufacture.fakecore` is not the core: no database, API server, blobs, CEL, schedules,
  autoscaling, or authentication. Passing on the fake core is necessary, not sufficient;
  the compose e2e against the real core stays the release gate.
- Defaults the core has not published for 0.2.1 are explicit constants in
  `fakecore/_spec.py` (questions below) and are easy to align.

## Questions to the core

1. Defaults when the spec omits them (0.2.1): step `timeout` (fake: 1h), `lease`
   (fake: min(30s, timeout/2)), `max_attempts` when `retry` is present without it
   (fake: 3), `initial`/`max` backoff (fake: 1s/5m), jitter formula (fake: ×U(0.5,1)).
   Without a `retry` block the fake core makes exactly one attempt (as `Retry.Retries(nil)`).
2. Under 0.2.1, when retries of `timeout`/`lost` are exhausted: run `failed` (fake core)
   or `needs_attention` (as 0.3.0 says for `on_timeout: retry`)?
3. Under 0.2.1, does the lease tick from dispatch or from the first heartbeat? The SDK
   sends its first heartbeat only after `lease/3`; ticking from dispatch marks work that
   waits behind prefetch as `lost`.
4. `lease_seconds` is an integer and SDK heartbeats are at least 1 s apart: is a spec
   lease below 3 s rejected, rounded, or allowed?
5. Does the core check that `run.requested` comes from the pipeline's `trigger.source`
   module (0.3.0 has `forbidden`), or may any module start any pipeline?
6. `concurrency.max_runs`: are queued runs admitted strictly FIFO in 0.2.1?
7. `blob_prefix` format: the fake core sends
   `s3://mf-blobs/runs/<run>/<step>/<attempt>/` per messaging.md §6; is the bucket fixed?

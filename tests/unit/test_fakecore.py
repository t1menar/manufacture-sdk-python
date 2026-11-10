"""Fake core over the in-memory broker."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any

import pytest
from pydantic import BaseModel

from manufacture import Context, Fatal, Module, NeedsAttention, RunRequest, SourceContext, Transient
from manufacture.contract import SpecError
from manufacture.fakecore import (
    FakeCore,
    MemoryBus,
    ProtocolViolationError,
    UnsupportedSpecError,
    merge_patch,
    step_key,
)
from manufacture.transport import Identity, Inbound


class Text(BaseModel):
    text: str


class Doc(BaseModel):
    model_config = {"extra": "allow"}


def module_doc(name: str, **spec: Any) -> dict[str, Any]:
    return {
        "apiVersion": "manufacture.dev/v1",
        "kind": "Module",
        "metadata": {"name": name},
        "spec": {"image": f"example/{name}:1", **spec},
    }


def pipeline_doc(name: str, stages: list[dict[str, Any]], **spec: Any) -> dict[str, Any]:
    return {
        "apiVersion": "manufacture.dev/v1",
        "kind": "Pipeline",
        "metadata": {"name": name},
        "spec": {"stages": stages, **spec},
    }


def append(name: str, suffix: str) -> Module:
    app = Module(name)

    @app.step(input=Text, output=Text)
    async def handle(ctx: Context, inp: Text) -> Text:
        return Text(text=inp.text + suffix)

    return app


def hello_spec(**pipeline: Any) -> list[dict[str, Any]]:
    return [
        module_doc("a"),
        module_doc("b", replicas=2),
        pipeline_doc(
            "p",
            [
                {"name": "one", "step": {"module": "a", "timeout": "5s"}},
                {"name": "two", "step": {"id": "second", "module": "b", "timeout": "5s"}},
            ],
            **pipeline,
        ),
    ]


async def test_linear_pipeline_runs_and_merges_outputs() -> None:
    async with FakeCore(hello_spec(), [append("a", "hello"), append("b", " world")]) as core:
        await core.wait_for_replicas(3)
        run = await core.run("p", {"text": "", "keep": 1})
    assert run.status == "succeeded"
    assert run.data == {"text": "hello world", "keep": 1}
    assert [s.step_id for s in run.steps] == ["one", "second"]
    assert run.step("second").output == {"text": "hello world"}
    one = run.step("one")
    assert one.step_key == step_key(run.id, "one", "one", None, 1)
    [attempt] = one.attempts
    assert (attempt.number, attempt.status, attempt.replica_id) == (1, "succeeded", "a-0")
    assert run.trigger.kind == "api"
    assert run.finished_at is not None
    assert {r.replica_id for r in core.replicas} == {"a-0", "b-0", "b-1"}
    run_states = [e.status for e in core.events if e.type == "run"]
    assert run_states == ["queued", "running", "succeeded"]


def source_module(*, on_start: bool = False) -> Module:
    app = Module("src")

    @app.source(pipeline="p", on_start=on_start)
    async def start(ctx: SourceContext) -> AsyncIterator[RunRequest]:
        text = (ctx.data or {}).get("text", "")
        yield RunRequest(data={"text": text}, dedup_key=f"{ctx.trigger_id}:0")

    return app


def source_spec() -> list[dict[str, Any]]:
    return [module_doc("src"), *hello_spec(trigger={"source": "src"})]


async def test_trigger_asks_the_source_module() -> None:
    mods = [source_module(), append("a", "!"), append("b", "?")]
    async with FakeCore(source_spec(), mods) as core:
        trigger_id = await core.trigger("p", {"text": "x"})
        [run] = await core.wait_for_runs("p", trigger_id=trigger_id)
    assert run.status == "succeeded"
    assert run.data == {"text": "x!?"}
    assert (run.trigger.kind, run.trigger.by) == ("source", "src")
    assert run.dedup_key == f"{trigger_id}:0"


async def test_on_start_sources_run_unless_disabled() -> None:
    mods = [source_module(on_start=True), append("a", "!"), append("b", "?")]
    async with FakeCore(source_spec(), mods) as core:
        [run] = await core.wait_for_runs("p")
        assert run.data == {"text": "!?"}
    async with FakeCore(source_spec(), mods, module_schedules=False) as core:
        await core.wait_for_replicas()
        await asyncio.sleep(0.1)
        assert core.runs == []


def flaky(fail_times: int, exc: type[Exception] = Transient, **kw: Any) -> tuple[Module, list[int]]:
    app = Module("a")
    calls: list[int] = []

    @app.step(input=Text, output=Text)
    async def handle(ctx: Context, inp: Text) -> Text:
        calls.append(ctx.attempt)
        if len(calls) <= fail_times:
            raise exc("nope", **kw)
        return Text(text=inp.text + "ok")

    return app, calls


def one_step(
    step: dict[str, Any], *modules: dict[str, Any], **pipeline: Any
) -> list[dict[str, Any]]:
    return [
        module_doc("a"),
        *modules,
        pipeline_doc("p", [{"name": "s", "step": {"module": "a", **step}}], **pipeline),
    ]


RETRY = {"max_attempts": 3, "initial": "10ms", "max": "20ms"}


async def test_transient_errors_are_retried() -> None:
    app, calls = flaky(2)
    async with FakeCore(one_step({"retry": RETRY}), [app], jitter=False) as core:
        run = await core.run("p", {"text": ""})
    assert run.status == "succeeded"
    assert calls == [1, 2, 3]
    assert [a.status for a in run.step("s").attempts] == ["failed", "failed", "succeeded"]


async def test_exhausted_retries_fail_the_run() -> None:
    app, calls = flaky(5)
    async with FakeCore(one_step({"retry": RETRY}), [app], jitter=False) as core:
        run = await core.run("p", {"text": ""})
    assert run.status == "failed"
    assert len(calls) == 3
    assert run.error is not None
    assert (run.error.class_, run.error.message) == ("transient", "nope")


async def test_without_retry_block_there_is_one_attempt() -> None:
    app, calls = flaky(1)
    async with FakeCore(one_step({}), [app]) as core:
        run = await core.run("p", {"text": ""})
    assert (run.status, calls) == ("failed", [1])


async def test_on_errors_limits_retried_classes() -> None:
    app, calls = flaky(1)
    retry = {**RETRY, "on_errors": ["timeout"]}
    async with FakeCore(one_step({"retry": retry}), [app]) as core:
        run = await core.run("p", {"text": ""})
    assert (run.status, calls) == ("failed", [1])


async def test_fatal_is_not_retried_and_optional_steps_do_not_fail_the_run() -> None:
    app, calls = flaky(1, Fatal, code="bad_input")
    spec = [
        module_doc("a"),
        module_doc("b"),
        pipeline_doc(
            "p",
            [
                {"name": "s", "step": {"module": "a", "retry": RETRY, "optional": True}},
                {"name": "t", "step": {"module": "b"}},
            ],
        ),
    ]
    async with FakeCore(spec, [app, append("b", "b")]) as core:
        run = await core.run("p", {"text": ""})
    assert run.status == "succeeded"
    assert calls == [1]
    s = run.step("s")
    assert s.status == "failed"
    assert s.error is not None
    assert s.error.code == "bad_input"
    assert run.data == {"text": "b"}


async def test_attention_parks_the_run() -> None:
    app, _ = flaky(1, NeedsAttention)
    async with FakeCore(one_step({"retry": RETRY}), [app]) as core:
        run = await core.run("p", {"text": ""})
    assert run.status == "needs_attention"
    assert run.finished_at is None


def sleeper(first_sleep: float) -> tuple[Module, list[str]]:
    app = Module("a")
    log: list[str] = []

    @app.step(input=Text, output=Text)
    async def handle(ctx: Context, inp: Text) -> Text:
        log.append(f"start {ctx.attempt}")
        try:
            if ctx.attempt == 1:
                await asyncio.sleep(first_sleep)
        except asyncio.CancelledError:
            log.append(f"cancelled {ctx.attempt}")
            raise
        return Text(text="done")

    return app, log


async def test_timeout_cancels_the_attempt_and_retries() -> None:
    app, log = sleeper(30)
    step = {"timeout": "2s", "lease": "1s", "retry": {**RETRY, "on_errors": ["timeout"]}}
    async with FakeCore(one_step(step), [app], jitter=False) as core:
        run = await core.run("p", {"text": ""}, timeout=10)
    assert run.status == "succeeded", core.describe()
    assert [a.status for a in run.step("s").attempts] == ["timed_out", "succeeded"]
    assert "cancelled 1" in log
    assert core.stale_results == []


async def test_lost_replica_is_detected_by_lease_and_stale_result_dropped() -> None:
    gate = asyncio.Event()
    app = Module("a")

    @app.step(input=Text, output=Text)
    async def handle(ctx: Context, inp: Text) -> Text:
        await gate.wait()
        return Text(text=f"attempt {ctx.attempt}")

    step = {"timeout": "20s", "lease": "1s", "retry": {**RETRY, "on_errors": ["lost"]}}
    async with FakeCore(one_step(step), [app], jitter=False) as core:
        run = await core.start_run("p", {"text": ""})
        await core.wait_until(
            lambda: run.steps and run.steps[0].status == "running", timeout=5, what="running"
        )
        await core.kill_replica("a-0")  # crash: no result; the delivery returns to the queue
        await core.wait_until(
            lambda: len(run.steps[0].attempts) == 2, timeout=5, what="second attempt"
        )
        assert run.steps[0].attempts[0].status == "lost"
        gate.set()
        await core.start_replica(app, "a-1")
        run = await core.wait(run.id, timeout=10)
        await core.wait_until(lambda: bool(core.stale_results), timeout=5, what="stale result")
    assert run.status == "succeeded"
    assert run.data == {"text": "attempt 2"}
    assert core.stale_results[0].reason == "not_current"


async def test_max_runs_queues_extra_runs() -> None:
    gate = asyncio.Event()
    app = Module("a")

    @app.step(input=Text, output=Text)
    async def handle(ctx: Context, inp: Text) -> Text:
        await gate.wait()
        return Text(text=inp.text)

    async with FakeCore(one_step({}, concurrency={"max_runs": 1}), [app]) as core:
        first = await core.start_run("p", {"text": "1"})
        second = await core.start_run("p", {"text": "2"})
        await core.wait_until(lambda: first.status == "running", timeout=5, what="first running")
        await asyncio.sleep(0.05)
        assert second.status == "queued"
        gate.set()
        await core.wait_for_runs("p", count=2)
    assert first.finished_at is not None
    assert second.started_at is not None
    assert first.finished_at <= second.started_at


async def test_dedup_key_returns_the_same_run() -> None:
    async with FakeCore(one_step({}), [append("a", "!")]) as core:
        r1 = await core.start_run("p", {"text": ""}, dedup_key="k")
        r2 = await core.start_run("p", {"text": "other"}, dedup_key="k")
        await core.wait(r1.id)
    assert r1 is r2
    assert len(core.runs) == 1


async def test_cancel_stops_the_handler() -> None:
    app, log = sleeper(30)
    async with FakeCore(one_step({"timeout": "20s"}), [app]) as core:
        run = await core.start_run("p", {"text": ""})
        await core.wait_until(lambda: "start 1" in log, timeout=5, what="started")
        run = await core.cancel(run.id)
        await core.wait_until(lambda: "cancelled 1" in log, timeout=5, what="handler cancelled")
    assert run.status == "cancelled"
    assert run.step("s").attempts[0].status == "cancelled"


async def test_merge_none_and_null_deletes() -> None:
    drop = Module("b")

    @drop.step(input=Doc, output=Doc)
    async def handle(ctx: Context, inp: Doc) -> Doc:
        return Doc.model_validate({"gone": None, "x": {"y": 2}})

    spec = [
        module_doc("a"),
        module_doc("b"),
        pipeline_doc(
            "p",
            [
                {"name": "s", "step": {"module": "a", "merge": "none"}},
                {"name": "t", "step": {"module": "b"}},
            ],
        ),
    ]
    async with FakeCore(spec, [append("a", "!"), drop]) as core:
        run = await core.run("p", {"text": "", "gone": 1, "x": {"z": 1}})
    assert run.data == {"text": "", "x": {"z": 1, "y": 2}}
    assert run.step("s").output == {"text": "!"}


def test_merge_patch_rfc7396() -> None:
    assert merge_patch({"a": "b", "c": {"d": "e", "f": "g"}}, {"a": "z", "c": {"f": None}}) == {
        "a": "z",
        "c": {"d": "e"},
    }
    assert merge_patch({"a": [1]}, {"a": [2]}) == {"a": [2]}
    assert merge_patch({"a": 1}, {"a": {"b": None}}) == {"a": {}}


async def test_context_schema_is_checked() -> None:
    spec = one_step({}, context={"schema": {"type": "object", "required": ["text"]}})
    async with FakeCore(spec, [append("a", "!")]) as core:
        with pytest.raises(ValueError, match="invalid_data"):
            await core.start_run("p", {})
        with pytest.raises(ValueError, match="unknown_pipeline"):
            await core.start_run("nope", {})


async def test_rejected_run_requests_are_recorded() -> None:
    app = Module("src")

    @app.source(pipeline="p")
    async def start(ctx: SourceContext) -> AsyncIterator[RunRequest]:
        yield RunRequest(data={"text": 1})

    schema = {"type": "object", "properties": {"text": {"type": "string"}}}
    spec = [module_doc("src"), *one_step({}, trigger={"source": "src"}, context={"schema": schema})]
    async with FakeCore(spec, [app, append("a", "")]) as core:
        await core.trigger("p")
        await core.wait_until(lambda: bool(core.rejections), timeout=5, what="rejection")
    [rej] = core.rejections
    assert (rej.pipeline, rej.code, rej.producer) == ("p", "invalid_data", "src")
    assert "text" in rej.message
    assert core.runs == []


def test_unsupported_features_are_refused() -> None:
    spec = [
        module_doc("a"),
        pipeline_doc(
            "p",
            [{"name": "s", "parallel": [{"id": "x", "module": "a"}, {"id": "y", "module": "a"}]}],
        ),
    ]
    with pytest.raises(UnsupportedSpecError, match="parallel"):
        FakeCore(spec, [append("a", "")])


def test_invalid_specs_and_missing_code_are_errors() -> None:
    with pytest.raises(SpecError):
        FakeCore([pipeline_doc("p", [])])
    with pytest.raises(ValueError, match="no code for: a"):
        FakeCore(one_step({}))
    with pytest.raises(ValueError, match="not declared"):
        FakeCore(one_step({}), [append("a", ""), append("zzz", "")])


async def test_invalid_envelopes_fail_the_test() -> None:
    async with FakeCore(one_step({}), [append("a", "")]) as core:
        transport = core.bus.transport()
        assert transport is not None
        await transport.connect(Identity("a", "rogue"))
        await transport.publish(
            b'{"type": "step.heartbeat"}', message_id="x", type_="step.heartbeat"
        )
        with pytest.raises(ProtocolViolationError):
            await core.run("p", {"text": ""})
        core.bus.violations.clear()


async def test_memory_bus_dead_letters_after_the_delivery_limit() -> None:
    bus = MemoryBus()
    await bus.start(["m"], on_event=lambda _: asyncio.sleep(0))
    transport = bus.transport()
    await transport.connect(Identity("m", "m-0"))
    seen: list[bool] = []

    async def handler(delivery: Any) -> None:
        seen.append(delivery.redelivered)
        await delivery.reject(requeue=True)

    await transport.consume(Inbound.WORK, handler, prefetch=1)
    await bus.send_work("m", b"{}", message_id="1", type_="step.requested")
    for _ in range(50):
        if bus.dead_letters:
            break
        await asyncio.sleep(0.01)
    assert seen == [False, True, True, True, True, True]
    [dead] = bus.dead_letters
    assert (dead.queue, dead.reason) == ("mf.module.m", "delivery_limit")
    await bus.close()


async def test_memory_bus_redelivers_unacked_work_of_a_closed_replica() -> None:
    bus = MemoryBus()
    await bus.start(["m"], on_event=lambda _: asyncio.sleep(0))
    first, second = bus.transport(), bus.transport()
    await first.connect(Identity("m", "m-0"))
    await second.connect(Identity("m", "m-1"))
    got: list[tuple[str, bool]] = []

    async def hold(delivery: Any) -> None:
        got.append(("m-0", delivery.redelivered))

    async def take(delivery: Any) -> None:
        got.append(("m-1", delivery.redelivered))
        await delivery.ack()

    await first.consume(Inbound.WORK, hold, prefetch=1)
    await bus.send_work("m", b"{}", message_id="1", type_="step.requested")
    await asyncio.sleep(0.01)
    await second.consume(Inbound.WORK, take, prefetch=1)
    await first.close()
    await asyncio.sleep(0.01)
    assert got == [("m-0", False), ("m-1", True)]
    await bus.close()


async def test_yaml_spec_file(tmp_path: Any) -> None:
    path = tmp_path / "pipeline.yaml"
    path.write_text(
        """
apiVersion: manufacture.dev/v1
kind: Module
metadata: {name: a}
spec: {image: example/a:1, config: {greeting: hi}}
---
apiVersion: manufacture.dev/v1
kind: Pipeline
metadata: {name: p}
spec:
  defaults: {timeout: 10s, lease: 2s}
  stages:
    - name: s
      step: {module: a, config: {extra: 1}}
""",
        encoding="utf-8",
    )
    seen: list[dict[str, Any]] = []
    app = Module("a")

    @app.step(input=Text, output=Text)
    async def handle(ctx: Context, inp: Text) -> Text:
        seen.append(dict(ctx.config))
        return Text(text="y")

    async with FakeCore(path, [app]) as core:
        run = await core.run("p", {"text": ""})
        [plan] = core.spec.pipelines["p"].steps
    assert run.status == "succeeded"
    assert seen == [{"greeting": "hi", "extra": 1}]
    assert (plan.timeout_s, plan.lease_s) == (10.0, 2)

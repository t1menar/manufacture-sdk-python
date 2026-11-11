from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from pydantic import BaseModel

from manufacture import Context, Fatal, Module, NeedsAttention, Transient
from manufacture import _wire as wire
from manufacture.runtime import Runtime
from manufacture.testing import (
    ATTEMPT_ID,
    RUN_ID,
    FakeTransport,
    call_step,
    step_cancel,
    step_request,
)
from manufacture.transport import Inbound
from tests.unit.conftest import RuntimeFactory, Text


def hello_module(suffix: str = "hello") -> Module:
    app = Module("hello-hello", version="0.1.0", concurrency=2)

    @app.step(input=Text, output=Text)
    async def handle(ctx: Context, inp: Text) -> Text:
        return Text(text=inp.text + suffix)

    return app


def failing_module(exc: BaseException) -> Module:
    app = Module("failing")

    @app.step(input=Text, output=Text)
    async def handle(ctx: Context, inp: Text) -> Text:
        raise exc

    return app


class Gate:
    """Lets a test hold a handler until it decides to release it."""

    def __init__(self) -> None:
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.calls = 0


def gated_module(gate: Gate, *, honour_cancel: bool = True) -> Module:
    app = Module("gated")

    @app.step(input=Text, output=Text)
    async def handle(ctx: Context, inp: Text) -> Text:
        gate.calls += 1
        gate.entered.set()
        if honour_cancel:
            release = asyncio.create_task(gate.release.wait())
            cancelled = asyncio.create_task(ctx.cancelled.wait())
            await asyncio.wait({release, cancelled}, return_when=asyncio.FIRST_COMPLETED)
            release.cancel()
            cancelled.cancel()
            if ctx.cancelled.is_set():
                raise Transient("stopped")
        else:
            await gate.release.wait()
        return Text(text=inp.text + "!")

    return app


async def test_success_publishes_result_and_acks(running: RuntimeFactory) -> None:
    async with running(hello_module()) as (runtime, transport):
        assert transport.prefetch[Inbound.WORK] == 2
        delivery = await transport.deliver(step_request({"text": ""}))
        assert await delivery.wait() == "acked"
        [result] = transport.of_type("step.succeeded")
        env = result.envelope
        assert result.routing_key == "step.succeeded"
        assert env.payload["output"] == {"text": "hello"}
        assert env.payload["replica_id"] == "r1"
        assert env.payload["duration_ms"] >= 0
        assert env.producer == "module:hello-hello"
        assert env.run_id == RUN_ID
        assert env.attempt == wire.AttemptRef(id=ATTEMPT_ID, number=1)
        assert env.message_id == wire.result_message_id(ATTEMPT_ID, "step.succeeded")
        assert runtime.in_flight == 0


async def test_call_step_helper() -> None:
    result = await call_step(hello_module(" world"), Text(text="hello"))
    assert result.succeeded
    assert result.output == {"text": "hello world"}


async def test_invalid_input_is_fatal() -> None:
    result = await call_step(hello_module(), {"txt": 1})
    assert result.type == "step.failed"
    assert result.payload["class"] == "fatal"
    assert result.payload["code"] == "input_invalid"
    assert "text" in result.payload["message"]


@pytest.mark.parametrize(
    ("exc", "expected"),
    [
        (
            Transient("busy", retry_after_s=29.2),
            {"class": "transient", "code": "transient", "retry_after_seconds": 30},
        ),
        (Fatal("bad", code="no_such_tile"), {"class": "fatal", "code": "no_such_tile"}),
        (NeedsAttention("look"), {"class": "attention", "code": "needs_attention"}),
        (RuntimeError("boom"), {"class": "transient", "code": "unhandled"}),
    ],
)
async def test_errors_are_classified(exc: Exception, expected: dict[str, Any]) -> None:
    result = await call_step(failing_module(exc), {"text": ""})
    assert result.type == "step.failed"
    assert expected.items() <= result.payload.items()
    if expected["code"] == "unhandled":
        assert result.payload["message"] == "RuntimeError: boom"


async def test_invalid_output_is_fatal() -> None:
    app = Module("bad-output")

    @app.step(input=Text, output=Text)
    async def handle(ctx: Context, inp: Text) -> Text:
        return {"nope": 1}  # type: ignore[return-value]

    result = await call_step(app, {"text": ""})
    assert (result.payload["class"], result.payload["code"]) == ("fatal", "output_invalid")


async def test_dict_output_is_validated_and_accepted() -> None:
    app = Module("dict-output")

    @app.step(input=Text, output=Text)
    async def handle(ctx: Context, inp: Text) -> Text:
        return {"text": "ok"}  # type: ignore[return-value]

    assert (await call_step(app, {"text": ""})).output == {"text": "ok"}


async def test_output_over_envelope_limit_is_fatal() -> None:
    app = Module("big")

    @app.step(input=Text, output=Text)
    async def handle(ctx: Context, inp: Text) -> Text:
        return Text(text="x" * 300_000)

    result = await call_step(app, {"text": ""})
    assert (result.payload["class"], result.payload["code"]) == ("fatal", "output_too_large")


async def test_context_fields_and_config() -> None:
    class Cfg(BaseModel):
        suffix: str

    seen: dict[str, Any] = {}
    app = Module("ctx")

    @app.step(input=Text, output=Text)
    async def handle(ctx: Context, inp: Text) -> Text:
        seen.update(
            run_id=ctx.run_id,
            pipeline=ctx.pipeline,
            revision=ctx.revision,
            stage=ctx.stage,
            step=ctx.step,
            attempt=ctx.attempt,
            attempt_id=ctx.attempt_id,
            blob_prefix=ctx.blob_prefix,
            item_index=ctx.item_index,
            remaining=ctx.remaining_s,
        )
        with pytest.raises(TypeError):
            ctx.config["x"] = 1  # type: ignore[index]
        return Text(text=inp.text + ctx.config_as(Cfg).suffix)

    result = await call_step(app, {"text": "a"}, config={"suffix": "b"})
    assert result.output == {"text": "ab"}
    assert seen["pipeline"] == "pipeline" and seen["attempt"] == 1 and seen["remaining"] > 0
    assert seen["attempt_id"] == ATTEMPT_ID and seen["revision"] == 1 and seen["item_index"] is None
    assert seen["blob_prefix"].startswith("s3://mf-blobs/runs/")

    bad = await call_step(app, {"text": "a"}, config={})
    assert (bad.payload["class"], bad.payload["code"]) == ("fatal", "config_invalid")


async def test_duplicate_delivery_while_running_runs_handler_once(running: RuntimeFactory) -> None:
    gate = Gate()
    async with running(gated_module(gate)) as (_, transport):
        first = await transport.deliver(step_request({"text": "a"}))
        await gate.entered.wait()
        second = await transport.deliver(step_request({"text": "a"}), redelivered=True)
        assert await second.wait() == "acked"  # acked at once, without a second start
        assert first.state == "pending"
        gate.release.set()
        assert await first.wait() == "acked"
        assert gate.calls == 1
        assert len(transport.of_type("step.succeeded")) == 1


async def test_duplicate_after_finish_republishes_same_result(running: RuntimeFactory) -> None:
    async with running(hello_module()) as (_, transport):
        await (await transport.deliver(step_request({"text": ""}))).wait()
        again = await transport.deliver(step_request({"text": ""}), redelivered=True)
        assert await again.wait() == "acked"
        first, second = transport.of_type("step.succeeded")
        assert first.message_id == second.message_id
        assert first.body == second.body


async def test_different_attempts_run_independently(running: RuntimeFactory) -> None:
    gate = Gate()
    async with running(gated_module(gate)) as (_, transport):
        other = "33333333-3333-4333-8333-333333333333"
        first = await transport.deliver(step_request({"text": "a"}))
        second = await transport.deliver(step_request({"text": "b"}, attempt_id=other, attempt=2))
        gate.release.set()
        assert await first.wait() == await second.wait() == "acked"
        assert gate.calls == 2
        ids = {
            p.envelope.attempt.id for p in transport.of_type("step.succeeded") if p.envelope.attempt
        }
        assert ids == {ATTEMPT_ID, other}


async def test_expired_request_is_dropped(running: RuntimeFactory) -> None:
    async with running(hello_module()) as (_, transport):
        past = datetime.now(UTC) - timedelta(seconds=1)
        delivery = await transport.deliver(step_request({"text": ""}, deadline=past))
        assert await delivery.wait() == "acked"
        assert transport.of_type("step.succeeded") == []


async def test_deadline_stops_handler_without_result(running: RuntimeFactory) -> None:
    gate = Gate()
    async with running(gated_module(gate, honour_cancel=False)) as (_, transport):
        delivery = await transport.deliver(step_request({"text": ""}, timeout_s=0.2))
        assert await delivery.wait() == "acked"
        assert transport.of_type("step.succeeded") == transport.of_type("step.failed") == []


async def test_invalid_envelope_is_dead_lettered(running: RuntimeFactory) -> None:
    async with running(hello_module()) as (_, transport):
        delivery = await transport.deliver(b'{"garbage": true}')
        assert await delivery.wait() == "rejected"
        wrong_queue = await transport.deliver(step_cancel())
        assert await wrong_queue.wait() == "rejected"  # step.cancel belongs on the control queue


async def test_request_violating_the_schema_is_dead_lettered(running: RuntimeFactory) -> None:
    async with running(hello_module()) as (_, transport):
        body = step_request({"text": ""}).replace(b'"lease_seconds":30', b'"lease_seconds":0')
        delivery = await transport.deliver(body)
        assert await delivery.wait() == "rejected"  # violates the contract schema -> DLQ
        assert transport.of_type("step.failed") == []


async def test_heartbeats_and_progress(running: RuntimeFactory) -> None:
    app = Module("slow")

    @app.step(input=Text, output=Text)
    async def handle(ctx: Context, inp: Text) -> Text:
        await ctx.progress(0.5, "half")
        with pytest.raises(ValueError, match=r"0\.\.1"):
            await ctx.progress(2)
        await asyncio.sleep(0.8)
        return inp

    async with running(app) as (_, transport):
        await transport.deliver(step_request({"text": ""}, lease_seconds=1))
        await transport.wait_for("step.succeeded")
        beats = transport.of_type("step.heartbeat")
        assert len(beats) >= 3  # progress + every lease/3 (~333 ms)
        assert beats[0].payload == {"replica_id": "r1"}  # at once: the core's timeout starts here
        assert {"progress": 0.5, "message": "half", "replica_id": "r1"} in [
            b.payload for b in beats
        ]
        assert all(b.routing_key == "step.heartbeat" for b in beats)
        assert beats[-1].envelope.attempt is not None


async def test_cancel_sets_flag_and_reports_nothing(running: RuntimeFactory) -> None:
    gate = Gate()
    async with running(gated_module(gate)) as (_, transport):
        delivery = await transport.deliver(step_request({"text": ""}))
        await gate.entered.wait()
        cancel = await transport.deliver(step_cancel(), stream=Inbound.CONTROL)
        assert await cancel.wait() == "acked"
        assert await delivery.wait() == "acked"
        assert transport.of_type("step.failed") == transport.of_type("step.succeeded") == []
        # A late redelivery of the cancelled attempt does not run it again.
        late = await transport.deliver(step_request({"text": ""}), redelivered=True)
        assert await late.wait() == "acked"
        assert gate.calls == 1


async def test_cancel_force_cancels_uncooperative_handler(running: RuntimeFactory) -> None:
    gate = Gate()
    async with running(gated_module(gate, honour_cancel=False), cancel_grace_s=0.05) as (
        _,
        transport,
    ):
        delivery = await transport.deliver(step_request({"text": ""}))
        await gate.entered.wait()
        await transport.deliver(step_cancel(), stream=Inbound.CONTROL)
        assert await delivery.wait(timeout=2) == "acked"
        assert transport.of_type("step.failed") == []


async def test_cancel_for_unknown_attempt_is_acked(running: RuntimeFactory) -> None:
    async with running(hello_module()) as (_, transport):
        assert (
            await (
                await transport.deliver(
                    step_cancel("44444444-4444-4444-8444-444444444444"), stream=Inbound.CONTROL
                )
            ).wait()
            == "acked"
        )
        bad = await transport.deliver(b"{}", stream=Inbound.CONTROL)
        assert await bad.wait() == "rejected"


async def test_publish_failures_are_retried(running: RuntimeFactory) -> None:
    async with running(hello_module(), publish_timeout_s=5) as (_, transport):
        transport.fail_next_publishes = 2
        delivery = await transport.deliver(step_request({"text": ""}))
        assert await delivery.wait() == "acked"
        assert len(transport.of_type("step.succeeded")) == 1


async def test_unpublishable_result_requeues_and_redelivery_republishes(
    running: RuntimeFactory,
) -> None:
    calls = 0
    app = Module("count")

    @app.step(input=Text, output=Text)
    async def handle(ctx: Context, inp: Text) -> Text:
        nonlocal calls
        calls += 1
        return inp

    async with running(app, publish_timeout_s=0.1) as (_, transport):
        transport.fail_next_publishes = 1000
        delivery = await transport.deliver(step_request({"text": ""}))
        assert await delivery.wait() == "requeued"
        transport.fail_next_publishes = 0
        again = await transport.deliver(step_request({"text": ""}), redelivered=True)
        assert await again.wait() == "acked"
        assert calls == 1
        assert len(transport.of_type("step.succeeded")) == 1


async def test_graceful_shutdown_waits_for_running_steps() -> None:
    gate = Gate()
    transport = FakeTransport()
    from tests.unit.conftest import fast_settings

    runtime: Runtime[Any] = Runtime(
        gated_module(gate), settings=fast_settings(shutdown_grace_s=2), transport=transport
    )
    await runtime.start()
    delivery = await transport.deliver(step_request({"text": "a"}))
    await gate.entered.wait()
    stopping = asyncio.create_task(runtime.stop())
    await asyncio.sleep(0.05)
    assert not runtime.ready
    assert Inbound.WORK in transport.stopped
    gate.release.set()
    await stopping
    assert await delivery.wait() == "acked"
    assert transport.of_type("step.succeeded")[0].payload["output"] == {"text": "a!"}
    [offline] = transport.of_type("module.offline")
    assert offline.payload == {"module": "gated", "replica_id": "r1"}
    assert transport.closed


async def test_shutdown_grace_exceeded_reports_module_shutdown() -> None:
    gate = Gate()
    transport = FakeTransport()
    from tests.unit.conftest import fast_settings

    runtime: Runtime[Any] = Runtime(
        gated_module(gate, honour_cancel=False),
        settings=fast_settings(shutdown_grace_s=0.05),
        transport=transport,
    )
    await runtime.start()
    delivery = await transport.deliver(step_request({"text": "a"}))
    await gate.entered.wait()
    await runtime.stop()
    assert await delivery.wait() == "acked"
    [failed] = transport.of_type("step.failed")
    assert (failed.payload["class"], failed.payload["code"]) == ("transient", "module_shutdown")


async def test_deliveries_during_shutdown_are_requeued(running: RuntimeFactory) -> None:
    async with running(hello_module()) as (runtime, transport):
        runtime._accepting = False  # what stop() does before the consumer is cancelled
        delivery = await transport.deliver(step_request({"text": ""}))
        assert await delivery.wait() == "requeued"


async def test_module_online_and_heartbeat(running: RuntimeFactory) -> None:
    async with running(hello_module(), module_heartbeat_s=0.05) as (_, transport):
        [online] = await transport.wait_for("module.online")
        payload = online.payload
        assert payload["module"] == "hello-hello"
        assert payload["replica_id"] == "r1"
        assert payload["concurrency"] == 2
        assert payload["kinds"] == ["step"]
        assert payload["sdk"]["name"] == "manufacture-sdk-python"
        assert payload["contract_version"] == "0.3.0"
        assert payload["input_schema"]["required"] == ["text"]
        beats = await transport.wait_for("module.heartbeat", count=2)
        assert beats[0].payload == {"module": "hello-hello", "replica_id": "r1", "in_flight": 0}


async def test_module_name_must_match_env() -> None:
    from tests.unit.conftest import fast_settings

    with pytest.raises(ValueError, match="MF_MODULE_NAME"):
        Runtime(
            hello_module(), settings=fast_settings(module_name="other"), transport=FakeTransport()
        )


async def test_step_requested_for_source_only_module_fails_fatal(running: RuntimeFactory) -> None:
    from collections.abc import AsyncIterator

    from manufacture import RunRequest, SourceContext

    app = Module("only-source")

    @app.source(pipeline="p")
    async def src(ctx: SourceContext) -> AsyncIterator[RunRequest]:
        yield RunRequest()

    async with running(app) as (_, transport):
        delivery = await transport.deliver(step_request({"text": ""}))
        assert await delivery.wait() == "acked"
        [failed] = transport.of_type("step.failed")
        assert failed.payload["code"] == "no_step_handler"


async def test_ready_follows_transport(running: RuntimeFactory) -> None:
    async with running(hello_module()) as (runtime, transport):
        assert runtime.ready
        transport.connected = False
        assert not runtime.ready


async def test_module_without_handlers_does_not_start() -> None:
    with pytest.raises(RuntimeError, match="no @step, @source"):
        await Runtime(Module("empty"), transport=FakeTransport()).start()

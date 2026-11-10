"""End-to-end runtime tests on a real RabbitMQ broker (no core: the tests play the core)."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any

import aio_pika
import pytest
from pydantic import BaseModel

from manufacture import Context, Module, RunRequest, RuntimeSettings, SourceContext
from manufacture import _wire as wire
from manufacture.runtime import Runtime
from manufacture.testing import ATTEMPT_ID, source_trigger, step_cancel, step_request
from tests.integration.conftest import AMQP_URL, FakeCore, settings_kwargs

pytestmark = pytest.mark.integration

TRIGGER = "66666666-6666-4666-8666-666666666666"


class Text(BaseModel):
    text: str


def hello(core: FakeCore, name: str = "hello-hello", gate: asyncio.Event | None = None) -> Module:
    core.modules.append(name)
    app = Module(name, concurrency=2)

    @app.step(input=Text, output=Text)
    async def handle(ctx: Context, inp: Text) -> Text:
        if gate is not None:
            stop = asyncio.create_task(ctx.cancelled.wait())
            go = asyncio.create_task(gate.wait())
            await asyncio.wait({stop, go}, return_when=asyncio.FIRST_COMPLETED)
            stop.cancel()
            go.cancel()
        return Text(text=inp.text + "hello")

    return app


async def start(app: Module, topology: wire.Topology, **overrides: Any) -> Runtime[Any]:
    runtime: Runtime[Any] = Runtime(
        app, settings=RuntimeSettings(**settings_kwargs(topology, **overrides))
    )
    await runtime.start()
    return runtime


async def test_step_round_trip_and_lifecycle(core: FakeCore, topology: wire.Topology) -> None:
    runtime = await start(hello(core), topology, replica_id="r1")
    try:
        assert runtime.ready
        [online] = await core.wait_for("module.online")
        assert wire.decode(online.body).payload["module"] == "hello-hello"
        await core.wait_for("module.heartbeat", count=2)

        await core.work("hello-hello", step_request({"text": ""}))
        [result] = await core.wait_for("step.succeeded")
        env = wire.decode(result.body)
        assert env.payload["output"] == {"text": "hello"}
        assert result.message_id == str(wire.result_message_id(ATTEMPT_ID, "step.succeeded"))
        assert result.routing_key == "step.succeeded"
        assert result.content_type == "application/json"
        assert result.user_id == aio_pika.connection.make_url(AMQP_URL).user
        assert result.delivery_mode == aio_pika.DeliveryMode.PERSISTENT
        await asyncio.sleep(0.2)
        assert await core.queue_depth(topology.work_queue("hello-hello")) == 0
    finally:
        await runtime.stop()
    [offline] = await core.wait_for("module.offline")
    assert wire.decode(offline.body).payload == {"module": "hello-hello", "replica_id": "r1"}


async def test_cancel_reaches_the_replica(core: FakeCore, topology: wire.Topology) -> None:
    gate = asyncio.Event()
    runtime = await start(hello(core, gate=gate), topology)
    try:
        await core.work("hello-hello", step_request({"text": ""}, lease_seconds=1))
        await core.wait_for("step.heartbeat")
        await core.cancel("hello-hello", step_cancel())
        async with asyncio.timeout(5):
            while runtime.in_flight:
                await asyncio.sleep(0.05)
        await asyncio.sleep(0.2)
        assert core.of_type("step.succeeded") == core.of_type("step.failed") == []
        assert await core.queue_depth(topology.work_queue("hello-hello")) == 0
    finally:
        await runtime.stop()


async def test_unacked_step_is_redelivered_to_another_replica(
    core: FakeCore, topology: wire.Topology
) -> None:
    gate = asyncio.Event()
    app = hello(core, gate=gate)
    first = await start(app, topology, replica_id="a")
    await core.work("hello-hello", step_request({"text": ""}, lease_seconds=1))
    await core.wait_for("step.heartbeat")
    await first.abort()  # crash: connection dropped, delivery never acked

    second = await start(app, topology, replica_id="b")
    try:
        gate.set()
        [result] = await core.wait_for("step.succeeded")
        assert wire.decode(result.body).payload["replica_id"] == "b"
    finally:
        await second.stop()


async def test_graceful_shutdown_finishes_running_step(
    core: FakeCore, topology: wire.Topology
) -> None:
    gate = asyncio.Event()
    runtime = await start(hello(core, gate=gate), topology)
    await core.work("hello-hello", step_request({"text": ""}, lease_seconds=1))
    await core.wait_for("step.heartbeat")
    stopping = asyncio.create_task(runtime.stop())
    await asyncio.sleep(0.1)
    assert not runtime.ready
    gate.set()
    await stopping
    assert len(await core.wait_for("step.succeeded")) == 1


async def test_invalid_message_is_dead_lettered(core: FakeCore, topology: wire.Topology) -> None:
    runtime = await start(hello(core), topology)
    try:
        await core.commands.publish(
            aio_pika.Message(b"garbage"), routing_key=topology.work_key("hello-hello")
        )
        async with asyncio.timeout(5):
            while await core.queue_depth(core.dead_letters) == 0:
                await asyncio.sleep(0.05)
    finally:
        await runtime.stop()


async def test_source_on_start_and_trigger(core: FakeCore, topology: wire.Topology) -> None:
    core.modules.append("hello-init")
    app = Module("hello-init")

    @app.source(pipeline="hello-world", on_start=True)
    async def begin(ctx: SourceContext) -> AsyncIterator[RunRequest]:
        yield RunRequest(data={"text": ""}, dedup_key=ctx.trigger_id)

    runtime = await start(app, topology)
    try:
        [run] = await core.wait_for("run.requested")
        assert wire.decode(run.body).payload["pipeline"] == "hello-world"
        await core.work("hello-init", source_trigger("hello-world", trigger_id=TRIGGER))
        runs = await core.wait_for("run.requested", count=2)
        payload = wire.decode(runs[1].body).payload
        assert payload["dedup_key"] == payload["trigger_id"] == TRIGGER
    finally:
        await runtime.stop()


async def test_missing_core_exchanges_fail_fast(topology: wire.Topology) -> None:
    app = Module("lonely")

    @app.step(input=Text, output=Text)
    async def handle(ctx: Context, inp: Text) -> Text:
        return inp

    runtime: Runtime[Any] = Runtime(app, settings=RuntimeSettings(**settings_kwargs(topology)))
    with pytest.raises(Exception, match=r"NOT_FOUND|not found"):
        await runtime.start()
    assert not runtime.ready

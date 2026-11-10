from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

from manufacture import Module, RunRequest, SourceContext
from manufacture import _wire as wire
from manufacture.testing import source_trigger
from manufacture.transport import Inbound
from tests.unit.conftest import RuntimeFactory

TRIGGER = "55555555-5555-4555-8555-555555555555"


def init_module(*, on_start: bool = False) -> Module:
    app = Module("hello-init")

    @app.source(pipeline="hello-world", on_start=on_start)
    async def start(ctx: SourceContext) -> AsyncIterator[RunRequest]:
        yield RunRequest(data={"text": ""}, dedup_key=ctx.trigger_id)

    return app


async def test_on_start_requests_one_run(running: RuntimeFactory) -> None:
    async with running(init_module(on_start=True)) as (_, transport):
        [run] = await transport.wait_for("run.requested")
        assert run.routing_key == "run.requested"
        key = run.payload["dedup_key"]
        assert run.payload == {"pipeline": "hello-world", "dedup_key": key, "data": {"text": ""}}
        assert run.message_id == str(wire.run_request_message_id("hello-world", key))
        [online] = transport.of_type("module.online")
        assert online.payload["kinds"] == ["source"]
        assert "input_schema" not in online.payload


async def test_trigger_message_runs_source_and_acks(running: RuntimeFactory) -> None:
    async with running(init_module()) as (_, transport):
        delivery = await transport.deliver(source_trigger("hello-world", trigger_id=TRIGGER))
        assert await delivery.wait() == "acked"
        [run] = transport.of_type("run.requested")
        assert run.payload["dedup_key"] == TRIGGER
        assert run.payload["trigger_id"] == TRIGGER


async def test_default_dedup_key_trigger_data_and_pipeline_override(
    running: RuntimeFactory,
) -> None:
    app = Module("multi")

    @app.source(pipeline="a")
    async def many(ctx: SourceContext) -> AsyncIterator[RunRequest]:
        assert ctx.trigger_kind == "trigger"
        for i in range(ctx.data["n"]):
            yield RunRequest(data={"i": i})
        yield RunRequest(data={}, pipeline="b", dedup_key="fixed")

    async with running(app) as (_, transport):
        delivery = await transport.deliver(source_trigger("a", trigger_id=TRIGGER, data={"n": 2}))
        await delivery.wait()
        keys = [
            (r.payload["pipeline"], r.payload["dedup_key"])
            for r in transport.of_type("run.requested")
        ]
        assert keys == [("a", f"{TRIGGER}:0"), ("a", f"{TRIGGER}:1"), ("b", "fixed")]


async def test_trigger_selects_source_by_pipeline(running: RuntimeFactory) -> None:
    app = Module("two")

    @app.source(pipeline="a")
    async def a(ctx: SourceContext) -> AsyncIterator[RunRequest]:
        yield RunRequest(data={"from": "a"})

    @app.source(pipeline="b")
    async def b(ctx: SourceContext) -> AsyncIterator[RunRequest]:
        yield RunRequest(data={"from": "b"})

    async with running(app) as (_, transport):
        await (await transport.deliver(source_trigger("b"))).wait()
        assert transport.of_type("run.requested")[0].payload["data"] == {"from": "b"}
        unknown = await transport.deliver(source_trigger("zzz"))
        assert await unknown.wait() == "rejected"


async def test_failing_source_dead_letters_trigger(running: RuntimeFactory) -> None:
    app = Module("broken")

    @app.source(pipeline="p")
    async def broken(ctx: SourceContext) -> AsyncIterator[RunRequest]:
        yield RunRequest(data={"ok": 1})
        raise RuntimeError("upstream down")

    async with running(app) as (_, transport):
        delivery = await transport.deliver(source_trigger("p"))
        assert await delivery.wait() == "rejected"
        assert len(transport.of_type("run.requested")) == 1  # already requested runs stay


async def test_source_yielding_wrong_type_fails(running: RuntimeFactory) -> None:
    app = Module("wrong")

    @app.source(pipeline="p")
    async def wrong(ctx: SourceContext) -> AsyncIterator[RunRequest]:
        yield {"data": {}}  # type: ignore[misc]

    async with running(app) as (_, transport):
        delivery = await transport.deliver(source_trigger("p"))
        assert await delivery.wait() == "rejected"


async def test_every_schedules_runs(running: RuntimeFactory) -> None:
    app = Module("ticker")

    @app.source(pipeline="p", every=0.05)
    async def tick(ctx: SourceContext) -> AsyncIterator[RunRequest]:
        assert ctx.trigger_kind == "every"
        yield RunRequest(data={})

    async with running(app) as (_, transport):
        runs = await transport.wait_for("run.requested", count=2, timeout=2)
        assert len({r.payload["dedup_key"] for r in runs}) == len(runs)


async def test_shutdown_requeues_unfinished_trigger(running: RuntimeFactory) -> None:
    hold = asyncio.Event()
    app = Module("slow-source")

    @app.source(pipeline="p")
    async def slow(ctx: SourceContext) -> AsyncIterator[RunRequest]:
        await hold.wait()
        yield RunRequest(data={})

    async with running(app, shutdown_grace_s=0.05) as (runtime, transport):
        delivery = await transport.deliver(source_trigger("p"))
        await asyncio.sleep(0.01)
        await runtime.stop()
        assert await delivery.wait() == "requeued"
        assert Inbound.WORK in transport.stopped

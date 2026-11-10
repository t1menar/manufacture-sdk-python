"""``manufacture.fakecore`` over a real RabbitMQ (``broker="amqp"``)."""

from __future__ import annotations

import asyncio
from typing import Any

import aio_pika
import pytest
from pydantic import SecretStr

from manufacture import Context, Module, RuntimeSettings, Transient
from manufacture.fakecore import FakeCore
from manufacture.runtime import Runtime
from tests.integration.conftest import AMQP_URL
from tests.unit.test_fakecore import RETRY, Text, append, hello_spec, module_doc, pipeline_doc

pytestmark = pytest.mark.integration


async def test_linear_pipeline_over_rabbitmq() -> None:
    async with FakeCore(
        hello_spec(),
        [append("a", "hello"), append("b", " world")],
        broker="amqp",
        amqp_url=AMQP_URL,
    ) as core:
        await core.wait_for_replicas(3)
        runs = [await core.start_run("p", {"text": str(i)}) for i in range(4)]
        done = await core.wait_for_runs("p", count=4, timeout=30)
    assert [r.status for r in done] == ["succeeded"] * 4
    assert {r.data["text"] for r in runs} == {f"{i}hello world" for i in range(4)}
    second = {r.step("second").attempts[0].replica_id for r in done}
    assert second <= {"b-0", "b-1"}


async def test_retry_and_external_module_over_rabbitmq() -> None:
    calls: list[int] = []
    flaky = Module("a")

    @flaky.step(input=Text, output=Text)
    async def handle(ctx: Context, inp: Text) -> Text:
        calls.append(ctx.attempt)
        if ctx.attempt == 1:
            raise Transient("first attempt fails")
        return Text(text=inp.text + "a")

    spec = [
        module_doc("a"),
        module_doc("ext", runtime="external"),
        pipeline_doc(
            "p",
            [
                {"name": "s", "step": {"module": "a", "retry": RETRY}},
                {"name": "t", "step": {"module": "ext"}},
            ],
        ),
    ]
    async with FakeCore(spec, [flaky], broker="amqp", amqp_url=AMQP_URL, jitter=False) as core:
        # "ext" is not in-process: a separate runtime (a container in real life) joins
        # the same broker and prefix by itself.
        external: Runtime[Any] = Runtime(
            append("ext", "x"),
            settings=RuntimeSettings(
                amqp_url=SecretStr(AMQP_URL),
                amqp_prefix=core.prefix,
                replica_id="ext-0",
                health_addr="off",
                min_heartbeat_s=0.05,
            ),
        )
        await external.start()
        try:
            run = await core.run("p", {"text": ""}, timeout=30)
        finally:
            await external.stop()
    assert run.status == "succeeded", core.describe()
    assert run.data == {"text": "ax"}
    assert calls == [1, 2]
    assert run.step("t").attempts[0].replica_id == "ext-0"


async def test_crashed_replica_work_is_redelivered_over_rabbitmq() -> None:
    started = asyncio.Event()
    calls: list[int] = []
    app = Module("a")

    @app.step(input=Text, output=Text)
    async def handle(ctx: Context, inp: Text) -> Text:
        calls.append(ctx.attempt)
        if len(calls) == 1:  # the first delivery hangs in a-0, which then crashes
            started.set()
            await asyncio.sleep(60)
        return Text(text=f"call {len(calls)}")

    spec = [
        module_doc("a"),
        pipeline_doc(
            "p", [{"name": "s", "step": {"module": "a", "timeout": "30s", "lease": "10s"}}]
        ),
    ]
    async with FakeCore(spec, [app], broker="amqp", amqp_url=AMQP_URL) as core:
        run = await core.start_run("p", {"text": ""})
        await asyncio.wait_for(started.wait(), 10)
        await core.start_replica(app, "a-1")
        await core.kill_replica("a-0")
        run = await core.wait(run.id, timeout=30)
    assert run.status == "succeeded"
    [attempt] = run.step("s").attempts  # redelivery keeps the attempt
    assert attempt.replica_id == "a-1"
    assert run.data == {"text": "call 2"}
    assert calls == [1, 1]


async def test_queues_are_removed_after_the_run() -> None:
    async with FakeCore(
        hello_spec(), [append("a", ""), append("b", "")], broker="amqp", amqp_url=AMQP_URL
    ) as core:
        prefix = core.prefix
    conn = await aio_pika.connect(AMQP_URL)
    try:
        channel = await conn.channel()
        with pytest.raises(aio_pika.exceptions.ChannelClosed):
            await channel.declare_queue(f"{prefix}mf.module.a", passive=True)
    finally:
        await conn.close()

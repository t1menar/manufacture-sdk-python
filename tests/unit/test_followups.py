"""0.2.1 follow-ups: re-announce after reconnect, temporary schedules, error codes, source helpers."""

from __future__ import annotations

import json
import logging
from collections.abc import AsyncIterator

import pytest

from manufacture import ErrorCode, Module, RunRequest, SourceContext
from manufacture import _wire as wire
from manufacture.runtime import classify_exception
from manufacture.testing import FakeTransport, call_on_start, call_source
from tests.unit.conftest import RuntimeFactory
from tests.unit.test_runtime_steps import hello_module

TRIGGER = "77777777-7777-4777-8777-777777777777"


async def test_module_online_is_repeated_after_reconnect(running: RuntimeFactory) -> None:
    async with running(hello_module()) as (_, transport):
        [first] = await transport.wait_for("module.online")
        await transport.simulate_reconnect()
        _, second = await transport.wait_for("module.online", count=2)
        assert second.payload == first.payload
        assert second.message_id != first.message_id


async def test_reconnect_after_stop_is_ignored(running: RuntimeFactory) -> None:
    async with running(hello_module()) as (runtime, transport):
        await runtime.stop()
        await transport.simulate_reconnect()
        assert len(transport.of_type("module.online")) == 1


def test_error_codes_are_plain_strings_on_the_wire() -> None:
    assert ErrorCode.MODULE_SHUTDOWN.value == "module_shutdown"
    failed = classify_exception(RuntimeError("boom"))
    assert failed.code == ErrorCode.UNHANDLED
    assert json.loads(failed.model_dump_json(by_alias=True))["code"] == "unhandled"


def scheduled_module(*, on_start: bool = True, every: float | None = None) -> Module:
    app = Module("sched")

    @app.source(pipeline="p", on_start=on_start, every=every)
    async def src(ctx: SourceContext) -> AsyncIterator[RunRequest]:
        yield RunRequest(data={"kind": ctx.trigger_kind, "data": ctx.data})
        yield RunRequest(data={"second": True})

    return app


async def test_temporary_schedules_log_a_warning(
    running: RuntimeFactory, caplog: pytest.LogCaptureFixture
) -> None:
    with caplog.at_level(logging.WARNING, logger="manufacture.runtime"):
        async with running(scheduled_module()) as (_, transport):
            await transport.wait_for("run.requested", count=2)
    warnings = [r for r in caplog.records if "temporary" in r.getMessage()]
    assert len(warnings) == 1
    assert getattr(warnings[0], "mf_fields", {})["source"] == "src"


async def test_call_source_runs_one_trigger_without_schedules() -> None:
    result = await call_source(scheduled_module(), {"n": 1}, trigger_id=TRIGGER)
    assert result.acked
    assert result.trigger_id == TRIGGER
    assert [r["data"] for r in result.runs] == [
        {"kind": "trigger", "data": {"n": 1}},
        {"second": True},
    ]
    assert [r["dedup_key"] for r in result.runs] == [f"{TRIGGER}:0", f"{TRIGGER}:1"]
    assert all(r["trigger_id"] == TRIGGER for r in result.runs)


async def test_call_source_reports_a_failing_source() -> None:
    app = Module("broken")

    @app.source(pipeline="p")
    async def src(ctx: SourceContext) -> AsyncIterator[RunRequest]:
        yield RunRequest(data={})
        raise RuntimeError("upstream down")

    result = await call_source(app)
    assert not result.acked
    assert len(result.runs) == 1


async def test_call_source_needs_pipeline_when_ambiguous() -> None:
    app = Module("two")

    @app.source(pipeline="a")
    async def a(ctx: SourceContext) -> AsyncIterator[RunRequest]:
        yield RunRequest(data={"from": "a"})

    @app.source(pipeline="b")
    async def b(ctx: SourceContext) -> AsyncIterator[RunRequest]:
        yield RunRequest(data={"from": "b"})

    with pytest.raises(ValueError, match="pass pipeline"):
        await call_source(app)
    result = await call_source(app, pipeline="b")
    assert [r["data"] for r in result.runs] == [{"from": "b"}]


async def test_call_on_start_returns_all_runs() -> None:
    result = await call_on_start(scheduled_module())
    assert [r["data"] for r in result.runs] == [{"kind": "start", "data": {}}, {"second": True}]
    assert all("trigger_id" not in r for r in result.runs)
    with pytest.raises(ValueError, match="no on_start"):
        await call_on_start(scheduled_module(on_start=False))


async def test_runtime_without_local_schedules() -> None:
    from manufacture import RuntimeSettings
    from manufacture.runtime import Runtime

    transport = FakeTransport()
    runtime = Runtime(
        scheduled_module(every=0.01),
        settings=RuntimeSettings(health_addr="", replica_id="t", module_heartbeat_s=3600),
        transport=transport,
        local_schedules=False,
    )
    await runtime.start()
    try:
        import asyncio

        await asyncio.sleep(0.05)
        assert transport.of_type("run.requested") == []
    finally:
        await runtime.stop()


def test_error_codes_keep_the_contract_length_limit() -> None:
    with pytest.raises(ValueError, match="at most 128 characters"):
        wire.StepFailed(error_class="fatal", code="x" * 129, message="m")


def test_run_request_copies_read_only_views() -> None:
    from types import MappingProxyType

    view = MappingProxyType({"a": MappingProxyType({"b": (1, 2)})})
    req = RunRequest(data={"copy": view})
    assert req.data == {"copy": {"a": {"b": [1, 2]}}}
    assert type(req.data["copy"]) is dict
    assert (
        req.model_dump_json()
        == '{"data":{"copy":{"a":{"b":[1,2]}}},"dedup_key":null,"pipeline":null}'
    )

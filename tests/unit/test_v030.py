"""Contract 0.3.0 in the runtime: waiting, late results, confirm, step_key/visit, usage, run notifications."""

from __future__ import annotations

import json
from typing import Any

import httpx
import pytest

from manufacture import (
    AttemptHandle,
    Cancelled,
    Context,
    DeadlineTooClose,
    Fatal,
    Module,
    RunFinished,
    RunRequest,
    RunStarted,
    Superseded,
)
from manufacture import _wire as wire
from manufacture.testing import (
    ATTEMPT_ID,
    RUN_ID,
    call_run_event,
    call_step,
    run_finished,
    run_started,
    step_request,
)
from tests.unit.conftest import RuntimeFactory, Text


def test_step_key_matches_core_vectors() -> None:
    import pathlib

    vectors = json.loads(
        (
            pathlib.Path(__file__).parent.parent / "contract/testdata/envelope/step_key.json"
        ).read_text()
    )
    for case in vectors["cases"]:
        assert (
            wire.step_key_for(
                case["run_id"], case["stage"], case["step"], case["item_index"], case["visit"]
            )
            == case["step_key"]
        )


def test_waiting_message_ids_match_core_vectors() -> None:
    import pathlib

    vectors = json.loads(
        (
            pathlib.Path(__file__).parent.parent / "contract/testdata/envelope/message_ids.json"
        ).read_text()
    )
    for case in vectors["cases"]:
        if case["kind"] == "waiting":
            assert (
                str(wire.waiting_message_id(case["attempt_id"], case["seq"])) == case["message_id"]
            )


async def test_context_exposes_step_key_visit_budget_and_labels() -> None:
    app = Module("probe")
    seen: dict[str, Any] = {}

    @app.step(input=Text, output=Text)
    async def handle(ctx: Context, inp: Text) -> Text:
        seen.update(
            key=ctx.step_key,
            visit=ctx.visit,
            budget=ctx.budget,
            labels=dict(ctx.labels),
            handle=str(ctx.attempt_handle),
        )
        return inp

    res = await call_step(
        app,
        {"text": "x"},
        visit=2,
        budget={"run_usd": 5, "spent_usd": 1, "remaining_usd": 4},
        labels={"idea_ref": "i1"},
    )
    assert res.succeeded
    assert seen["visit"] == 2 and seen["labels"] == {"idea_ref": "i1"}
    assert seen["key"] == wire.step_key_for(RUN_ID, "stage", "step", None, 2)
    assert seen["budget"].remaining_usd == 4
    assert seen["handle"] == f"{RUN_ID}:{ATTEMPT_ID}:1"


async def test_usage_is_cumulative_on_heartbeat_and_result() -> None:
    app = Module("spender")

    @app.step(input=Text, output=Text)
    async def handle(ctx: Context, inp: Text) -> Text:
        ctx.add_usage(0.25, model="m1", input_tokens=10, output_tokens=5)
        ctx.add_usage(0.25, model="m1", input_tokens=1)
        return inp

    res = await call_step(app, {"text": ""})
    assert res.payload["usage"] == {"cost_usd": 0.5, "tokens": {"m1": {"input": 11, "output": 5}}}


async def test_wait_sends_step_waiting_acks_and_complete_finishes(running: RuntimeFactory) -> None:
    app = Module("review")
    handles: list[str] = []

    @app.step(input=Text, output=Text)
    async def handle(ctx: Context, inp: Text) -> Text:
        handles.append(str(ctx.attempt_handle))
        await ctx.wait(reason="human", message="waiting for the owner")
        raise AssertionError("unreachable")

    async with running(app) as (_, transport):
        delivery = await transport.deliver(step_request({"text": "x"}))
        (waiting,) = await transport.wait_for("step.waiting")
        assert waiting.payload["reason"] == "human" and waiting.payload["seq"] == 1
        assert await delivery.wait() == "acked"  # the prefetch slot is free
        assert transport.of_type("step.succeeded") == []
        # a duplicate delivery of the parked attempt is acked without a new start
        again = await transport.deliver(step_request({"text": "x"}))
        assert await again.wait() == "acked" and len(handles) == 1
        # later, from "another replica": complete by the stored handle
        await app.complete(handles[0], {"text": "approved"}, cost_usd=0.01)
        (done,) = await transport.wait_for("step.succeeded")
        assert done.payload["output"] == {"text": "approved"} and done.payload["usage"] == {
            "cost_usd": 0.01
        }
        assert done.envelope.attempt == wire.AttemptRef(id=ATTEMPT_ID, number=1)


async def test_fail_after_wait(running: RuntimeFactory) -> None:
    app = Module("review")

    @app.step(input=Text, output=Text)
    async def handle(ctx: Context, inp: Text) -> Text:
        await ctx.wait()

    async with running(app) as (_, transport):
        await transport.deliver(step_request({"text": "x"}))
        await transport.wait_for("step.waiting")
        await app.fail(
            AttemptHandle(RUN_ID, ATTEMPT_ID, 1), Fatal("owner timed out", code="expired")
        )
        (failed,) = await transport.wait_for("step.failed")
        assert failed.payload["class"] == "fatal" and failed.payload["code"] == "expired"


async def test_complete_validates_the_output() -> None:
    app = Module("review")

    @app.step(input=Text, output=Text)
    async def handle(ctx: Context, inp: Text) -> Text:
        return inp

    with pytest.raises(RuntimeError, match="not running"):
        await app.complete(f"{RUN_ID}:{ATTEMPT_ID}:1", {"text": "x"})
    with pytest.raises(ValueError, match="attempt handle"):
        AttemptHandle.parse("nonsense")


def _confirm_core(result: str, seen: list[httpx.Request]) -> Any:
    def handler(req: httpx.Request) -> httpx.Response:
        seen.append(req)
        return httpx.Response(
            200, json={"result": result, "attempt_id": ATTEMPT_ID, "detail": "new_attempt"}
        )

    return handler


async def test_confirm_ok_posts_to_the_core_with_the_module_token() -> None:
    app = Module("publisher")
    done: list[str] = []

    @app.step(input=Text, output=Text)
    async def handle(ctx: Context, inp: Text) -> Text:
        await ctx.confirm(action="media_publish")
        done.append("published")
        return inp

    seen: list[httpx.Request] = []
    res = await call_step(app, {"text": ""}, core=_confirm_core("ok", seen))
    assert res.succeeded and done == ["published"]
    req = seen[0]
    assert req.url.path == f"/api/v1/attempts/{ATTEMPT_ID}/confirm"
    assert req.headers["authorization"] == "Bearer mft_test"
    body = json.loads(req.content)
    assert body["action"] == "media_publish" and body["extend_seconds"] == 120
    assert body["step_key"] == wire.step_key_for(RUN_ID, "stage", "step", None, 1)


@pytest.mark.parametrize("result", ["superseded", "cancelled"])
async def test_refused_confirm_stops_the_action_and_reports_nothing(
    result: str, running: RuntimeFactory
) -> None:
    app = Module("publisher")
    acted: list[str] = []

    @app.step(input=Text, output=Text)
    async def handle(ctx: Context, inp: Text) -> Text:
        try:
            await ctx.confirm(action="media_publish")
        except (Superseded, Cancelled) as exc:
            acted.append(type(exc).__name__)
            raise
        acted.append("published")
        return inp

    token = "mft_x"  # noqa: S105
    async with running(app, core_url="http://core.test", module_token=token) as (
        runtime,
        transport,
    ):
        runtime._http = httpx.AsyncClient(transport=httpx.MockTransport(_confirm_core(result, [])))
        delivery = await transport.deliver(step_request({"text": ""}))
        assert await delivery.wait() == "acked"
    assert acted == ["Superseded" if result == "superseded" else "Cancelled"]
    assert transport.of_type("step.succeeded") == [] and transport.of_type("step.failed") == []


async def test_confirm_deadline_is_transient_and_missing_config_is_fatal() -> None:
    app = Module("publisher")

    @app.step(input=Text, output=Text)
    async def handle(ctx: Context, inp: Text) -> Text:
        await ctx.confirm(action="media_publish")
        return inp

    res = await call_step(app, {"text": ""}, core=_confirm_core("deadline", []))
    assert res.payload["class"] == "transient" and res.payload["code"] == "deadline_too_close"
    res = await call_step(app, {"text": ""})
    assert res.payload["class"] == "fatal" and res.payload["code"] == "confirm_unavailable"
    assert DeadlineTooClose.__mro__[1].__name__ == "ConfirmError"


async def test_attention_carries_recommendation_and_details() -> None:
    from manufacture import NeedsAttention

    app = Module("p")

    @app.step(input=Text, output=Text)
    async def handle(ctx: Context, inp: Text) -> Text:
        raise NeedsAttention(
            "unknown outcome",
            code="publish_unknown",
            recommendation="check Instagram",
            details={"id": "1"},
        )

    res = await call_step(app, {"text": ""})
    assert res.payload["recommendation"] == "check Instagram" and res.payload["details"] == {
        "id": "1"
    }


async def test_run_notifications_reach_listeners_once() -> None:
    app = Module("idea")
    started: list[RunStarted] = []
    finished: list[tuple[str, RunFinished]] = []

    @app.on_run_started
    async def on_started(run_id: str, ev: RunStarted) -> None:
        started.append(ev)

    @app.on_run_finished
    async def on_finished(run_id: str, ev: RunFinished) -> None:
        finished.append((run_id, ev))

    assert await call_run_event(app, run_started(labels={"idea_ref": "i1"}))
    assert await call_run_event(
        app, run_finished(status="failed", labels={"idea_ref": "i1"}, cost_usd=1.5)
    )
    assert started[0].labels == {"idea_ref": "i1"}
    assert (
        finished[0][0] == RUN_ID
        and finished[0][1].status == "failed"
        and finished[0][1].usage["cost_usd"] == 1.5
    )
    assert app.manifest()["run_events"] == ["run.finished", "run.started"]


async def test_failing_listener_asks_for_redelivery() -> None:
    app = Module("idea")

    @app.on_run_finished
    async def boom(run_id: str, ev: RunFinished) -> None:
        raise RuntimeError("db down")

    assert await call_run_event(app, run_finished()) is False


def test_run_request_carries_priority_and_labels() -> None:
    req = RunRequest(data={}, priority=10, labels={"idea_ref": "i"})
    assert req.priority == 10 and req.labels == {"idea_ref": "i"}

"""The fake core engine: runs linear pipelines from their spec over a :class:`Bus`."""

from __future__ import annotations

import asyncio
import contextlib
import heapq
import itertools
import os
import random
import uuid
from collections import deque
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, Literal, Self

from pydantic import ValidationError

from manufacture import _wire as wire
from manufacture._log import get_logger
from manufacture.fakecore._bus import Bus, DeadLetter, MemoryBus
from manufacture.fakecore._model import (
    Attempt,
    ErrorInfo,
    ModuleReplica,
    Rejection,
    Run,
    StaleResult,
    Step,
    Trigger,
    step_key,
)
from manufacture.fakecore._spec import PipelinePlan, SpecSet, StepPlan, load_documents, plan
from manufacture.runtime import Runtime
from manufacture.settings import RuntimeSettings

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable
    from pathlib import Path
    from types import TracebackType

    from manufacture.app import Module

__all__ = ["CoreEvent", "FakeCore", "ProtocolViolationError", "merge_patch"]

PRODUCER = "core"
_STEP_EVENTS: dict[str, type[wire.StepHeartbeat | wire.StepSucceeded | wire.StepFailed]] = {
    "step.heartbeat": wire.StepHeartbeat,
    "step.succeeded": wire.StepSucceeded,
    "step.failed": wire.StepFailed,
}
_log = get_logger("manufacture.fakecore")


class ProtocolViolationError(AssertionError):
    """A module published a message that does not match the envelope schema."""


def merge_patch(target: Any, patch: Any) -> Any:  # noqa: ANN401 (any JSON value)
    """RFC 7396 JSON Merge Patch: how the core applies ``step.succeeded.output`` to data."""
    if not isinstance(patch, dict):
        return patch
    result = dict(target) if isinstance(target, dict) else {}
    for key, value in patch.items():
        if value is None:
            result.pop(key, None)
        else:
            result[key] = merge_patch(result.get(key), value)
    return result


@dataclass(frozen=True, slots=True)
class CoreEvent:
    """One state change, in order (a test-side stand-in for the gateway's event stream)."""

    type: Literal["run", "step", "attempt"]
    run_id: str
    status: str
    step_id: str | None = None
    attempt: int | None = None


@dataclass(eq=False)
class _Live:
    """Engine-side state of the current attempt of a step."""

    run: Run
    step: Step
    attempt: Attempt
    plan: StepPlan
    changed: asyncio.Event = field(default_factory=asyncio.Event)
    last_seen: float | None = None
    result: wire.StepSucceeded | wire.StepFailed | None = None


@dataclass(eq=False)
class _Admission:
    """``concurrency.max_runs``: FIFO within a priority, higher priority first (0.3.0 R10;
    with the default priority 0 this is plain FIFO, as in 0.2.1)."""

    limit: int | None
    active: int = 0
    waiting: list[tuple[int, int, asyncio.Future[None]]] = field(default_factory=list)
    seq: itertools.count[int] = field(default_factory=itertools.count)

    async def acquire(self, priority: int) -> None:
        if self.limit is None or (self.active < self.limit and not self.waiting):
            self.active += 1
            return
        fut: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        heapq.heappush(self.waiting, (-priority, next(self.seq), fut))
        try:
            await fut
        except asyncio.CancelledError:
            if fut.done() and not fut.cancelled():
                self.release()
            raise

    def release(self) -> None:
        self.active -= 1
        while self.waiting:
            _, _, fut = heapq.heappop(self.waiting)
            if not fut.done():
                self.active += 1
                fut.set_result(None)
                return


def _make_bus(
    broker: Literal["memory", "amqp"] | Bus, amqp_url: str | None, prefix: str | None
) -> tuple[Bus, bool, str | None]:
    """The bus, whether the fake core owns (and cleans up) it, and the AMQP URL."""
    if broker == "memory":
        return MemoryBus(prefix=prefix or ""), True, amqp_url
    if broker == "amqp":
        from manufacture.fakecore._amqp import AmqpBus  # noqa: PLC0415 (aio-pika only here)

        url = amqp_url or os.environ.get("MF_AMQP_URL", "amqp://guest:guest@localhost:5672/")
        prefix = f"fc-{uuid.uuid4().hex[:8]}." if prefix is None else prefix
        return AmqpBus(url, prefix=prefix), True, url
    return broker, False, amqp_url


def _now() -> datetime:
    return datetime.now(UTC)


class FakeCore:
    """Run pipelines from their spec without the platform.

    ``spec`` is a path to a YAML/JSON file, a document, or a list of either; it holds the
    ``Module`` and ``Pipeline`` resources, validated against the vendored contract.
    ``modules`` are the in-process modules (:class:`~manufacture.Module` objects); each
    gets the number of replicas from its spec (or ``replicas``). Modules of the spec not
    passed here are *external*: with ``broker="amqp"`` they may be real processes or
    containers connected to the same broker; with ``broker="memory"`` they are an error.

    ``broker`` is ``"memory"`` (default, an in-process broker with RabbitMQ semantics),
    ``"amqp"`` (a real RabbitMQ at ``amqp_url``, default ``$MF_AMQP_URL``) or a
    :class:`Bus`. Use it as an async context manager::

        async with FakeCore("pipeline.yaml", [init.app, hello.app]) as core:
            run = await core.run("hello-world", {"text": ""})
            assert run.status == "succeeded"
    """

    def __init__(
        self,
        spec: str | Path | Mapping[str, Any] | Iterable[str | Path | Mapping[str, Any]],
        modules: Iterable[Module[Any]] | Mapping[str, Module[Any]] = (),
        *,
        broker: Literal["memory", "amqp"] | Bus = "memory",
        amqp_url: str | None = None,
        prefix: str | None = None,
        replicas: Mapping[str, int] | None = None,
        module_schedules: bool = True,
        jitter: bool = True,
        runtime_settings: Mapping[str, Any] | None = None,
    ) -> None:
        """``module_schedules`` keeps the SDK's local ``on_start``/``every`` sources on (as
        in a deployment today); ``jitter=False`` makes retry delays exact for tests;
        ``runtime_settings`` are extra :class:`~manufacture.settings.RuntimeSettings`
        fields for the in-process replicas."""
        self.spec: SpecSet = plan(load_documents(spec))
        items = modules.values() if isinstance(modules, Mapping) else modules
        self._code: dict[str, Module[Any]] = {}
        for m in items:
            if m.name not in self.spec.modules:
                raise ValueError(f"module {m.name} is not declared in the spec")
            self._code[m.name] = m
        self._replicas = dict(replicas or {})
        bus, self._owns_bus, amqp_url = _make_bus(broker, amqp_url, prefix)
        if isinstance(bus, MemoryBus):
            missing = sorted(set(self._used_modules()) - set(self._code))
            if missing:
                raise ValueError(
                    "broker='memory' runs every module in-process; no code for: "
                    + ", ".join(missing)
                )
        self.bus = bus
        self.prefix = bus.prefix
        self._amqp_url = amqp_url
        self._module_schedules = module_schedules
        self._jitter = jitter
        self._runtime_settings = dict(runtime_settings or {})
        self.runtimes: list[Runtime[Any]] = []
        self._runs: dict[str, Run] = {}
        self._dedup: dict[tuple[str, str], str] = {}
        self._live: dict[str, _Live] = {}
        self._finished_attempts: set[str] = set()
        self._seen: deque[str] = deque(maxlen=10_000)
        self._seen_set: set[str] = set()
        self._replicas_seen: dict[tuple[str, str], ModuleReplica] = {}
        self._admission = {name: _Admission(p.max_runs) for name, p in self.spec.pipelines.items()}
        self._inbox: asyncio.Queue[bytes] = asyncio.Queue()
        self._tasks: dict[str, asyncio.Task[None]] = {}
        self._cancel_requested: set[str] = set()
        self._loop_task: asyncio.Task[None] | None = None
        self._tick = asyncio.Event()
        self._retry_after: dict[str, int] = {}
        self.events: list[CoreEvent] = []
        """State changes in order."""
        self.stale_results: list[StaleResult] = []
        self.rejections: list[Rejection] = []
        self.triggers: dict[str, str] = {}
        """``trigger_id`` -> pipeline of every ``source.trigger`` sent."""

    def _used_modules(self) -> Iterable[str]:
        for p in self.spec.pipelines.values():
            yield from (s.module for s in p.steps)
            if p.source is not None:
                yield p.source

    # --- lifecycle --------------------------------------------------------------------------

    async def start(self) -> None:
        """Declare the topology, start the engine, then the in-process replicas."""
        await self.bus.start(sorted(self.spec.modules), self._on_event)
        self._loop_task = asyncio.create_task(self._loop(), name="fakecore-engine")
        try:
            for name, module in self._code.items():
                count = self._replicas.get(name, self.spec.modules[name].replicas)
                for i in range(count):
                    await self.start_replica(module, f"{name}-{i}")
        except BaseException:
            await self.stop()
            raise

    async def start_replica(self, module: Module[Any], replica_id: str) -> Runtime[Any]:
        """Start one more in-process replica of ``module``."""
        plan_ = self.spec.modules[module.name]
        fields: dict[str, Any] = {
            "module_name": module.name,
            "replica_id": replica_id,
            "health_addr": "off",
            "amqp_prefix": self.prefix,
            "min_heartbeat_s": 0.05,
            "shutdown_grace_s": 5.0,
            "cancel_grace_s": 2.0,
        }
        if self._amqp_url is not None:
            fields["amqp_url"] = self._amqp_url
        if plan_.concurrency is not None:
            fields["concurrency"] = plan_.concurrency
        fields.update(self._runtime_settings)
        runtime = Runtime(
            module,
            settings=RuntimeSettings(**fields),
            transport=self.bus.transport(),
            local_schedules=self._module_schedules,
        )
        await runtime.start()
        self.runtimes.append(runtime)
        return runtime

    async def kill_replica(self, replica_id: str) -> None:
        """Crash a replica: no result, no ``module.offline``; its unacked work is redelivered."""
        for rt in list(self.runtimes):
            if rt.identity.replica_id == replica_id:
                self.runtimes.remove(rt)
                await rt.abort()
                return
        raise KeyError(replica_id)

    async def stop(self) -> None:
        """Stop the replicas gracefully, then the engine and the bus."""
        for rt in reversed(self.runtimes):
            with contextlib.suppress(Exception):
                await rt.stop()
        self.runtimes.clear()
        tasks = [*self._tasks.values()]
        if self._loop_task is not None:
            tasks.append(self._loop_task)
        for t in tasks:
            t.cancel()
        if tasks:
            await asyncio.wait(tasks)
        self._loop_task = None
        if self._owns_bus:
            with contextlib.suppress(Exception):
                await self.bus.delete_queues()
        await self.bus.close()

    async def __aenter__(self) -> Self:
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.stop()

    # --- API (mirrors the core gateway) -----------------------------------------------------

    async def start_run(
        self,
        pipeline: str,
        data: Mapping[str, Any] | None = None,
        *,
        dedup_key: str | None = None,
        priority: int = 0,
        labels: Mapping[str, str] | None = None,
    ) -> Run:
        """Create a run directly (``POST /pipelines/{name}/runs`` with data). With
        ``dedup_key`` a repeated call returns the existing run."""
        self.check()
        problems = self._admit_problems(pipeline, data or {})
        if problems:
            raise ValueError(f"run rejected ({problems[0]}): {problems[1]}")
        return self._create_run(
            pipeline,
            dict(data or {}),
            Trigger(kind="api"),
            dedup_key=dedup_key,
            priority=priority,
            labels=labels,
        )

    async def trigger(self, pipeline: str, data: Mapping[str, Any] | None = None) -> str:
        """Ask the pipeline's source module for runs (``source.trigger``); returns the
        ``trigger_id`` (the gateway's 202 answer). Runs appear once the module answers."""
        self.check()
        p = self._pipeline(pipeline)
        if p.source is None:
            raise ValueError(f"pipeline {pipeline} has no trigger.source; use start_run()")
        trigger_id = str(uuid.uuid4())
        self.triggers[trigger_id] = pipeline
        payload = wire.SourceTrigger(
            pipeline=pipeline, trigger_id=trigger_id, data=dict(data) if data is not None else None
        )
        mid, body = wire.encode(type_="source.trigger", producer=PRODUCER, payload=payload)
        await self.bus.send_work(p.source, body, message_id=str(mid), type_="source.trigger")
        return trigger_id

    async def run(
        self,
        pipeline: str,
        data: Mapping[str, Any] | None = None,
        *,
        timeout: float = 30.0,
        dedup_key: str | None = None,
        priority: int = 0,
        labels: Mapping[str, str] | None = None,
    ) -> Run:
        """:meth:`start_run` and :meth:`wait` in one call."""
        run = await self.start_run(
            pipeline, data, dedup_key=dedup_key, priority=priority, labels=labels
        )
        return await self.wait(run.id, timeout=timeout)

    async def cancel(self, run_id: str) -> Run:
        """Cancel a run; the current attempt gets ``step.cancel {reason: cancelled}``."""
        run = self._runs[run_id]
        if run.done:
            return run
        self._cancel_requested.add(run_id)
        for live in self._live.values():
            if live.run.id == run_id:
                live.changed.set()
        if run.status == "queued":
            task = self._tasks.get(run_id)
            if task is not None:
                task.cancel()
            await self._finish(run, "cancelled", None)
        return await self.wait(run_id, timeout=30)

    def get_run(self, run_id: str) -> Run:
        return self._runs[run_id]

    @property
    def runs(self) -> list[Run]:
        """All runs, oldest first."""
        return list(self._runs.values())

    def runs_of(self, pipeline: str | None = None, *, trigger_id: str | None = None) -> list[Run]:
        return [
            r
            for r in self._runs.values()
            if (pipeline is None or r.pipeline == pipeline)
            and (trigger_id is None or r.trigger.trigger_id == trigger_id)
        ]

    @property
    def replicas(self) -> list[ModuleReplica]:
        """Replicas known from ``module.online``/``module.heartbeat``/``module.offline``."""
        return list(self._replicas_seen.values())

    @property
    def dead_letters(self) -> list[DeadLetter]:
        return self.bus.dead_letters

    def check(self) -> None:
        """Raise :class:`ProtocolViolationError` if a module sent an invalid message."""
        violations = self.bus.violations
        if violations:
            raise ProtocolViolationError(
                "invalid envelopes from modules: " + " | ".join(violations)
            )

    async def wait(self, run_id: str, *, timeout: float = 30.0) -> Run:
        """Until the run is terminal or ``needs_attention``."""
        await self._wait_until(lambda: self._runs[run_id].done, timeout, f"run {run_id}")
        return self._runs[run_id]

    async def wait_for_runs(
        self,
        pipeline: str | None = None,
        *,
        count: int = 1,
        trigger_id: str | None = None,
        timeout: float = 30.0,
    ) -> list[Run]:
        """Until ``count`` runs (of ``pipeline``/``trigger_id``) exist and are done."""

        def ready() -> bool:
            runs = self.runs_of(pipeline, trigger_id=trigger_id)
            return len(runs) >= count and all(r.done for r in runs[:count])

        await self._wait_until(ready, timeout, f"{count} run(s) of {pipeline or 'any pipeline'}")
        return self.runs_of(pipeline, trigger_id=trigger_id)[:count]

    async def wait_for_replicas(self, count: int | None = None, *, timeout: float = 10.0) -> None:
        """Until ``count`` replicas (default: every in-process one) said ``module.online``."""
        want = len(self.runtimes) if count is None else count
        await self._wait_until(
            lambda: sum(r.status == "online" for r in self._replicas_seen.values()) >= want,
            timeout,
            f"{want} replica(s) online",
        )

    async def wait_until(
        self, cond: Callable[[], object], *, timeout: float = 10.0, what: str = "condition"
    ) -> None:
        """Until ``cond()`` is true. Re-checked on every engine change and every 50 ms (so
        conditions on the test's own state work too); the timeout error lists the runs."""
        await self._wait_until(lambda: bool(cond()), timeout, what)

    async def _wait_until(self, cond: Callable[[], bool], timeout: float, what: str) -> None:
        async def waiter() -> None:
            while True:
                self.check()
                if cond():
                    return
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(self._tick.wait(), 0.05)

        try:
            await asyncio.wait_for(waiter(), timeout)
        except TimeoutError:
            raise TimeoutError(
                f"timed out after {timeout}s waiting for {what}; {self.describe()}"
            ) from None

    def describe(self) -> str:
        """A one-line summary of every run, for failure messages."""
        parts = []
        for r in self._runs.values():
            step = r.steps[-1] if r.steps else None
            where = f" at {step.step_id}#{len(step.attempts)} {step.status}" if step else ""
            parts.append(f"{r.pipeline}/{r.id[:8]} {r.status}{where}")
        return "runs: " + (", ".join(parts) or "none")

    # --- engine -----------------------------------------------------------------------------

    def _pipeline(self, name: str) -> PipelinePlan:
        try:
            return self.spec.pipelines[name]
        except KeyError:
            raise KeyError(f"unknown pipeline {name}") from None

    def _admit_problems(
        self, pipeline: str, data: Mapping[str, Any]
    ) -> tuple[Literal["unknown_pipeline", "invalid_data"], str] | None:
        p = self.spec.pipelines.get(pipeline)
        if p is None:
            return ("unknown_pipeline", f"no pipeline {pipeline}")
        errors = p.check_data(data)
        if errors:
            return ("invalid_data", "; ".join(errors))
        return None

    def _create_run(
        self,
        pipeline: str,
        data: dict[str, Any],
        trigger: Trigger,
        *,
        dedup_key: str | None,
        priority: int = 0,
        labels: Mapping[str, str] | None = None,
    ) -> Run:
        if dedup_key is not None and (pipeline, dedup_key) in self._dedup:
            return self._runs[self._dedup[(pipeline, dedup_key)]]
        p = self._pipeline(pipeline)
        run = Run(
            id=str(uuid.uuid4()),
            pipeline=pipeline,
            revision=p.revision,
            trigger=trigger,
            data=data,
            created_at=_now(),
            dedup_key=dedup_key,
            priority=priority,
            labels=dict(labels or {}),
        )
        self._runs[run.id] = run
        if dedup_key is not None:
            self._dedup[(pipeline, dedup_key)] = run.id
        self._emit(CoreEvent("run", run.id, run.status))
        self._tasks[run.id] = asyncio.create_task(
            self._drive(run, p), name=f"fakecore-run-{run.id}"
        )
        return run

    def _emit(self, event: CoreEvent) -> None:
        self.events.append(event)
        self._notify()

    def _notify(self) -> None:
        # Wake every waiter once; each re-checks its condition (single-threaded loop).
        self._tick.set()
        self._tick = asyncio.Event()

    async def _finish(
        self,
        run: Run,
        status: Literal["succeeded", "failed", "needs_attention", "cancelled"],
        error: ErrorInfo | None,
    ) -> None:
        if run.status in ("succeeded", "failed", "cancelled"):
            return
        run.status = status
        run.error = error
        if status != "needs_attention":
            run.finished_at = _now()
        self._emit(CoreEvent("run", run.id, status))

    async def _drive(self, run: Run, p: PipelinePlan) -> None:
        admission = self._admission[p.name]
        try:
            await admission.acquire(run.priority)
        except asyncio.CancelledError:
            return
        try:
            await self._execute(run, p)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001  # pragma: no cover - engine bug: fail the run, keep the test informative
            _log.exception("fake core engine error", run_id=run.id)
            await self._finish(run, "failed", ErrorInfo("fatal", "fakecore_error", repr(exc)))
        finally:
            admission.release()
            self._tasks.pop(run.id, None)

    async def _execute(self, run: Run, p: PipelinePlan) -> None:
        run.status = "running"
        run.started_at = _now()
        self._emit(CoreEvent("run", run.id, "running"))
        for sp in p.steps:
            if run.id in self._cancel_requested:
                await self._finish(run, "cancelled", None)
                return
            run.stage_index, run.stage = sp.stage_index, sp.stage
            step = Step(
                stage=sp.stage,
                stage_index=sp.stage_index,
                step_id=sp.step_id,
                module=sp.module,
                step_key=step_key(run.id, sp.stage, sp.step_id, None, 1),
                optional=sp.optional,
            )
            run.steps.append(step)
            outcome = await self._execute_step(run, step, sp)
            if outcome == "succeeded":
                if sp.merge and step.output:
                    run.data = merge_patch(run.data, step.output)
                continue
            if outcome == "cancelled":
                await self._finish(run, "cancelled", None)
                return
            if outcome == "attention":
                await self._finish(run, "needs_attention", step.error)
                return
            if not sp.optional:
                await self._finish(run, "failed", step.error)
                return
        run.stage_index = run.stage = None
        await self._finish(run, "succeeded", None)

    def _set_step(self, run: Run, step: Step, status: Any, number: int | None = None) -> None:  # noqa: ANN401
        step.status = status
        self._emit(CoreEvent("step", run.id, status, step.step_id, number))

    async def _execute_step(
        self, run: Run, step: Step, sp: StepPlan
    ) -> Literal["succeeded", "failed", "attention", "cancelled"]:
        for number in range(1, sp.retry.max_attempts + 1):
            attempt = Attempt(attempt_id=str(uuid.uuid4()), number=number)
            step.attempts.append(attempt)
            result = await self._attempt(run, step, sp, attempt)
            if result == "succeeded":
                self._set_step(run, step, "succeeded", number)
                return "succeeded"
            if result == "cancelled":
                self._set_step(run, step, "cancelled", number)
                return "cancelled"
            err = attempt.error
            assert err is not None  # noqa: S101
            step.error = err
            if err.class_ == "attention":
                self._set_step(run, step, "failed", number)
                return "attention"
            retry_after = self._retry_after.pop(attempt.attempt_id, 0)
            if (
                err.class_ == "fatal"
                or not sp.retry.retries(err.class_)
                or number == sp.retry.max_attempts
            ):
                break
            delay = sp.retry.delay(number)
            if self._jitter and sp.retry.jitter:
                delay *= random.uniform(0.5, 1.0)  # noqa: S311 (not security relevant)
            delay = max(delay, float(retry_after))
            self._set_step(run, step, "pending", number)
            if await self._sleep_unless_cancelled(run, delay):
                self._set_step(run, step, "cancelled", number)
                return "cancelled"
        self._set_step(run, step, "failed", len(step.attempts))
        return "failed"

    async def _sleep_unless_cancelled(self, run: Run, delay: float) -> bool:
        end = asyncio.get_running_loop().time() + delay
        while (left := end - asyncio.get_running_loop().time()) > 0:
            if run.id in self._cancel_requested:
                return True
            await asyncio.sleep(min(left, 0.05))
        return run.id in self._cancel_requested

    def _set_attempt(self, live: _Live, status: Any) -> None:  # noqa: ANN401
        live.attempt.status = status
        self._emit(
            CoreEvent("attempt", live.run.id, status, live.step.step_id, live.attempt.number)
        )

    async def _attempt(
        self, run: Run, step: Step, sp: StepPlan, attempt: Attempt
    ) -> Literal["succeeded", "failed", "cancelled"]:
        loop = asyncio.get_running_loop()
        live = _Live(run, step, attempt, sp)
        self._live[attempt.attempt_id] = live
        try:
            deadline = _now() + timedelta(seconds=sp.timeout_s)
            payload = wire.StepRequested(
                pipeline=run.pipeline,
                revision=run.revision,
                stage=sp.stage,
                step=sp.step_id,
                input=run.data,
                config=dict(sp.config),
                deadline=deadline,
                lease_seconds=sp.lease_s,
                blob_prefix=f"s3://mf-blobs/runs/{run.id}/{sp.step_id}/{attempt.attempt_id}/",
            )
            mid, body = wire.encode(
                type_="step.requested",
                producer=PRODUCER,
                payload=payload,
                run_id=run.id,
                attempt=wire.AttemptRef(id=attempt.attempt_id, number=attempt.number),
            )
            attempt.dispatched_at = _now()
            self._set_attempt(live, "dispatched")
            self._set_step(run, step, "dispatched", attempt.number)
            await self.bus.send_work(sp.module, body, message_id=str(mid), type_="step.requested")
            # 0.2.1: the timeout runs from dispatch (payload.deadline). The lease ticks
            # from the first sign of life, so work waiting in the queue is not "lost".
            # (0.3.0 moves the timeout start to the first heartbeat: change only here.)
            timeout_at = loop.time() + sp.timeout_s
            while True:
                if live.result is not None:
                    return self._apply_result(live)
                if run.id in self._cancel_requested:
                    await self._cancel_attempt(live, "cancelled", "cancelled")
                    return "cancelled"
                now = loop.time()
                lease_at = (
                    live.last_seen + sp.lease_s if live.last_seen is not None else float("inf")
                )
                if now >= timeout_at:
                    await self._cancel_attempt(live, "timeout", "timed_out")
                    attempt.error = ErrorInfo(
                        "timeout", "timeout", f"no result within {sp.timeout_s:g}s"
                    )
                    return "failed"
                if now >= lease_at:
                    await self._cancel_attempt(live, "lost", "lost")
                    attempt.error = ErrorInfo("lost", "lost", f"no heartbeat for {sp.lease_s}s")
                    return "failed"
                live.changed.clear()
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(live.changed.wait(), min(timeout_at, lease_at) - now)
        finally:
            self._live.pop(attempt.attempt_id, None)
            self._finished_attempts.add(attempt.attempt_id)
            attempt.finished_at = attempt.finished_at or _now()

    async def _cancel_attempt(self, live: _Live, reason: wire.CancelReason, status: Any) -> None:  # noqa: ANN401
        self._set_attempt(live, status)
        mid, body = wire.encode(
            type_="step.cancel",
            producer=PRODUCER,
            payload=wire.StepCancel(reason=reason),
            run_id=live.run.id,
            attempt=wire.AttemptRef(id=live.attempt.attempt_id, number=live.attempt.number),
        )
        await self.bus.send_control(
            live.plan.module, body, message_id=str(mid), type_="step.cancel"
        )

    def _apply_result(self, live: _Live) -> Literal["succeeded", "failed"]:
        result, attempt = live.result, live.attempt
        attempt.finished_at = _now()
        if isinstance(result, wire.StepSucceeded):
            attempt.output = result.output
            live.step.output = result.output
            live.step.error = None
            self._set_attempt(live, "succeeded")
            return "succeeded"
        assert isinstance(result, wire.StepFailed)  # noqa: S101
        attempt.error = ErrorInfo(result.error_class, result.code or "", result.message)
        if result.retry_after_seconds:
            self._retry_after[attempt.attempt_id] = result.retry_after_seconds
        self._set_attempt(live, "failed")
        return "failed"

    # --- inbound ----------------------------------------------------------------------------

    async def _on_event(self, body: bytes) -> None:
        self._inbox.put_nowait(body)

    async def _loop(self) -> None:
        while True:
            body = await self._inbox.get()
            try:
                self._handle(body)
            except Exception:  # noqa: BLE001  # pragma: no cover - keep the engine alive, report the bug
                _log.exception("fake core could not handle a message")

    def _handle(self, body: bytes) -> None:
        env = wire.decode(body)
        mid = str(env.message_id)
        if env.type in {"step.succeeded", "step.failed", "run.requested"}:
            if mid in self._seen_set:
                return  # at-least-once delivery: apply each message once
            if len(self._seen) == self._seen.maxlen:
                self._seen_set.discard(self._seen[0])
            self._seen.append(mid)
            self._seen_set.add(mid)
        match env.type:
            case "step.heartbeat" | "step.succeeded" | "step.failed":
                self._on_step_event(env)
            case "run.requested":
                self._on_run_requested(env)
            case "module.online" | "module.heartbeat" | "module.offline":
                self._on_module_event(env)
            case _:
                _log.warning("fake core ignored a message", type=env.type)
        self._notify()

    def _current(self, env: wire.Envelope) -> _Live | None:
        """The live attempt a step event is about; ``None`` (and a stale record for
        results) when the attempt is not current any more."""
        attempt_id = env.attempt.id if env.attempt is not None else ""
        live = self._live.get(attempt_id)
        if live is not None and live.result is None:
            return live
        if env.type != "step.heartbeat":
            reason: Literal["not_current", "unknown_attempt"] = (
                "not_current"
                if live is not None or attempt_id in self._finished_attempts
                else "unknown_attempt"
            )
            self.stale_results.append(StaleResult(env.type, attempt_id, reason))
        return None

    def _on_step_event(self, env: wire.Envelope) -> None:
        live = self._current(env)
        if live is None:
            return
        try:
            payload = _STEP_EVENTS[env.type].model_validate(env.payload)
        except ValidationError as exc:
            self.bus.violations.append(f"{env.type}: {exc}")
            return
        live.last_seen = asyncio.get_running_loop().time()
        attempt = live.attempt
        if payload.replica_id is not None:
            attempt.replica_id = payload.replica_id
        if attempt.started_at is None:
            attempt.started_at = _now()
        if isinstance(payload, wire.StepHeartbeat):
            attempt.progress = (
                payload.progress if payload.progress is not None else attempt.progress
            )
            attempt.message = payload.message or attempt.message
            if attempt.status == "dispatched":
                self._set_attempt(live, "running")
                self._set_step(live.run, live.step, "running", attempt.number)
        else:
            live.result = payload
        live.changed.set()

    def _on_run_requested(self, env: wire.Envelope) -> None:
        req = wire.RunRequested.model_validate(env.payload)
        producer = env.producer.removeprefix("module:")
        problems = self._admit_problems(req.pipeline, req.data)
        if problems is not None:
            self.rejections.append(
                Rejection(req.pipeline, req.dedup_key, problems[0], problems[1], producer)
            )
            _log.warning("run request rejected", pipeline=req.pipeline, code=problems[0])
            return
        self._create_run(
            req.pipeline,
            req.data,
            Trigger(kind="source", by=producer, trigger_id=req.trigger_id),
            dedup_key=req.dedup_key,
        )

    def _on_module_event(self, env: wire.Envelope) -> None:
        now = _now()
        if env.type == "module.online":
            on = wire.ModuleOnline.model_validate(env.payload)
            self._replicas_seen[(on.module, on.replica_id)] = ModuleReplica(
                on.module,
                on.replica_id,
                "online",
                0,
                on.concurrency or 1,
                on.sdk.name + "/" + on.sdk.version,
                now,
            )
        elif env.type == "module.heartbeat":
            hb = wire.ModuleHeartbeat.model_validate(env.payload)
            prev = self._replicas_seen.get((hb.module, hb.replica_id))
            self._replicas_seen[(hb.module, hb.replica_id)] = ModuleReplica(
                hb.module,
                hb.replica_id,
                "online",
                hb.in_flight or 0,
                prev.concurrency if prev else 1,
                prev.sdk if prev else None,
                now,
            )
        else:
            off = wire.ModuleOffline.model_validate(env.payload)
            prev = self._replicas_seen.get((off.module, off.replica_id))
            self._replicas_seen[(off.module, off.replica_id)] = ModuleReplica(
                off.module,
                off.replica_id,
                "offline",
                0,
                prev.concurrency if prev else 1,
                prev.sdk if prev else None,
                now,
            )

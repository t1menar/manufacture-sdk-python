"""Module runtime: consumes commands, runs handlers, reports results to the core.

Implements the replica side of ``docs/contract/messaging.md`` (contract 0.2.1):

- **Lifecycle**: connect, declare the work and control queues, ``module.online``,
  ``module.heartbeat`` every 10 s, ``module.offline`` on graceful shutdown.
- **Late ack**: a ``step.requested`` delivery is acked only after the result is confirmed
  by the broker, so prefetch (= concurrency) bounds in-flight work and the queue depth is an
  honest load metric. If the replica dies, the broker redelivers.
- **Idempotency by attempt id**: a duplicate delivery of a running attempt never starts the
  handler again (it is acked right away, messaging.md §4); a duplicate of a finished attempt
  re-publishes the cached result (same ``message_id`` = ``uuid5(NS, "<attempt>:<type>")``).
- **Heartbeats** every ``lease_seconds / 3`` while a handler runs, plus ``ctx.progress``.
- **Deadline**: the handler is cancelled at ``payload.deadline``; nothing is reported.
- **Cancellation**: ``step.cancel`` sets ``ctx.cancelled`` and cancels the handler task
  after ``cancel_grace_s``. Nothing is reported for a cancelled attempt.
- **Graceful shutdown** on SIGTERM: stop consuming, wait ``shutdown_grace_s``, then cancel
  what is left and report ``transient/module_shutdown`` so the core retries elsewhere.
"""

from __future__ import annotations

import asyncio
import contextlib
import math
import signal
import time
import uuid
from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Final, Generic, Literal

from pydantic import BaseModel, ValidationError

from manufacture import _wire as wire
from manufacture._contract import CONTRACT_VERSION
from manufacture._log import StructLogger, get_logger
from manufacture._version import __version__
from manufacture.app import RunRequest
from manufacture.context import Context, S, Secrets, SourceContext
from manufacture.errors import StepError
from manufacture.health import HealthServer
from manufacture.settings import RuntimeSettings
from manufacture.transport import Delivery, Identity, Inbound, Transport

if TYPE_CHECKING:
    from collections.abc import Coroutine

    from manufacture.app import Module, SourceSpec, StepSpec

__all__ = ["Outcome", "Runtime", "classify_exception"]

_COMPLETED_CAPACITY: Final = 10_000
_MAX_ERROR_MESSAGE: Final = 1000

StopReason = Literal["cancel", "shutdown", "deadline"]
ResultType = Literal["step.succeeded", "step.failed"]


@dataclass(frozen=True, slots=True)
class Outcome:
    """A serialized ``step.succeeded`` / ``step.failed`` ready to (re-)publish."""

    type: ResultType
    message_id: uuid.UUID
    body: bytes


def classify_exception(exc: BaseException) -> wire.StepFailed:
    """Map a handler exception to the error classes of the contract."""
    if isinstance(exc, StepError):
        retry = math.ceil(exc.retry_after_s) if exc.retry_after_s is not None else None
        return wire.StepFailed(
            error_class=exc.error_class,
            code=exc.code,
            message=exc.message[:_MAX_ERROR_MESSAGE],
            retry_after_seconds=retry,
        )
    text = f"{type(exc).__name__}: {exc}"[:_MAX_ERROR_MESSAGE]
    return wire.StepFailed(error_class="transient", code="unhandled", message=text)


def _failed(error_class: wire.ErrorClass, code: str, message: str) -> wire.StepFailed:
    return wire.StepFailed(error_class=error_class, code=code, message=message[:_MAX_ERROR_MESSAGE])


@dataclass(eq=False)
class _Execution:
    env: wire.Envelope
    attempt: wire.AttemptRef
    request: wire.StepRequested
    ctx: Context[Any]
    deliveries: list[Delivery]
    task: asyncio.Task[None] | None = None
    handler_task: asyncio.Task[Any] | None = None
    stop_reason: StopReason | None = None
    progress: float | None = None
    message: str | None = None
    last_beat: float = 0.0
    started: float = field(default_factory=time.monotonic)

    @property
    def duration_ms(self) -> int:
        return int((time.monotonic() - self.started) * 1000)


class Runtime(Generic[S]):
    """Runs one :class:`~manufacture.Module` replica. Usually created by ``Module.serve``."""

    def __init__(
        self,
        module: Module[S],
        *,
        settings: RuntimeSettings | None = None,
        transport: Transport | None = None,
        secrets: Secrets | None = None,
    ) -> None:
        self.module = module
        self.settings = settings or RuntimeSettings()
        if self.settings.module_name and self.settings.module_name != module.name:
            raise ValueError(
                f"MF_MODULE_NAME={self.settings.module_name!r} but the code declares "
                f"Module({module.name!r})"
            )
        self.identity = Identity(module.name, self.settings.replica_for(module.name))
        self.topology = wire.Topology(prefix=self.settings.amqp_prefix)
        if transport is None:
            from manufacture.transport.amqp import AmqpTransport  # noqa: PLC0415

            transport = AmqpTransport(
                self.settings.amqp_url, self.topology, declare=self.settings.declare_topology
            )
        self.transport = transport
        self.producer = f"module:{module.name}"
        self.concurrency = self.settings.concurrency or module.concurrency
        self.secrets = secrets or Secrets()
        self.log: StructLogger = get_logger(
            "manufacture.runtime", module=module.name, replica=self.identity.replica_id
        )
        self._executions: dict[str, _Execution] = {}
        self._completed: OrderedDict[str, Outcome | None] = OrderedDict()
        self._cancelled: OrderedDict[str, None] = OrderedDict()
        self._background: set[asyncio.Task[Any]] = set()
        self._source_runs: set[asyncio.Task[Any]] = set()
        self._shutdown = asyncio.Event()
        self._accepting = False
        self._started = False
        self._health: HealthServer | None = None

    # --- state ------------------------------------------------------------------------------

    @property
    def ready(self) -> bool:
        """Readiness: consuming and connected to the broker."""
        return self._accepting and self.transport.ready

    @property
    def in_flight(self) -> int:
        return len(self._executions)

    @property
    def health_port(self) -> int | None:
        return self._health.bound_port if self._health is not None else None

    def request_shutdown(self) -> None:
        self._shutdown.set()

    # --- lifecycle --------------------------------------------------------------------------

    async def serve(self, *, install_signal_handlers: bool = True) -> None:
        if install_signal_handlers:
            loop = asyncio.get_running_loop()
            for sig in (signal.SIGTERM, signal.SIGINT):
                with contextlib.suppress(NotImplementedError, RuntimeError):
                    loop.add_signal_handler(sig, self.request_shutdown)
        try:
            await self.start()
            await self._shutdown.wait()
        finally:
            await self.stop()

    async def start(self) -> None:
        if self._started:
            raise RuntimeError("runtime already started")
        spec, sources = self.module.step_spec, self.module.sources
        if spec is None and not sources:
            raise RuntimeError(f"module {self.module.name} has no @step or @source handler")
        _ = self.module.settings  # fail fast on invalid module settings
        self._started = True
        try:
            if (addr := self.settings.health) is not None:
                self._health = HealthServer(addr[0], addr[1], ready=lambda: self.ready)
                await self._health.start()
            await self.transport.connect(self.identity)
            await self.transport.consume(Inbound.CONTROL, self._on_control, prefetch=64)
            await self.transport.consume(Inbound.WORK, self._on_work, prefetch=self.concurrency)
        except BaseException:
            await self.abort()
            raise
        self._accepting = True
        await self._publish_best_effort("module.online", self._online_payload())
        self._spawn(self._module_heartbeat_loop())
        for src in sources:
            if src.on_start:
                self._spawn_source(src, str(uuid.uuid4()), "start", {}, send_trigger_id=False)
            if src.every_s is not None:
                self._spawn(self._every_loop(src, src.every_s))
        self.log.info(
            "module started",
            version=self.module.version,
            sdk_version=__version__,
            contract_version=CONTRACT_VERSION,
            concurrency=self.concurrency,
        )

    async def stop(self) -> None:
        """Graceful shutdown; safe to call more than once."""
        if not self._started:
            return
        self._started = False
        self._accepting = False
        self.log.info("shutting down", in_flight=self.in_flight)
        with contextlib.suppress(Exception):
            await self.transport.stop_consuming(Inbound.WORK)
        pending = {t for e in self._executions.values() if (t := e.task) is not None}
        pending |= self._source_runs
        if pending:
            await asyncio.wait(pending, timeout=self.settings.shutdown_grace_s)
        for ex in list(self._executions.values()):
            self._stop_execution(ex, "shutdown", grace_s=0)
        for task in list(self._source_runs):
            task.cancel()
        leftovers = {t for e in self._executions.values() if (t := e.task) is not None}
        if leftovers | self._source_runs:
            await asyncio.wait(leftovers | self._source_runs, timeout=10)
        for task in list(self._background):
            task.cancel()
        await asyncio.gather(*self._background, return_exceptions=True)
        await self._publish_best_effort(
            "module.offline",
            wire.ModuleOffline(module=self.module.name, replica_id=self.identity.replica_id),
        )
        if self._health is not None:
            await self._health.stop()
        with contextlib.suppress(Exception):
            await self.transport.close()
        self.log.info("stopped")

    async def abort(self) -> None:
        """Stop at once without reporting anything, as if the process had crashed.

        Unacked deliveries go back to the queue when the connection closes. Used when
        :meth:`start` fails and by tests that simulate a crashed replica.
        """
        self._started = False
        self._accepting = False
        tasks = {t for e in self._executions.values() if (t := e.task) is not None}
        tasks |= self._source_runs | self._background
        with contextlib.suppress(Exception):
            await self.transport.close()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self._executions.clear()
        if self._health is not None:
            with contextlib.suppress(Exception):
                await self._health.stop()
            self._health = None

    # --- work queue -------------------------------------------------------------------------

    async def _on_work(self, delivery: Delivery) -> None:
        try:
            env = wire.decode(delivery.body)
        except wire.EnvelopeError as exc:
            self.log.error("dead-lettering invalid message", reason=str(exc)[:300])
            await self._settle(delivery, "reject")
            return
        if env.type == "step.requested":
            await self._on_step(env, delivery)
        elif env.type == "source.trigger":
            await self._on_trigger(env, delivery)
        else:
            self.log.error("dead-lettering unexpected message on the work queue", type=env.type)
            await self._settle(delivery, "reject")

    # --- step.requested ---------------------------------------------------------------------

    async def _on_step(self, env: wire.Envelope, delivery: Delivery) -> None:
        attempt = env.attempt
        if attempt is None:  # the contract schema requires it for step messages
            await self._settle(delivery, "reject")
            return
        request = wire.StepRequested.model_validate(env.payload)
        log = self.log.bind(
            run_id=env.run_id, step=request.step, attempt_id=attempt.id, attempt=attempt.number
        )
        if self.module.step_spec is None:
            log.error("step.requested for a module without @step")
            outcome = self._outcome(
                env, attempt, _failed("fatal", "no_step_handler", "module has no step handler"), 0
            )
            await self._publish_and_settle(outcome, [delivery])
            return
        if not self._accepting:
            await self._settle(delivery, "requeue")
            return
        if attempt.id in self._completed:
            log.info("duplicate of a finished attempt")
            await self._publish_and_settle(self._completed[attempt.id], [delivery])
            return
        if attempt.id in self._cancelled:
            log.info("attempt was cancelled; dropping")
            await self._settle(delivery, "ack")
            return
        if attempt.id in self._executions:
            # messaging.md §4: a duplicate of an attempt running here is acked without a start.
            log.info("duplicate delivery of a running attempt; acking")
            await self._settle(delivery, "ack")
            return
        if request.deadline <= datetime.now(UTC):
            log.warning("deadline already passed; dropping", deadline=request.deadline.isoformat())
            self._remember(attempt.id, None)
            await self._settle(delivery, "ack")
            return
        self._start_execution(env, attempt, request, delivery, log)

    def _start_execution(
        self,
        env: wire.Envelope,
        attempt: wire.AttemptRef,
        request: wire.StepRequested,
        delivery: Delivery,
        log: StructLogger,
    ) -> None:
        holder: list[_Execution] = []

        async def sink(progress: float | None, message: str | None) -> None:
            await self._report_progress(holder[0], progress, message)

        ctx: Context[Any] = Context(
            module=self.module.name,
            run_id=env.run_id or "",
            pipeline=request.pipeline,
            revision=request.revision,
            stage=request.stage,
            step=request.step,
            item_index=request.item_index,
            attempt_id=attempt.id,
            attempt=attempt.number,
            deadline=request.deadline,
            config=request.config,
            settings=self.module.settings,
            secrets=self.secrets,
            log=log,
            progress_sink=sink,
            blob_prefix=request.blob_prefix,
            traceparent=env.traceparent,
        )
        ex = _Execution(env=env, attempt=attempt, request=request, ctx=ctx, deliveries=[delivery])
        holder.append(ex)
        self._executions[attempt.id] = ex
        ex.task = asyncio.create_task(self._run(ex), name=f"mf-step-{attempt.id}")

    async def _run(self, ex: _Execution) -> None:
        heartbeat = asyncio.create_task(self._heartbeat_loop(ex))
        try:
            outcome = await self._invoke(ex)
        finally:
            heartbeat.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await heartbeat
        self._executions.pop(ex.attempt.id, None)
        self._remember(ex.attempt.id, outcome)
        await self._publish_and_settle(outcome, ex.deliveries)

    async def _invoke(self, ex: _Execution) -> Outcome | None:
        spec: StepSpec[Any, Any, Any] | None = self.module.step_spec
        if spec is None:
            raise RuntimeError("no step handler registered")
        log = ex.ctx.log
        try:
            inp = spec.input_model.model_validate(ex.request.input)
        except ValidationError as exc:
            log.error("invalid step input", errors=exc.error_count())
            return self._fail(ex, _failed("fatal", "input_invalid", _summarize(exc)))
        log.info("step started")
        ex.handler_task = asyncio.create_task(spec.handler(ex.ctx, inp))
        try:
            done, _ = await asyncio.wait({ex.handler_task}, timeout=max(ex.ctx.remaining_s, 0))
        except asyncio.CancelledError:
            ex.handler_task.cancel()
            raise
        if not done:
            self._stop_execution(ex, "deadline", grace_s=0)
            with contextlib.suppress(BaseException):
                await ex.handler_task

        if ex.stop_reason in {"cancel", "deadline"}:
            log.info("step stopped", reason=ex.stop_reason, duration_ms=ex.duration_ms)
            return None
        try:
            returned = ex.handler_task.result()
        except asyncio.CancelledError:
            if ex.stop_reason == "shutdown":
                log.warning("step interrupted by shutdown", duration_ms=ex.duration_ms)
                return self._fail(
                    ex,
                    _failed("transient", "module_shutdown", "replica shut down before finishing"),
                )
            log.warning("handler cancelled itself", duration_ms=ex.duration_ms)
            return self._fail(ex, _failed("transient", "cancelled", "handler was cancelled"))
        except Exception as exc:
            failure = classify_exception(exc)
            if isinstance(exc, StepError):
                log.warning("step failed", error_class=failure.error_class, code=failure.code)
            else:
                log.exception("step raised", duration_ms=ex.duration_ms)
            return self._fail(ex, failure)

        try:
            model = spec.output_model.model_validate(
                returned.model_dump() if isinstance(returned, BaseModel) else returned
            )
        except ValidationError as exc:
            log.error("invalid step output", errors=exc.error_count())
            return self._fail(ex, _failed("fatal", "output_invalid", _summarize(exc)))
        succeeded = wire.StepSucceeded(
            output=model.model_dump(mode="json"),
            replica_id=self.identity.replica_id,
            duration_ms=ex.duration_ms,
        )
        outcome = self._outcome(ex.env, ex.attempt, succeeded, ex.duration_ms)
        if len(outcome.body) > wire.MAX_MESSAGE_BYTES:
            log.error("step output too large", size=len(outcome.body))
            return self._fail(
                ex,
                _failed(
                    "fatal",
                    "output_too_large",
                    f"result is {len(outcome.body)} bytes; limit {wire.MAX_MESSAGE_BYTES} "
                    "(large values need blobs)",
                ),
            )
        log.info("step succeeded", duration_ms=ex.duration_ms)
        return outcome

    def _fail(self, ex: _Execution, failure: wire.StepFailed) -> Outcome:
        return self._outcome(ex.env, ex.attempt, failure, ex.duration_ms)

    def _stop_execution(self, ex: _Execution, reason: StopReason, *, grace_s: float) -> None:
        if ex.stop_reason is None:
            ex.stop_reason = reason
        ex.ctx.cancelled.set()
        task = ex.handler_task
        if task is None or task.done():
            return
        if grace_s <= 0:
            task.cancel()
            return

        async def force() -> None:
            await asyncio.wait({task}, timeout=grace_s)
            if not task.done():
                ex.ctx.log.warning("handler ignored cancellation; cancelling task", grace_s=grace_s)
                task.cancel()

        self._spawn(force())

    # --- control queue: step.cancel ---------------------------------------------------------

    async def _on_control(self, delivery: Delivery) -> None:
        try:
            env = wire.decode(delivery.body)
            if env.type != "step.cancel" or env.attempt is None:
                raise wire.EnvelopeError(f"unexpected {env.type} on the control queue")
        except wire.EnvelopeError as exc:
            self.log.error("dropping invalid control message", reason=str(exc)[:300])
            await self._settle(delivery, "reject")
            return
        cancel = wire.StepCancel.model_validate(env.payload)
        attempt_id = env.attempt.id
        self._cancelled[attempt_id] = None
        while len(self._cancelled) > _COMPLETED_CAPACITY:
            self._cancelled.popitem(last=False)
        if (ex := self._executions.get(attempt_id)) is not None:
            ex.ctx.log.info("cancel requested", reason=cancel.reason)
            self._stop_execution(ex, "cancel", grace_s=self.settings.cancel_grace_s)
        await self._settle(delivery, "ack")

    # --- heartbeat and progress -------------------------------------------------------------

    async def _heartbeat_loop(self, ex: _Execution) -> None:
        period = max(self.settings.min_heartbeat_s, ex.request.lease_seconds / 3)
        while True:
            await asyncio.sleep(period)
            if time.monotonic() - ex.last_beat >= period * 0.5:
                await self._send_heartbeat(ex)

    async def _report_progress(
        self, ex: _Execution, progress: float | None, message: str | None
    ) -> None:
        if progress is not None:
            ex.progress = progress
        if message is not None:
            ex.message = message[:1024]
        if time.monotonic() - ex.last_beat >= self.settings.min_heartbeat_s:
            await self._send_heartbeat(ex)

    async def _send_heartbeat(self, ex: _Execution) -> None:
        if ex.stop_reason is not None:
            return
        ex.last_beat = time.monotonic()
        payload = wire.StepHeartbeat(
            progress=ex.progress, message=ex.message, replica_id=self.identity.replica_id
        )
        await self._publish_best_effort(
            "step.heartbeat",
            payload,
            run_id=ex.env.run_id,
            attempt=ex.attempt,
            traceparent=ex.env.traceparent,
        )

    async def _module_heartbeat_loop(self) -> None:
        while True:
            await asyncio.sleep(self.settings.module_heartbeat_s)
            await self._publish_best_effort(
                "module.heartbeat",
                wire.ModuleHeartbeat(
                    module=self.module.name,
                    replica_id=self.identity.replica_id,
                    in_flight=self.in_flight,
                ),
            )

    def _online_payload(self) -> wire.ModuleOnline:
        spec = self.module.step_spec
        return wire.ModuleOnline(
            module=self.module.name,
            replica_id=self.identity.replica_id,
            contract_version=CONTRACT_VERSION,
            sdk=wire.SdkInfo(version=__version__),
            version=self.module.version,
            concurrency=self.concurrency,
            kinds=self.module.kinds,
            input_schema=spec.input_model.model_json_schema() if spec else None,
            output_schema=spec.output_model.model_json_schema() if spec else None,
        )

    # --- sources ----------------------------------------------------------------------------

    async def _on_trigger(self, env: wire.Envelope, delivery: Delivery) -> None:
        trigger = wire.SourceTrigger.model_validate(env.payload)
        source = next((s for s in self.module.sources if s.pipeline == trigger.pipeline), None)
        if source is None:
            self.log.error(
                "dead-lettering trigger for an unknown pipeline", pipeline=trigger.pipeline
            )
            await self._settle(delivery, "reject")
            return
        if not self._accepting:
            await self._settle(delivery, "requeue")
            return
        self._spawn_source(
            source,
            trigger.trigger_id,
            "trigger",
            trigger.data or {},
            delivery=delivery,
            send_trigger_id=True,
        )

    def _spawn_source(
        self,
        source: SourceSpec[S],
        trigger_id: str,
        kind: str,
        data: dict[str, Any],
        *,
        delivery: Delivery | None = None,
        send_trigger_id: bool,
    ) -> None:
        task = asyncio.create_task(
            self._run_source(
                source, trigger_id, kind, data, delivery, send_trigger_id=send_trigger_id
            )
        )
        self._source_runs.add(task)
        task.add_done_callback(self._source_runs.discard)

    async def _run_source(
        self,
        source: SourceSpec[S],
        trigger_id: str,
        kind: str,
        data: dict[str, Any],
        delivery: Delivery | None,
        *,
        send_trigger_id: bool,
    ) -> None:
        log = self.log.bind(source=source.name, pipeline=source.pipeline, trigger_id=trigger_id)
        ctx: SourceContext[S] = SourceContext(
            module=self.module.name,
            pipeline=source.pipeline,
            trigger_id=trigger_id,
            trigger_kind=kind,
            data=data,
            settings=self.module.settings,
            secrets=self.secrets,
            log=log,
        )
        count = 0
        try:
            async for item in source.handler(ctx):
                if not isinstance(item, RunRequest):
                    raise TypeError(f"source yielded {type(item).__name__}, expected RunRequest")
                await self._request_run(
                    source, item, f"{trigger_id}:{count}", trigger_id if send_trigger_id else None
                )
                count += 1
        except asyncio.CancelledError:
            log.warning("source interrupted", requested=count)
            if delivery is not None:
                await self._settle(delivery, "requeue")
            raise
        except Exception:
            log.exception("source failed", requested=count)
            if delivery is not None:
                await self._settle(delivery, "reject")
            return
        log.info("source finished", requested=count)
        if delivery is not None:
            await self._settle(delivery, "ack")

    async def _request_run(
        self, source: SourceSpec[S], item: RunRequest, default_key: str, trigger_id: str | None
    ) -> None:
        pipeline = item.pipeline or source.pipeline
        dedup_key = item.dedup_key or default_key
        payload = wire.RunRequested(
            pipeline=pipeline, data=item.data, dedup_key=dedup_key, trigger_id=trigger_id
        )
        mid, body = wire.encode(
            type_="run.requested",
            producer=self.producer,
            payload=payload,
            msg_id=wire.run_request_message_id(pipeline, dedup_key),
        )
        if len(body) > wire.MAX_MESSAGE_BYTES:
            raise ValueError(f"run.requested is {len(body)} bytes; limit {wire.MAX_MESSAGE_BYTES}")
        if not await self._publish_with_retry("run.requested", mid, body):
            raise ConnectionError("could not publish run.requested")

    async def _every_loop(self, source: SourceSpec[S], every_s: float) -> None:
        while True:
            await asyncio.sleep(every_s)
            if self._accepting:
                self._spawn_source(source, str(uuid.uuid4()), "every", {}, send_trigger_id=False)

    # --- publishing helpers -----------------------------------------------------------------

    def _outcome(
        self,
        env: wire.Envelope,
        attempt: wire.AttemptRef,
        payload: wire.StepSucceeded | wire.StepFailed,
        duration_ms: int,
    ) -> Outcome:
        if isinstance(payload, wire.StepFailed):
            payload = payload.model_copy(
                update={"replica_id": self.identity.replica_id, "duration_ms": duration_ms}
            )
            type_: ResultType = "step.failed"
        else:
            type_ = "step.succeeded"
        mid = wire.result_message_id(attempt.id, type_)
        _, body = wire.encode(
            type_=type_,
            producer=self.producer,
            payload=payload,
            msg_id=mid,
            run_id=env.run_id,
            attempt=attempt,
            traceparent=env.traceparent,
        )
        return Outcome(type_, mid, body)

    def _remember(self, attempt_id: str, outcome: Outcome | None) -> None:
        self._completed[attempt_id] = outcome
        self._completed.move_to_end(attempt_id)
        while len(self._completed) > _COMPLETED_CAPACITY:
            self._completed.popitem(last=False)

    async def _publish_and_settle(
        self, outcome: Outcome | None, deliveries: list[Delivery]
    ) -> None:
        if outcome is not None and not await self._publish_with_retry(
            outcome.type, outcome.message_id, outcome.body
        ):
            # Keep the command in the queue; a redelivery re-publishes the cached outcome.
            for d in deliveries:
                await self._settle(d, "requeue")
            return
        for d in deliveries:
            await self._settle(d, "ack")

    async def _publish_best_effort(
        self,
        type_: wire.MessageType,
        payload: BaseModel,
        *,
        run_id: str | None = None,
        attempt: wire.AttemptRef | None = None,
        traceparent: str | None = None,
    ) -> None:
        mid, body = wire.encode(
            type_=type_,
            producer=self.producer,
            payload=payload,
            run_id=run_id,
            attempt=attempt,
            traceparent=traceparent,
        )
        try:
            await self.transport.publish(body, message_id=str(mid), type_=type_)
        except Exception as exc:  # noqa: BLE001 (heartbeats and announcements are best effort)
            self.log.warning("publish failed", type=type_, error=type(exc).__name__)

    async def _publish_with_retry(
        self, type_: wire.MessageType, mid: uuid.UUID, body: bytes
    ) -> bool:
        deadline = time.monotonic() + self.settings.publish_timeout_s
        delay = 0.2
        while True:
            try:
                await self.transport.publish(body, message_id=str(mid), type_=type_)
            except Exception as exc:  # noqa: BLE001 (retried with backoff)
                if time.monotonic() + delay > deadline:
                    self.log.error("giving up publishing", type=type_, error=type(exc).__name__)
                    return False
                self.log.warning("publish failed; retrying", type=type_, error=type(exc).__name__)
                await asyncio.sleep(delay)
                delay = min(delay * 2, 10.0)
            else:
                return True

    async def _settle(self, delivery: Delivery, how: Literal["ack", "requeue", "reject"]) -> None:
        try:
            if how == "ack":
                await delivery.ack()
            else:
                await delivery.reject(requeue=how == "requeue")
        except Exception as exc:  # noqa: BLE001 (channel gone: the broker redelivers)
            self.log.warning("could not settle delivery", how=how, error=type(exc).__name__)

    def _spawn(self, coro: Coroutine[Any, Any, None]) -> None:
        task = asyncio.create_task(coro)
        self._background.add(task)
        task.add_done_callback(self._background.discard)


def _summarize(exc: ValidationError) -> str:
    parts = [
        f"{'.'.join(str(p) for p in err['loc']) or '<root>'}: {err['msg']}"
        for err in exc.errors()[:5]
    ]
    more = exc.error_count() - len(parts)
    return "; ".join(parts) + (f"; and {more} more" if more > 0 else "")

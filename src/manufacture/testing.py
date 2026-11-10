"""Test helpers: an in-memory transport and builders of core messages.

Example::

    async def test_hello() -> None:
        result = await call_step(app, {"text": ""})
        assert result.succeeded and result.payload["output"] == {"text": "hello"}
"""

from __future__ import annotations

import asyncio
import json
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, Final, Literal

from pydantic import BaseModel

from manufacture import _wire as wire
from manufacture.contract import validate_envelope
from manufacture.settings import RuntimeSettings
from manufacture.transport import DeliveryHandler, Identity, Inbound

if TYPE_CHECKING:
    from manufacture.app import Module

__all__ = [
    "ATTEMPT_ID",
    "RUN_ID",
    "ContractViolation",
    "FakeDelivery",
    "FakeTransport",
    "Published",
    "StepCallResult",
    "call_step",
    "source_trigger",
    "step_cancel",
    "step_request",
]

Settlement = Literal["pending", "acked", "requeued", "rejected"]


@dataclass
class FakeDelivery:
    body: bytes
    redelivered: bool = False
    state: Settlement = "pending"
    settled: asyncio.Event = field(default_factory=asyncio.Event)

    async def ack(self) -> None:
        self._settle("acked")

    async def reject(self, *, requeue: bool) -> None:
        self._settle("requeued" if requeue else "rejected")

    def _settle(self, state: Settlement) -> None:
        if self.state != "pending":
            raise RuntimeError(f"delivery already {self.state}")
        self.state = state
        self.settled.set()

    async def wait(self, timeout: float = 5.0) -> Settlement:
        await asyncio.wait_for(self.settled.wait(), timeout)
        return self.state


@dataclass(frozen=True)
class Published:
    routing_key: str
    message_id: str
    type: str
    body: bytes

    @property
    def envelope(self) -> wire.Envelope:
        return wire.Envelope.model_validate_json(self.body)

    @property
    def payload(self) -> dict[str, Any]:
        return self.envelope.payload


class ContractViolation(AssertionError):
    """The runtime published a message that the core contract rejects."""


class FakeTransport:
    """In-memory :class:`~manufacture.transport.Transport` for unit tests.

    Every published message is checked against the vendored envelope schema, so any test
    using it also proves the SDK speaks the contract.
    """

    def __init__(self) -> None:
        self.identity: Identity | None = None
        self.handlers: dict[Inbound, DeliveryHandler] = {}
        self.prefetch: dict[Inbound, int] = {}
        self.stopped: set[Inbound] = set()
        self.published: list[Published] = []
        self.fail_next_publishes = 0
        self.closed = False
        self.connected = False
        self._changed = asyncio.Condition()

    @property
    def ready(self) -> bool:
        return self.connected and not self.closed

    async def connect(self, identity: Identity) -> None:
        self.identity = identity
        self.connected = True

    async def consume(self, stream: Inbound, handler: DeliveryHandler, *, prefetch: int) -> None:
        self.handlers[stream] = handler
        self.prefetch[stream] = prefetch

    async def stop_consuming(self, stream: Inbound) -> None:
        self.stopped.add(stream)

    async def publish(self, body: bytes, *, message_id: str, type_: str) -> None:
        doc = json.loads(body)
        if errors := validate_envelope(doc):
            raise ContractViolation(f"{type_}: " + "; ".join(str(e) for e in errors))
        if (
            doc["message_id"] != message_id
            or doc["type"] != type_
            or len(body) > wire.MAX_MESSAGE_BYTES
        ):
            raise ContractViolation("message properties do not match the envelope")
        if self.fail_next_publishes > 0:
            self.fail_next_publishes -= 1
            raise ConnectionError("fake broker is down")
        async with self._changed:
            self.published.append(Published(type_, message_id, type_, body))
            self._changed.notify_all()

    async def close(self) -> None:
        self.closed = True

    # --- test API ---------------------------------------------------------------------------

    async def deliver(
        self, body: bytes, *, stream: Inbound = Inbound.WORK, redelivered: bool = False
    ) -> FakeDelivery:
        """Hand a message to the runtime like the broker would."""
        if stream in self.stopped:
            raise RuntimeError(f"{stream} is not being consumed")
        delivery = FakeDelivery(body, redelivered)
        await self.handlers[stream](delivery)
        return delivery

    def of_type(self, type_: str) -> list[Published]:
        return [p for p in self.published if p.type == type_]

    async def wait_for(self, type_: str, count: int = 1, timeout: float = 5.0) -> list[Published]:
        """Wait until ``count`` messages of ``type_`` have been published."""
        async with asyncio.timeout(timeout), self._changed:
            await self._changed.wait_for(lambda: len(self.of_type(type_)) >= count)
        return self.of_type(type_)

    async def wait_for_result(self, timeout: float = 5.0) -> Published:
        async with asyncio.timeout(timeout), self._changed:
            await self._changed.wait_for(
                lambda: any(p.type in {"step.succeeded", "step.failed"} for p in self.published)
            )
        return next(p for p in self.published if p.type in {"step.succeeded", "step.failed"})


RUN_ID: Final = "11111111-1111-4111-8111-111111111111"
ATTEMPT_ID: Final = "22222222-2222-4222-8222-222222222222"


def _envelope(
    type_: wire.MessageType,
    payload: dict[str, Any],
    *,
    run_id: str | None = None,
    attempt: wire.AttemptRef | None = None,
    traceparent: str | None = None,
) -> bytes:
    env = wire.Envelope(
        message_id=uuid.uuid4(),
        type=type_,
        schema_version=2,
        occurred_at=datetime.now(UTC),
        producer="core",
        traceparent=traceparent,
        run_id=run_id,
        attempt=attempt,
        payload=payload,
    )
    body = env.model_dump_json(exclude_none=True).encode()
    if errors := validate_envelope(json.loads(body)):
        raise ContractViolation("; ".join(str(e) for e in errors))
    return body


def step_request(
    input: dict[str, Any] | BaseModel | None = None,
    *,
    run_id: str = RUN_ID,
    attempt_id: str = ATTEMPT_ID,
    attempt: int = 1,
    pipeline: str = "pipeline",
    revision: int = 1,
    stage: str = "stage",
    step: str = "step",
    item_index: int | None = None,
    config: dict[str, Any] | None = None,
    lease_seconds: int = 30,
    deadline: datetime | None = None,
    timeout_s: float = 60.0,
    traceparent: str | None = None,
) -> bytes:
    """Body of a ``step.requested`` envelope as the core would send it."""
    data = input.model_dump(mode="json") if isinstance(input, BaseModel) else input
    payload: dict[str, Any] = {
        "pipeline": pipeline,
        "revision": revision,
        "stage": stage,
        "step": step,
        "input": data or {},
        "config": config or {},
        "deadline": (deadline or datetime.now(UTC) + timedelta(seconds=timeout_s)).isoformat(),
        "lease_seconds": lease_seconds,
        "blob_prefix": f"s3://mf-blobs/runs/{run_id}/{step}/{attempt_id}/",
    }
    if item_index is not None:
        payload["item_index"] = item_index
    ref = wire.AttemptRef(id=attempt_id, number=attempt)
    return _envelope("step.requested", payload, run_id=run_id, attempt=ref, traceparent=traceparent)


def step_cancel(
    attempt_id: str = ATTEMPT_ID,
    reason: wire.CancelReason = "cancelled",
    *,
    run_id: str = RUN_ID,
    attempt: int = 1,
) -> bytes:
    ref = wire.AttemptRef(id=attempt_id, number=attempt)
    return _envelope("step.cancel", {"reason": reason}, run_id=run_id, attempt=ref)


def source_trigger(
    pipeline: str, *, trigger_id: str | None = None, data: dict[str, Any] | None = None
) -> bytes:
    payload: dict[str, Any] = {"pipeline": pipeline, "trigger_id": trigger_id or str(uuid.uuid4())}
    if data is not None:
        payload["data"] = data
    return _envelope("source.trigger", payload)


@dataclass(frozen=True)
class StepCallResult:
    type: str
    payload: dict[str, Any]
    heartbeats: list[dict[str, Any]]

    @property
    def succeeded(self) -> bool:
        return self.type == "step.succeeded"

    @property
    def output(self) -> dict[str, Any]:
        if not self.succeeded:
            raise AssertionError(f"step failed: {self.payload}")
        return dict(self.payload["output"])


async def call_step(
    module: Module[Any],
    input: dict[str, Any] | BaseModel,
    *,
    config: dict[str, Any] | None = None,
    timeout_s: float = 60.0,
) -> StepCallResult:
    """Run the module's step handler once, without a broker, and return its outcome."""
    from manufacture.runtime import Runtime  # noqa: PLC0415 (import cycle)

    transport = FakeTransport()
    settings = RuntimeSettings(health_addr="", replica_id="test", module_heartbeat_s=3600)
    runtime = Runtime(module, settings=settings, transport=transport)
    await runtime.start()
    try:
        await transport.deliver(step_request(input, config=config, timeout_s=timeout_s))
        result = await transport.wait_for_result(timeout=timeout_s + 5)
    finally:
        await runtime.stop()
    beats = [p.payload for p in transport.of_type("step.heartbeat")]
    return StepCallResult(result.type, result.payload, beats)

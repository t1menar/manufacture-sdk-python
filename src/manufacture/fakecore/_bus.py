"""Brokers the fake core talks through.

:class:`MemoryBus` is a small in-process model of the RabbitMQ topology of
messaging.md §2: one shared work queue per module with competing consumers, prefetch,
ack/requeue, ``redelivered`` and the quorum ``x-delivery-limit`` (then dead letters),
and a control fanout to every replica. Replicas get a :class:`MemoryTransport`, which
implements the same :class:`~manufacture.transport.Transport` protocol as the AMQP one.

:class:`AmqpBus` (in ``_amqp``) does the same over a real broker.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from collections import deque
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Protocol

from manufacture import _wire as wire
from manufacture.contract import validate_envelope

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from manufacture.transport import DeliveryHandler, Identity, Inbound, ReconnectHandler

__all__ = ["Bus", "DeadLetter", "MemoryBus", "MemoryTransport"]


@dataclass(frozen=True, slots=True)
class DeadLetter:
    queue: str
    body: bytes
    reason: str
    """``delivery_limit`` or ``rejected`` (RabbitMQ ``x-first-death-reason``)."""


class Bus(Protocol):
    """What the engine needs from a broker."""

    async def start(self, modules: list[str], on_event: Callable[[bytes], Awaitable[None]]) -> None:
        """Declare the topology for ``modules`` and deliver module events to ``on_event``."""
        ...

    async def send_work(self, module: str, body: bytes, *, message_id: str, type_: str) -> None: ...

    async def send_control(
        self, module: str, body: bytes, *, message_id: str, type_: str
    ) -> None: ...

    def transport(self) -> MemoryTransport | None:
        """A transport for an in-process replica, or ``None`` to use the AMQP default."""
        ...

    @property
    def prefix(self) -> str:
        """Name prefix of every exchange and queue (isolates test runs on one broker)."""
        ...

    @property
    def dead_letters(self) -> list[DeadLetter]: ...

    @property
    def violations(self) -> list[str]:
        """Module messages that failed the envelope schema; the engine raises on them."""
        ...

    async def delete_queues(self) -> None:
        """Remove what :meth:`start` declared (only for buses the fake core created)."""
        ...

    async def close(self) -> None: ...


@dataclass(eq=False)
class _Message:
    body: bytes
    deliveries: int = 0


@dataclass(eq=False)
class _MemoryDelivery:
    queue: _WorkQueue
    consumer: _Consumer
    message: _Message
    redelivered: bool
    settled: bool = False

    @property
    def body(self) -> bytes:
        return self.message.body

    async def ack(self) -> None:
        self._settle()

    async def reject(self, *, requeue: bool) -> None:
        self._settle()
        if requeue:
            self.queue.requeue(self.message)
        else:
            self.queue.bus.dead(self.queue.name, self.message, "rejected")

    def _settle(self) -> None:
        if self.settled:
            raise RuntimeError("delivery already settled")
        self.settled = True
        self.consumer.unacked.discard(self)
        self.queue.pump()


@dataclass(eq=False)
class _Consumer:
    handler: DeliveryHandler
    prefetch: int
    active: bool = True
    unacked: set[_MemoryDelivery] = field(default_factory=set)


class _WorkQueue:
    def __init__(self, bus: MemoryBus, name: str) -> None:
        self.bus = bus
        self.name = name
        self.ready: deque[_Message] = deque()
        self.consumers: list[_Consumer] = []
        self._next = 0

    def publish(self, body: bytes) -> None:
        self.ready.append(_Message(body))
        self.pump()

    def requeue(self, message: _Message) -> None:
        # Quorum queues count every return; past the limit the message is dead-lettered.
        if message.deliveries > self.bus.topology.delivery_limit:
            self.bus.dead(self.name, message, "delivery_limit")
        else:
            self.ready.appendleft(message)
        self.pump()

    def pump(self) -> None:
        while self.ready:
            consumer = self._pick()
            if consumer is None:
                return
            message = self.ready.popleft()
            delivery = _MemoryDelivery(self, consumer, message, redelivered=message.deliveries > 0)
            message.deliveries += 1
            consumer.unacked.add(delivery)
            self.bus.spawn(consumer.handler(delivery))

    def _pick(self) -> _Consumer | None:
        n = len(self.consumers)
        for i in range(n):
            c = self.consumers[(self._next + i) % n]
            if c.active and len(c.unacked) < c.prefetch:
                self._next = (self._next + i + 1) % n
                return c
        return None

    def drop(self, consumer: _Consumer) -> None:
        """The consumer's channel closed: its unacked messages go back, marked redelivered."""
        with contextlib.suppress(ValueError):
            self.consumers.remove(consumer)
        for d in sorted(consumer.unacked, key=id):
            d.settled = True
            self.requeue(d.message)
        consumer.unacked.clear()


class MemoryTransport:
    """In-process :class:`~manufacture.transport.Transport` of one replica."""

    def __init__(self, bus: MemoryBus) -> None:
        self._bus = bus
        self._identity: Identity | None = None
        self._consumers: dict[str, _Consumer] = {}
        self._reconnect: list[ReconnectHandler] = []
        self._connected = False

    @property
    def identity(self) -> Identity | None:
        return self._identity

    @property
    def ready(self) -> bool:
        return self._connected

    def on_reconnect(self, handler: ReconnectHandler) -> None:
        self._reconnect.append(handler)

    async def connect(self, identity: Identity) -> None:
        self._identity = identity
        self._connected = True
        self._bus.transports.add(self)

    async def consume(self, stream: Inbound, handler: DeliveryHandler, *, prefetch: int) -> None:
        if self._identity is None:
            raise RuntimeError("connect() first")
        consumer = _Consumer(handler, prefetch if stream == "work" else 1_000_000)
        self._consumers[str(stream)] = consumer
        if stream == "work":
            queue = self._bus.work_queue(self._identity.module)
            queue.consumers.append(consumer)
            queue.pump()

    async def stop_consuming(self, stream: Inbound) -> None:
        consumer = self._consumers.get(str(stream))
        if consumer is not None:
            consumer.active = False

    async def publish(self, body: bytes, *, message_id: str, type_: str) -> None:
        if not self._connected:
            raise ConnectionError("transport closed")
        del message_id, type_
        await self._bus.deliver_event(body)

    async def close(self) -> None:
        if not self._connected:
            return
        self._connected = False
        self._bus.transports.discard(self)
        work = self._consumers.pop("work", None)
        if work is not None and self._identity is not None:
            self._bus.work_queue(self._identity.module).drop(work)
        self._consumers.clear()

    def deliver_control(self, body: bytes) -> None:
        consumer = self._consumers.get("control")
        if consumer is None or not consumer.active:
            return
        delivery = _ControlDelivery(body)
        self._bus.spawn(consumer.handler(delivery))


@dataclass(eq=False)
class _ControlDelivery:
    body: bytes
    redelivered: bool = False

    async def ack(self) -> None:
        return None

    async def reject(self, *, requeue: bool) -> None:
        del requeue


class MemoryBus:
    """The in-memory broker. Every message a module publishes is checked against the
    envelope schema before the engine sees it, so a protocol bug fails the test."""

    def __init__(self, *, prefix: str = "") -> None:
        self.topology = wire.Topology(prefix=prefix)
        self.transports: set[MemoryTransport] = set()
        self._queues: dict[str, _WorkQueue] = {}
        self._dead: list[DeadLetter] = []
        self._tasks: set[asyncio.Task[None]] = set()
        self._on_event: Callable[[bytes], Awaitable[None]] | None = None
        self._violations: list[str] = []

    @property
    def prefix(self) -> str:
        return self.topology.prefix

    @property
    def violations(self) -> list[str]:
        return self._violations

    async def delete_queues(self) -> None:
        self._queues.clear()

    def work_queue(self, module: str) -> _WorkQueue:
        name = self.topology.work_queue(module)
        queue = self._queues.get(name)
        if queue is None:
            queue = self._queues[name] = _WorkQueue(self, name)
        return queue

    def queue_depth(self, module: str) -> int:
        return len(self.work_queue(module).ready)

    def spawn(self, coro: Awaitable[None]) -> None:
        async def run() -> None:
            await coro

        task = asyncio.get_running_loop().create_task(run())
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    def dead(self, queue: str, message: _Message, reason: str) -> None:
        self._dead.append(DeadLetter(queue, message.body, reason))

    async def deliver_event(self, body: bytes) -> None:
        try:
            problems = [str(p) for p in validate_envelope(json.loads(body))]
        except ValueError as exc:
            problems = [f"not JSON: {exc}"]
        if problems:
            # Recorded, not raised: the replica would only log a publish error. The
            # engine turns violations into a test failure (FakeCore.check()).
            self._violations.append("; ".join(problems))
            return
        if self._on_event is not None:
            await self._on_event(body)

    # Bus protocol

    async def start(self, modules: list[str], on_event: Callable[[bytes], Awaitable[None]]) -> None:
        self._on_event = on_event
        for module in modules:
            self.work_queue(module)

    async def send_work(self, module: str, body: bytes, *, message_id: str, type_: str) -> None:
        del message_id, type_
        self.work_queue(module).publish(body)

    async def send_control(self, module: str, body: bytes, *, message_id: str, type_: str) -> None:
        del message_id, type_
        for t in list(self.transports):
            if t.identity is not None and t.identity.module == module:
                t.deliver_control(body)

    def transport(self) -> MemoryTransport:
        return MemoryTransport(self)

    @property
    def dead_letters(self) -> list[DeadLetter]:
        return list(self._dead)

    async def close(self) -> None:
        for t in list(self.transports):
            await t.close()
        for task in list(self._tasks):
            task.cancel()
        if self._tasks:
            await asyncio.wait(self._tasks, timeout=1)

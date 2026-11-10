"""The fake core over a real RabbitMQ: it declares what the core declares (exchanges,
work queues, the dead-letter queue and its own ``mf.core.events`` queue) and replicas
use the regular :class:`~manufacture.transport.amqp.AmqpTransport`."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import aio_pika

from manufacture import _wire as wire
from manufacture.contract import validate_envelope
from manufacture.fakecore._bus import DeadLetter

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from aio_pika.abc import (
        AbstractChannel,
        AbstractExchange,
        AbstractIncomingMessage,
        AbstractQueue,
        AbstractRobustConnection,
    )

    from manufacture.fakecore._bus import MemoryTransport

__all__ = ["AmqpBus"]


class AmqpBus:
    """Core side of the topology on a real broker. Use a ``prefix`` per test run so
    parallel runs on one broker do not share queues."""

    def __init__(self, url: str, *, prefix: str = "") -> None:
        self._url = url
        self.topology = wire.Topology(prefix=prefix)
        self._conn: AbstractRobustConnection | None = None
        self._channel: AbstractChannel | None = None
        self._commands: AbstractExchange | None = None
        self._control: AbstractExchange | None = None
        self._queues: list[AbstractQueue] = []
        self._consumers: list[tuple[AbstractQueue, str]] = []
        self._dead: list[DeadLetter] = []
        self._violations: list[str] = []

    @property
    def prefix(self) -> str:
        return self.topology.prefix

    @property
    def violations(self) -> list[str]:
        return self._violations

    @property
    def events_queue(self) -> str:
        return f"{self.topology.prefix}mf.core.events"

    @property
    def dead_letter_queue(self) -> str:
        return f"{self.topology.prefix}mf.dead-letters"

    async def start(self, modules: list[str], on_event: Callable[[bytes], Awaitable[None]]) -> None:
        t = self.topology
        self._conn = conn = await aio_pika.connect_robust(self._url)
        self._channel = ch = await conn.channel(publisher_confirms=True)
        await ch.set_qos(prefetch_count=256)
        self._commands = await ch.declare_exchange(
            t.commands, aio_pika.ExchangeType.DIRECT, durable=True
        )
        self._control = await ch.declare_exchange(
            t.control, aio_pika.ExchangeType.TOPIC, durable=True
        )
        events = await ch.declare_exchange(t.events, aio_pika.ExchangeType.TOPIC, durable=True)
        dlx = await ch.declare_exchange(t.dlx, aio_pika.ExchangeType.FANOUT, durable=True)
        dead = await ch.declare_queue(
            self.dead_letter_queue, durable=True, arguments={"x-queue-type": "quorum"}
        )
        await dead.bind(dlx)
        for module in modules:
            q = await ch.declare_queue(
                t.work_queue(module), durable=True, arguments=t.work_queue_arguments()
            )
            await q.bind(self._commands, routing_key=t.work_key(module))
            self._queues.append(q)
        inbox = await ch.declare_queue(
            self.events_queue, durable=True, arguments={"x-queue-type": "quorum"}
        )
        await inbox.bind(events, routing_key="#")
        self._queues += [dead, inbox]

        async def on_message(message: AbstractIncomingMessage) -> None:
            async with message.process(requeue=False):
                body = message.body
                try:
                    problems = [str(p) for p in validate_envelope(json.loads(body))]
                except ValueError as exc:
                    problems = [f"not JSON: {exc}"]
                if problems:
                    self._violations.append("; ".join(problems))
                    return
                await on_event(body)

        async def on_dead(message: AbstractIncomingMessage) -> None:
            async with message.process():
                headers = message.headers or {}
                reason = str(headers.get("x-first-death-reason", "unknown"))
                queue = str(headers.get("x-first-death-queue", ""))
                self._dead.append(DeadLetter(queue, message.body, reason))

        self._consumers = [
            (inbox, await inbox.consume(on_message)),
            (dead, await dead.consume(on_dead)),
        ]

    async def _publish(
        self,
        exchange: AbstractExchange | None,
        key: str,
        body: bytes,
        *,
        message_id: str,
        type_: str,
    ) -> None:
        if exchange is None:
            raise RuntimeError("bus not started")
        await exchange.publish(
            aio_pika.Message(
                body,
                message_id=message_id,
                type=type_,
                content_type="application/json",
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            ),
            routing_key=key,
            mandatory=False,
        )

    async def send_work(self, module: str, body: bytes, *, message_id: str, type_: str) -> None:
        await self._publish(
            self._commands, self.topology.work_key(module), body, message_id=message_id, type_=type_
        )

    async def send_control(self, module: str, body: bytes, *, message_id: str, type_: str) -> None:
        await self._publish(
            self._control,
            self.topology.control_key(module),
            body,
            message_id=message_id,
            type_=type_,
        )

    def transport(self) -> MemoryTransport | None:
        return None

    @property
    def dead_letters(self) -> list[DeadLetter]:
        return list(self._dead)

    async def delete_queues(self) -> None:
        """Remove what :meth:`start` declared under the prefix (test clean-up)."""
        for queue, tag in self._consumers:
            await queue.cancel(tag)
        self._consumers.clear()
        for q in self._queues:
            await q.delete(if_unused=False, if_empty=False)
        self._queues.clear()

    async def close(self) -> None:
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

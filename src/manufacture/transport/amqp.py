"""RabbitMQ transport on aio-pika: robust connection, publisher confirms, late acks.

Topology (names, queue arguments, who declares what) follows ``docs/contract/messaging.md``
§2 of contract 0.2.1 via :class:`manufacture._wire.Topology`; see ADR 0003.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import aio_pika
from pydantic import SecretStr
from yarl import URL

from manufacture.transport import DeliveryHandler, Identity, Inbound, ReconnectHandler

if TYPE_CHECKING:
    from aio_pika.abc import (
        AbstractChannel,
        AbstractExchange,
        AbstractIncomingMessage,
        AbstractQueue,
        AbstractRobustConnection,
    )

    from manufacture._wire import Topology

__all__ = ["AmqpTransport"]

_log = logging.getLogger("manufacture.transport")


class _AmqpDelivery:
    __slots__ = ("_message",)

    def __init__(self, message: AbstractIncomingMessage) -> None:
        self._message = message

    @property
    def body(self) -> bytes:
        return self._message.body

    @property
    def redelivered(self) -> bool:
        return bool(self._message.redelivered)

    async def ack(self) -> None:
        await self._message.ack()

    async def reject(self, *, requeue: bool) -> None:
        await self._message.reject(requeue=requeue)


@dataclass(slots=True)
class _Consumer:
    channel: AbstractChannel
    queue: AbstractQueue
    tag: str


class AmqpTransport:
    """:class:`~manufacture.transport.Transport` over RabbitMQ (messaging.md §2).

    The replica always (re)declares its work queue with the core's arguments and its own
    exclusive control queue. Exchanges belong to the core and are only checked passively,
    unless ``declare=True`` (tests, standalone use) declares them too.
    """

    def __init__(
        self,
        url: SecretStr | str,
        topology: Topology,
        *,
        declare: bool = False,
        timeout_s: float = 10,
    ) -> None:
        self._url = url if isinstance(url, SecretStr) else SecretStr(url)
        self._topology = topology
        self._declare = declare
        self._timeout_s = timeout_s
        self._identity: Identity | None = None
        self._conn: AbstractRobustConnection | None = None
        self._pub_channel: AbstractChannel | None = None
        self._events: AbstractExchange | None = None
        self._consumers: dict[Inbound, _Consumer] = {}
        self._user_id = URL(self._url.get_secret_value()).user or None
        self._reconnect_handlers: list[ReconnectHandler] = []
        self._reconnect_tasks: set[asyncio.Task[None]] = set()

    @property
    def ready(self) -> bool:
        conn, ch = self._conn, self._pub_channel
        return bool(
            conn and not conn.is_closed and conn.connected.is_set() and ch and not ch.is_closed
        )

    def on_reconnect(self, handler: ReconnectHandler) -> None:
        self._reconnect_handlers.append(handler)

    def _reconnected(self, *_: object) -> None:
        # aio-pika calls this after the robust channels re-declared their queues, bindings
        # and consumers. Run the handlers in tasks so a slow publish cannot stall recovery.
        for handler in self._reconnect_handlers:
            task = asyncio.create_task(handler())
            self._reconnect_tasks.add(task)
            task.add_done_callback(self._reconnect_done)

    def _reconnect_done(self, task: asyncio.Task[None]) -> None:
        self._reconnect_tasks.discard(task)
        if not task.cancelled() and (exc := task.exception()) is not None:
            _log.warning("reconnect handler failed: %s", type(exc).__name__)

    async def connect(self, identity: Identity) -> None:
        self._identity = identity
        self._conn = await aio_pika.connect_robust(
            self._url.get_secret_value(),
            timeout=self._timeout_s,
            client_properties={"connection_name": f"mf:{identity.module}:{identity.replica_id}"},
        )
        self._conn.reconnect_callbacks.add(self._reconnected)
        self._pub_channel = await self._conn.channel(publisher_confirms=True, on_return_raises=True)
        self._events = await self._exchange(
            self._pub_channel, self._topology.events, aio_pika.ExchangeType.TOPIC
        )

    async def consume(self, stream: Inbound, handler: DeliveryHandler, *, prefetch: int) -> None:
        if self._conn is None or self._identity is None:
            raise RuntimeError("connect() first")
        if stream in self._consumers:
            raise RuntimeError(f"already consuming {stream}")
        module, t = self._identity.module, self._topology
        channel = await self._conn.channel()
        await channel.set_qos(prefetch_count=prefetch)
        queue: AbstractQueue
        if stream is Inbound.CONTROL:
            control = await self._exchange(channel, t.control, aio_pika.ExchangeType.TOPIC)
            queue = await channel.declare_queue(
                t.control_queue(module, self._identity.replica_id), exclusive=True, auto_delete=True
            )
            await queue.bind(control, t.control_key(module))
        else:
            if self._declare:
                await channel.declare_exchange(t.dlx, aio_pika.ExchangeType.FANOUT, durable=True)
            commands = await self._exchange(channel, t.commands, aio_pika.ExchangeType.DIRECT)
            notify = stream is Inbound.NOTIFY
            queue = await channel.declare_queue(
                t.notify_queue(module) if notify else t.work_queue(module),
                durable=True,
                arguments=t.work_queue_arguments(),
            )
            await queue.bind(commands, t.notify_key(module) if notify else t.work_key(module))

        async def on_message(message: AbstractIncomingMessage) -> None:
            await handler(_AmqpDelivery(message))

        tag = await queue.consume(on_message, no_ack=False)
        self._consumers[stream] = _Consumer(channel, queue, tag)

    async def stop_consuming(self, stream: Inbound) -> None:
        consumer = self._consumers.get(stream)
        if consumer is not None and not consumer.channel.is_closed:
            await consumer.queue.cancel(consumer.tag)

    async def publish(self, body: bytes, *, message_id: str, type_: str) -> None:
        if self._events is None:
            raise RuntimeError("connect() first")
        message = aio_pika.Message(
            body,
            content_type="application/json",
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            message_id=message_id,
            type=type_,
            timestamp=datetime.now(UTC),
            user_id=self._user_id,
        )
        # mandatory: an unroutable message (core queue missing) is an error, not a silent drop.
        await self._events.publish(message, routing_key=type_, mandatory=True)

    async def close(self) -> None:
        for task in list(self._reconnect_tasks):
            task.cancel()
        for consumer in self._consumers.values():
            with contextlib.suppress(Exception):
                await consumer.channel.close()
        self._consumers.clear()
        if self._conn is not None:
            await self._conn.close()
            self._conn = None
        self._pub_channel = None
        self._events = None

    async def _exchange(
        self, channel: AbstractChannel, name: str, kind: aio_pika.ExchangeType
    ) -> AbstractExchange:
        if self._declare:
            return await channel.declare_exchange(name, kind, durable=True)
        return await channel.get_exchange(name, ensure=True)

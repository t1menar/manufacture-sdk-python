"""Fixtures for tests against a real RabbitMQ (``MF_TEST_AMQP_URL``).

Each test gets its own name prefix, so tests never see each other's exchanges or queues.
"""

from __future__ import annotations

import asyncio
import os
import uuid
from collections.abc import AsyncIterator
from typing import Any

import aio_pika
import pytest
from aio_pika.abc import AbstractExchange, AbstractIncomingMessage, AbstractRobustConnection

from manufacture import _wire as wire

AMQP_URL = os.environ.get("MF_TEST_AMQP_URL", "")


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    skip = pytest.mark.skip(reason="set MF_TEST_AMQP_URL to run integration tests")
    for item in items:
        if "integration" in item.keywords and not AMQP_URL:
            item.add_marker(skip)


class FakeCore:
    """The core side of the broker (messaging.md §2): owns the exchanges, sends commands,
    and collects every module event like ``mf.core.events`` does."""

    def __init__(self, conn: AbstractRobustConnection, topology: wire.Topology) -> None:
        self.conn = conn
        self.t = topology
        self.events: list[AbstractIncomingMessage] = []
        self.modules: list[str] = []
        self._changed = asyncio.Condition()

    async def setup(self) -> None:
        self.channel = await self.conn.channel()
        t, kind = self.t, aio_pika.ExchangeType
        self.commands: AbstractExchange = await self.channel.declare_exchange(
            t.commands, kind.DIRECT, durable=True
        )
        self.control: AbstractExchange = await self.channel.declare_exchange(
            t.control, kind.TOPIC, durable=True
        )
        events = await self.channel.declare_exchange(t.events, kind.TOPIC, durable=True)
        dlx = await self.channel.declare_exchange(t.dlx, kind.FANOUT, durable=True)
        self.dead_letters = t.prefix + "mf.dead-letters"
        dead = await self.channel.declare_queue(self.dead_letters, durable=True)
        await dead.bind(dlx)
        core_events = await self.channel.declare_queue(t.prefix + "mf.core.events", exclusive=True)
        await core_events.bind(events, "#")
        await core_events.consume(self._on_event, no_ack=True)

    async def _on_event(self, message: AbstractIncomingMessage) -> None:
        async with self._changed:
            self.events.append(message)
            self._changed.notify_all()

    def of_type(self, type_: str) -> list[AbstractIncomingMessage]:
        return [m for m in self.events if m.type == type_]

    async def wait_for(
        self, type_: str, count: int = 1, timeout: float = 10
    ) -> list[AbstractIncomingMessage]:
        async with asyncio.timeout(timeout), self._changed:
            await self._changed.wait_for(lambda: len(self.of_type(type_)) >= count)
        return self.of_type(type_)

    async def _publish(self, exchange: AbstractExchange, key: str, body: bytes) -> None:
        env = wire.decode(body)
        message = aio_pika.Message(
            body,
            message_id=str(env.message_id),
            type=env.type,
            content_type="application/json",
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        )
        await exchange.publish(message, routing_key=key)

    async def work(self, module: str, body: bytes) -> None:
        await self._publish(self.commands, self.t.work_key(module), body)

    async def cancel(self, module: str, body: bytes) -> None:
        await self._publish(self.control, self.t.control_key(module), body)

    async def queue_depth(self, name: str) -> int:
        ch = await self.conn.channel()
        try:
            q = await ch.declare_queue(name, passive=True)
            return int(q.declaration_result.message_count or 0)
        finally:
            await ch.close()

    async def cleanup(self) -> None:
        ch = await self.conn.channel()
        t = self.t
        for name in [t.work_queue(m) for m in self.modules] + [self.dead_letters]:
            await ch.queue_delete(name)
        for x in (t.commands, t.control, t.events, t.dlx):
            await ch.exchange_delete(x)
        await ch.close()


@pytest.fixture
def topology() -> wire.Topology:
    return wire.Topology(prefix=f"it-{uuid.uuid4().hex[:8]}.")


@pytest.fixture
async def core(topology: wire.Topology) -> AsyncIterator[FakeCore]:
    conn = await aio_pika.connect_robust(AMQP_URL)
    fake = FakeCore(conn, topology)
    await fake.setup()
    try:
        yield fake
    finally:
        await fake.cleanup()
        await conn.close()


def settings_kwargs(topology: wire.Topology, **overrides: Any) -> dict[str, Any]:
    return {
        "amqp_url": AMQP_URL,
        "amqp_prefix": topology.prefix,
        "health_addr": "",
        "min_heartbeat_s": 0.05,
        "module_heartbeat_s": 0.2,
        "shutdown_grace_s": 2,
        "cancel_grace_s": 0.2,
        "publish_timeout_s": 5,
        **overrides,
    }

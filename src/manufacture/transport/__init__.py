"""Broker abstraction used by the runtime; the AMQP implementation lives in ``amqp``."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol

__all__ = ["Delivery", "DeliveryHandler", "Identity", "Inbound", "Transport"]


class Inbound(StrEnum):
    """Inbound streams of a module replica (messaging.md §2)."""

    WORK = "work"
    """``mf.module.<name>``: ``step.requested`` and ``source.trigger``, shared by replicas."""
    CONTROL = "control"
    """``mf.control.<name>.<replica>``: ``step.cancel``, one queue per replica."""


@dataclass(frozen=True, slots=True)
class Identity:
    module: str
    replica_id: str


class Delivery(Protocol):
    """One inbound message. Exactly one of ``ack``/``reject`` should be called."""

    @property
    def body(self) -> bytes: ...

    @property
    def redelivered(self) -> bool: ...

    async def ack(self) -> None: ...

    async def reject(self, *, requeue: bool) -> None: ...


DeliveryHandler = Callable[[Delivery], Awaitable[None]]


class Transport(Protocol):
    """What the runtime needs from a broker."""

    @property
    def ready(self) -> bool:
        """Connected and able to publish."""
        ...

    async def connect(self, identity: Identity) -> None: ...

    async def consume(self, stream: Inbound, handler: DeliveryHandler, *, prefetch: int) -> None:
        """Start delivering ``stream`` to ``handler``. The handler must return quickly."""
        ...

    async def stop_consuming(self, stream: Inbound) -> None:
        """Stop new deliveries of ``stream``; unacked ones stay valid."""
        ...

    async def publish(self, body: bytes, *, message_id: str, type_: str) -> None:
        """Publish to the events exchange (routing key = type) and wait for the confirm."""
        ...

    async def close(self) -> None: ...

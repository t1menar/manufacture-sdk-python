from __future__ import annotations

from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import Any

import pytest
from pydantic import BaseModel

from manufacture import Module
from manufacture.runtime import Runtime
from manufacture.settings import RuntimeSettings
from manufacture.testing import FakeTransport


class Text(BaseModel):
    text: str


def fast_settings(**overrides: Any) -> RuntimeSettings:
    base: dict[str, Any] = {
        "health_addr": "",
        "replica_id": "r1",
        "min_heartbeat_s": 0.01,
        "cancel_grace_s": 0.1,
        "shutdown_grace_s": 0.2,
        "publish_timeout_s": 0.5,
        "module_heartbeat_s": 3600,
    }
    return RuntimeSettings(**{**base, **overrides})


RuntimeFactory = Callable[..., AbstractAsyncContextManager[tuple[Runtime[Any], FakeTransport]]]


@pytest.fixture
def running() -> RuntimeFactory:
    """``async with running(app, **settings) as (runtime, transport): ...``"""

    @asynccontextmanager
    async def factory(
        module: Module[Any], **overrides: Any
    ) -> AsyncIterator[tuple[Runtime[Any], FakeTransport]]:
        transport = FakeTransport()
        runtime: Runtime[Any] = Runtime(
            module, settings=fast_settings(**overrides), transport=transport
        )
        await runtime.start()
        try:
            yield runtime, transport
        finally:
            await runtime.stop()

    return factory

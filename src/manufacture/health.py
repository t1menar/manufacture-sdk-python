"""Minimal HTTP server for container probes: ``/healthz`` (alive) and ``/readyz`` (ready).

Kept dependency-free on purpose; ``/metrics`` joins it with the telemetry PR (S4).
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from typing import TYPE_CHECKING, Final

if TYPE_CHECKING:
    from collections.abc import Callable

__all__ = ["HealthServer"]

_MAX_REQUEST_LINE: Final = 4096
_READ_TIMEOUT_S: Final = 5.0
_REASONS: Final = {
    200: "OK",
    404: "Not Found",
    405: "Method Not Allowed",
    503: "Service Unavailable",
}


class HealthServer:
    def __init__(self, host: str, port: int, *, ready: Callable[[], bool]) -> None:
        self.host = host
        self.port = port
        self._ready = ready
        self._server: asyncio.Server | None = None

    @property
    def bound_port(self) -> int:
        """Actual port (useful with ``port=0`` in tests)."""
        if self._server is None or not self._server.sockets:
            raise RuntimeError("health server is not running")
        return int(self._server.sockets[0].getsockname()[1])

    async def start(self) -> None:
        self._server = await asyncio.start_server(self._handle, self.host, self.port)

    async def stop(self) -> None:
        if self._server is not None:
            self._server.close()
            with contextlib.suppress(Exception):
                await asyncio.wait_for(self._server.wait_closed(), timeout=2)
            self._server = None

    def route(self, method: str, path: str) -> tuple[int, dict[str, object]]:
        path = path.split("?", 1)[0]
        if path not in {"/healthz", "/readyz"}:
            return 404, {"error": "not found"}
        if method not in {"GET", "HEAD"}:
            return 405, {"error": "method not allowed"}
        if path == "/healthz":
            return 200, {"status": "ok"}
        ready = self._ready()
        return (200 if ready else 503), {"status": "ready" if ready else "not ready"}

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            line = await asyncio.wait_for(reader.readline(), timeout=_READ_TIMEOUT_S)
            parts = line[:_MAX_REQUEST_LINE].decode("latin-1").split()
            method, path = (parts[0], parts[1]) if len(parts) >= 2 else ("", "")
            status, doc = self.route(method, path)
            body = json.dumps(doc).encode()
            head = (
                f"HTTP/1.1 {status} {_REASONS[status]}\r\n"
                "Content-Type: application/json\r\n"
                f"Content-Length: {len(body)}\r\n"
                "Connection: close\r\n\r\n"
            ).encode()
            writer.write(head if method == "HEAD" else head + body)
            await writer.drain()
        except (TimeoutError, ConnectionError, UnicodeDecodeError):
            pass
        finally:
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()

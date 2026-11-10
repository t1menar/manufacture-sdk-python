from __future__ import annotations

import asyncio

import pytest

from manufacture.health import HealthServer


async def request(port: int, line: str) -> tuple[int, bytes]:
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    writer.write(f"{line}\r\nHost: x\r\n\r\n".encode())
    await writer.drain()
    data = await reader.read()
    writer.close()
    await writer.wait_closed()
    head, _, body = data.partition(b"\r\n\r\n")
    return int(head.split()[1]), body


@pytest.mark.parametrize(
    ("line", "ready", "status", "body"),
    [
        ("GET /healthz HTTP/1.1", False, 200, b'{"status": "ok"}'),
        ("GET /readyz HTTP/1.1", True, 200, b'{"status": "ready"}'),
        ("GET /readyz HTTP/1.1", False, 503, b'{"status": "not ready"}'),
        ("HEAD /readyz HTTP/1.1", True, 200, b""),
        ("GET /metrics HTTP/1.1", True, 404, b'{"error": "not found"}'),
        ("POST /healthz HTTP/1.1", True, 405, b'{"error": "method not allowed"}'),
        ("garbage", True, 404, b'{"error": "not found"}'),
    ],
)
async def test_routes(line: str, ready: bool, status: int, body: bytes) -> None:
    server = HealthServer("127.0.0.1", 0, ready=lambda: ready)
    await server.start()
    try:
        assert await request(server.bound_port, line) == (status, body)
    finally:
        await server.stop()


async def test_bound_port_requires_running_server() -> None:
    with pytest.raises(RuntimeError):
        _ = HealthServer("127.0.0.1", 0, ready=lambda: True).bound_port

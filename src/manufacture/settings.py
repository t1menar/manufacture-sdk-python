"""Runtime configuration of a module process, read from ``MF_*`` environment variables.

The variables are defined by the contract (messaging.md §1); the runtime driver of the
core sets them for every container. ``MF_AMQP_PREFIX``, ``MF_DECLARE_TOPOLOGY`` and the
``*_S`` timings are SDK-specific.
"""

from __future__ import annotations

import secrets
from typing import Annotated, Literal

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

__all__ = ["RuntimeSettings", "parse_health_addr"]


def parse_health_addr(addr: str) -> tuple[str, int] | None:
    """``":8081"`` -> ``("0.0.0.0", 8081)``; empty string or ``"off"`` disables the endpoint."""
    if addr in {"", "off"}:
        return None
    host, sep, port = addr.rpartition(":")
    if not sep or not port.isdigit() or not 0 <= int(port) <= 65535:
        raise ValueError(f"MF_HEALTH_ADDR must be [host]:port, got {addr!r}")
    return (host.strip("[]") or "0.0.0.0"), int(port)  # noqa: S104 (probes come from outside)


class RuntimeSettings(BaseSettings):
    """SDK settings; environment variables override defaults, arguments override both."""

    model_config = SettingsConfigDict(env_prefix="MF_", extra="ignore", frozen=True)

    module_name: str | None = None
    """``MF_MODULE_NAME``: must equal the name in code when set (catches wiring mistakes)."""

    replica_id: Annotated[str, Field(min_length=1, max_length=128)] | None = None
    """``MF_REPLICA_ID``; default ``<module>-<random>``."""

    amqp_url: SecretStr = SecretStr("amqp://guest:guest@localhost:5672/")
    """``MF_AMQP_URL`` with the module's own broker user."""

    concurrency: Annotated[int, Field(ge=1, le=1024)] | None = None
    """``MF_CONCURRENCY``: steps per replica (prefetch); default is the module's own value."""

    health_addr: str = ":8081"
    """``MF_HEALTH_ADDR`` for ``/healthz`` and ``/readyz``; empty disables."""

    log_level: str = "info"
    log_format: Literal["json", "text"] = "json"

    amqp_prefix: str = ""
    """Prefix of exchange and queue names; isolates test runs on a shared broker."""

    declare_topology: bool = False
    """Also declare the exchanges (normally the core does). For tests and standalone use."""

    shutdown_grace_s: Annotated[float, Field(ge=0)] = 25.0
    cancel_grace_s: Annotated[float, Field(ge=0)] = 10.0
    min_heartbeat_s: Annotated[float, Field(gt=0)] = 1.0
    module_heartbeat_s: Annotated[float, Field(gt=0)] = 10.0
    publish_timeout_s: Annotated[float, Field(gt=0)] = 300.0

    @field_validator("health_addr")
    @classmethod
    def _check_addr(cls, value: str) -> str:
        parse_health_addr(value)
        return value

    @property
    def health(self) -> tuple[str, int] | None:
        return parse_health_addr(self.health_addr)

    def replica_for(self, module: str) -> str:
        return self.replica_id or f"{module}-{secrets.token_hex(4)}"

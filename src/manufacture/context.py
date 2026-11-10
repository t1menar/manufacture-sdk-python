"""What a handler sees: :class:`Context` for steps, :class:`SourceContext` for sources."""

from __future__ import annotations

import asyncio
import os
from collections.abc import Awaitable, Callable, Mapping
from datetime import UTC, datetime
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from pydantic import BaseModel, SecretStr, ValidationError
from typing_extensions import TypeVar as TypeVarWithDefault

from manufacture.errors import Fatal

if TYPE_CHECKING:
    from manufacture._log import StructLogger

__all__ = ["Context", "NoSettings", "ProgressSink", "Secrets", "SourceContext"]

M = TypeVar("M", bound=BaseModel)


class NoSettings(BaseModel):
    """Settings type of modules that do not declare their own."""


S = TypeVarWithDefault("S", bound=BaseModel, default=NoSettings)

ProgressSink = Callable[[float | None, str | None], Awaitable[None]]


class Secrets:
    """Secrets of the module (from env, filled by the runtime from ``secretRef``).

    Values are :class:`pydantic.SecretStr`, so they never show up in ``repr`` or logs.
    """

    __slots__ = ("_env",)

    def __init__(self, env: Mapping[str, str] | None = None) -> None:
        self._env = env if env is not None else os.environ

    def get(self, name: str) -> SecretStr | None:
        value = self._env.get(name)
        return SecretStr(value) if value is not None else None

    def require(self, name: str) -> SecretStr:
        """Return the secret or fail the step as ``fatal`` (``secret_missing``)."""
        value = self.get(name)
        if value is None:
            raise Fatal(f"secret {name} is not set", code="secret_missing")
        return value


class Context(Generic[S]):
    """Per-attempt context of a step handler.

    Created by the runtime; handlers only read it and call :meth:`progress`.
    """

    def __init__(
        self,
        *,
        module: str,
        run_id: str,
        pipeline: str,
        revision: int,
        stage: str,
        step: str,
        attempt_id: str,
        attempt: int,
        deadline: datetime,
        config: Mapping[str, Any],
        settings: S,
        secrets: Secrets,
        log: StructLogger,
        progress_sink: ProgressSink,
        blob_prefix: str,
        item_index: int | None = None,
        traceparent: str | None = None,
    ) -> None:
        self.module = module
        self.run_id = run_id
        self.pipeline = pipeline
        self.revision = revision
        self.stage = stage
        self.step = step
        """Step id within the pipeline (``steps.<step>.output``)."""
        self.item_index = item_index
        """Item position in a ``foreach`` stage, else ``None``."""
        self.attempt_id = attempt_id
        self.attempt = attempt
        """Attempt number, starting at 1."""
        self.deadline = deadline
        self.config: Mapping[str, Any] = MappingProxyType(dict(config))
        self.settings: S = settings
        self.secrets = secrets
        self.log = log
        self.blob_prefix = blob_prefix
        """Where this attempt may write blobs (``s3://bucket/runs/<run>/<step>/<attempt>/``)."""
        self.traceparent = traceparent
        self.cancelled = asyncio.Event()
        """Set when the core cancels the attempt or the replica shuts down. Long handlers
        should check it between units of work; after a grace period the task is cancelled."""
        self._progress_sink = progress_sink

    @property
    def remaining_s(self) -> float:
        """Seconds until the attempt deadline (negative when it has passed)."""
        return (self.deadline - datetime.now(UTC)).total_seconds()

    async def progress(self, value: float | None = None, message: str | None = None) -> None:
        """Report progress (``0..1``) and/or a status message; also extends the lease.

        Best effort: a broker problem is logged, never raised into the handler.
        """
        if value is not None and not 0.0 <= value <= 1.0:
            raise ValueError("progress must be within 0..1")
        await self._progress_sink(value, message)

    def config_as(self, model: type[M]) -> M:
        """Validate the step ``config`` into ``model``; invalid config fails the step as fatal."""
        try:
            return model.model_validate(dict(self.config))
        except ValidationError as exc:
            raise Fatal(
                f"invalid step config: {exc.error_count()} error(s)", code="config_invalid"
            ) from exc


class SourceContext(Generic[S]):
    """Context of a source handler invocation (one trigger)."""

    def __init__(
        self,
        *,
        module: str,
        pipeline: str,
        trigger_id: str,
        trigger_kind: str,
        data: Mapping[str, Any],
        settings: S,
        secrets: Secrets,
        log: StructLogger,
    ) -> None:
        self.module = module
        self.pipeline = pipeline
        self.trigger_id = trigger_id
        """Unique id of this trigger; a good default ``dedup_key`` for one-run triggers."""
        self.trigger_kind = trigger_kind
        """``start`` / ``every`` (by the replica) or ``trigger`` (``source.trigger`` from the core)."""
        self.data: Mapping[str, Any] = MappingProxyType(dict(data))
        """``data`` of ``source.trigger`` (e.g. from the UI "Run" form); empty otherwise."""
        self.settings: S = settings
        self.secrets = secrets
        self.log = log

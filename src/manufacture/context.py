"""What a handler sees: :class:`Context` for steps, :class:`SourceContext` for sources."""

from __future__ import annotations

import asyncio
import os
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Generic, NoReturn, TypeVar

from pydantic import BaseModel, SecretStr, ValidationError
from typing_extensions import TypeVar as TypeVarWithDefault

from manufacture.errors import ErrorCode, Fatal, StepWaiting

if TYPE_CHECKING:
    from manufacture._log import StructLogger

__all__ = [
    "AttemptHandle",
    "Budget",
    "ConfirmSink",
    "Context",
    "NoSettings",
    "ProgressSink",
    "Secrets",
    "SourceContext",
    "WaitSink",
]

M = TypeVar("M", bound=BaseModel)


class NoSettings(BaseModel):
    """Settings type of modules that do not declare their own."""


S = TypeVarWithDefault("S", bound=BaseModel, default=NoSettings)

ProgressSink = Callable[[float | None, str | None], Awaitable[None]]
WaitSink = Callable[[str, str | None, datetime | None], Awaitable[None]]
"""``(reason, message, expected_by)``: publishes ``step.waiting`` and returns once the broker confirmed."""
ConfirmSink = Callable[[str, int], Awaitable[None]]
"""``(action, extend_seconds)``: raises ``Superseded`` / ``Cancelled`` / ``DeadlineTooClose``."""


@dataclass(frozen=True, slots=True)
class Budget:
    """Run budget at the moment the step was dispatched (``payload.budget``)."""

    run_usd: float
    spent_usd: float
    remaining_usd: float


@dataclass(frozen=True, slots=True)
class AttemptHandle:
    """Enough to finish an attempt from any replica after ``ctx.wait``: store ``str(handle)``."""

    run_id: str
    attempt_id: str
    number: int

    def __str__(self) -> str:
        return f"{self.run_id}:{self.attempt_id}:{self.number}"

    @classmethod
    def parse(cls, value: str | AttemptHandle) -> AttemptHandle:
        if isinstance(value, AttemptHandle):
            return value
        try:
            run_id, attempt_id, number = value.rsplit(":", 2)
            return cls(run_id, attempt_id, int(number))
        except ValueError as exc:
            raise ValueError(f"not an attempt handle: {value!r}") from exc


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
        step_key: str = "",
        visit: int = 1,
        budget: Budget | None = None,
        labels: Mapping[str, str] | None = None,
        priority: int | None = None,
        wait_sink: WaitSink | None = None,
        confirm_sink: ConfirmSink | None = None,
    ) -> None:
        self.module = module
        self.step_key = step_key
        """Id of this visit of the step, the same for every attempt: the idempotency key of
        external side effects and of module session state (uuid5, messaging.md §4.5)."""
        self.visit = visit
        """Visit number of the stage within the run (1 on the first pass, +1 per ``goto``)."""
        self.budget = budget
        """Run budget, or ``None`` when the pipeline has none."""
        self.labels: Mapping[str, str] = MappingProxyType(dict(labels or {}))
        self.priority = priority
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
        self._wait_sink = wait_sink
        self._confirm_sink = confirm_sink
        self._cost_usd = 0.0
        self._tokens: dict[str, dict[str, int]] = {}

    @property
    def attempt_handle(self) -> AttemptHandle:
        """Store it before ``wait``; ``Module.complete(handle, output)`` finishes the attempt."""
        return AttemptHandle(self.run_id, self.attempt_id, self.attempt)

    def add_usage(
        self,
        cost_usd: float = 0.0,
        *,
        model: str | None = None,
        input_tokens: int = 0,
        output_tokens: int = 0,
        cache_read: int = 0,
        cache_write: int = 0,
    ) -> None:
        """Add spend to the attempt total (priced by the module). Sent with heartbeats and the result."""
        if cost_usd < 0:
            raise ValueError("cost_usd must be >= 0")
        self._cost_usd += cost_usd
        if model is not None:
            slot = self._tokens.setdefault(model, {})
            for key, value in (
                ("input", input_tokens),
                ("output", output_tokens),
                ("cache_read", cache_read),
                ("cache_write", cache_write),
            ):
                if value:
                    slot[key] = slot.get(key, 0) + value

    @property
    def usage(self) -> dict[str, Any]:
        """Cumulative usage of this attempt: ``{"cost_usd", "tokens"}`` (empty when nothing was added)."""
        doc: dict[str, Any] = {}
        if self._cost_usd:
            doc["cost_usd"] = round(self._cost_usd, 6)
        if self._tokens:
            doc["tokens"] = {m: dict(t) for m, t in self._tokens.items()}
        return doc

    async def wait(
        self,
        *,
        reason: str = "human",
        message: str | None = None,
        expected_by: datetime | None = None,
    ) -> NoReturn:
        """Park the attempt (``step.waiting``) and stop the handler; it never returns.

        The lease and the execution timeout pause; the core's ``wait.timeout`` runs. Finish the
        attempt later, from any replica, with ``Module.complete(handle, output)`` or
        ``Module.fail(handle, error)``; keep ``ctx.attempt_handle`` (and ``ctx.step_key``)
        in your own storage before calling this.
        """
        if reason not in {"human", "external"}:
            raise ValueError("reason must be 'human' or 'external'")
        if self._wait_sink is None:
            raise RuntimeError("this context cannot wait")
        await self._wait_sink(reason, message, expected_by)
        raise StepWaiting

    async def confirm(self, *, action: str, extend_seconds: int = 120) -> None:
        """Ask the core for permission right before an irreversible action.

        Raises :class:`~manufacture.errors.Superseded`, :class:`~manufacture.errors.Cancelled` or
        :class:`~manufacture.errors.DeadlineTooClose`; then the action must not be performed.
        After success the core protects the attempt: without a result it needs attention
        (``outcome_unknown``) instead of being retried.
        """
        if not 1 <= extend_seconds <= 600:
            raise ValueError("extend_seconds must be within 1..600")
        if self._confirm_sink is None:
            raise RuntimeError("this context cannot confirm")
        await self._confirm_sink(action, extend_seconds)

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
                f"invalid step config: {exc.error_count()} error(s)", code=ErrorCode.CONFIG_INVALID
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

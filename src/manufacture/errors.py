"""Errors a step handler raises to tell the core how to treat a failure (§3.4)."""

from __future__ import annotations

from enum import StrEnum
from typing import ClassVar

from manufacture._wire import ErrorClass

__all__ = [
    "Cancelled",
    "ConfirmError",
    "DeadlineTooClose",
    "ErrorClass",
    "ErrorCode",
    "Fatal",
    "NeedsAttention",
    "StepError",
    "StepWaiting",
    "Superseded",
    "Transient",
]


class ErrorCode(StrEnum):
    """``step.failed.code`` values the SDK itself reports.

    Handlers may use any other ``snake_case`` code. ``MODULE_SHUTDOWN`` is reserved by the
    core (answer to ADR 0003 Q6): the replica stopped before the step finished, retry at once.
    """

    MODULE_SHUTDOWN = "module_shutdown"
    """transient: SIGTERM and the shutdown grace period ran out."""
    UNHANDLED = "unhandled"
    """transient: the handler raised an exception that is not a :class:`StepError`."""
    CANCELLED = "cancelled"
    """transient: the handler cancelled itself (not a ``step.cancel`` from the core)."""
    INPUT_INVALID = "input_invalid"
    """fatal: ``payload.input`` does not match the step's input model."""
    OUTPUT_INVALID = "output_invalid"
    """fatal: the handler returned a value that does not match the output model."""
    CONFIG_INVALID = "config_invalid"
    """fatal: ``ctx.config_as(Model)`` failed."""
    NO_STEP_HANDLER = "no_step_handler"
    """fatal: ``step.requested`` reached a module without ``@app.step``."""


class StepError(Exception):
    """Base class of handler failures with an explicit error class.

    ``code`` is a short machine-readable reason (``snake_case``) shown in the UI and used
    in metrics; ``message`` is for humans and must not contain secrets.
    """

    error_class: ClassVar[ErrorClass]
    default_code: ClassVar[str]

    def __init__(
        self,
        message: str = "",
        *,
        code: str | None = None,
        retry_after_s: float | None = None,
        recommendation: str | None = None,
        details: dict[str, object] | None = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.code = code or self.default_code
        self.recommendation = recommendation
        """Shown to the operator for ``attention`` (what to check or do)."""
        self.details = details
        """Structured context for the operator (at most 16 KiB)."""
        if retry_after_s is not None and retry_after_s < 0:
            raise ValueError("retry_after_s must be >= 0")
        self.retry_after_s = retry_after_s


class Transient(StepError):
    """Temporary failure: the core retries the step according to its ``retry`` policy."""

    error_class = "transient"
    default_code = "transient"


class Fatal(StepError):
    """Permanent failure: retrying cannot help; the run fails unless the step is optional."""

    error_class = "fatal"
    default_code = "fatal"


class NeedsAttention(StepError):
    """An operator has to decide: the run moves to ``needs_attention``."""

    error_class = "attention"
    default_code = "needs_attention"


class StepWaiting(BaseException):
    """Raised by ``ctx.wait``: ``step.waiting`` was sent and the handler must stop.

    The result is delivered later by :meth:`Module.complete` / :meth:`Module.fail`, possibly
    from another replica. It derives from ``BaseException`` so a broad ``except Exception``
    in a handler cannot swallow it.
    """


class ConfirmError(Exception):
    """``ctx.confirm`` was refused: the irreversible action must **not** be performed.

    A runtime that sees this escape the handler reports nothing (the core already knows).
    """

    def __init__(self, message: str, *, detail: str | None = None) -> None:
        super().__init__(message)
        self.detail = detail


class Superseded(ConfirmError):
    """The attempt is no longer current (a newer attempt, ``lost`` or ``timed_out``)."""


class Cancelled(ConfirmError):
    """The run or the step was cancelled."""


class DeadlineTooClose(ConfirmError):
    """Less time is left before the step deadline than ``extend_seconds``."""

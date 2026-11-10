"""Errors a step handler raises to tell the core how to treat a failure (§3.4)."""

from __future__ import annotations

from typing import ClassVar

from manufacture._wire import ErrorClass

__all__ = ["ErrorClass", "Fatal", "NeedsAttention", "StepError", "Transient"]


class StepError(Exception):
    """Base class of handler failures with an explicit error class.

    ``code`` is a short machine-readable reason (``snake_case``) shown in the UI and used
    in metrics; ``message`` is for humans and must not contain secrets.
    """

    error_class: ClassVar[ErrorClass]
    default_code: ClassVar[str]

    def __init__(
        self, message: str = "", *, code: str | None = None, retry_after_s: float | None = None
    ) -> None:
        super().__init__(message)
        self.message = message
        self.code = code or self.default_code
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

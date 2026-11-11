"""Manufacture SDK: write pipeline modules for the Manufacture core in Python."""

from manufacture._contract import CONTRACT_VERSION
from manufacture._version import __version__
from manufacture._wire import RunFinished, RunStarted
from manufacture.app import Module, RunRequest
from manufacture.context import AttemptHandle, Context, NoSettings, Secrets, SourceContext
from manufacture.errors import (
    Cancelled,
    DeadlineTooClose,
    ErrorCode,
    Fatal,
    NeedsAttention,
    StepError,
    Superseded,
    Transient,
)
from manufacture.settings import RuntimeSettings

__all__ = [
    "CONTRACT_VERSION",
    "AttemptHandle",
    "Cancelled",
    "Context",
    "DeadlineTooClose",
    "ErrorCode",
    "Fatal",
    "Module",
    "NeedsAttention",
    "NoSettings",
    "RunFinished",
    "RunRequest",
    "RunStarted",
    "RuntimeSettings",
    "Secrets",
    "SourceContext",
    "StepError",
    "Superseded",
    "Transient",
    "__version__",
]

"""Manufacture SDK: write pipeline modules for the Manufacture core in Python."""

from manufacture._contract import CONTRACT_VERSION
from manufacture._version import __version__
from manufacture.app import Module, RunRequest
from manufacture.context import Context, NoSettings, Secrets, SourceContext
from manufacture.errors import Fatal, NeedsAttention, StepError, Transient
from manufacture.settings import RuntimeSettings

__all__ = [
    "CONTRACT_VERSION",
    "Context",
    "Fatal",
    "Module",
    "NeedsAttention",
    "NoSettings",
    "RunRequest",
    "RuntimeSettings",
    "Secrets",
    "SourceContext",
    "StepError",
    "Transient",
    "__version__",
]

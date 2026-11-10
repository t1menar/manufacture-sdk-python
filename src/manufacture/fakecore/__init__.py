"""Fake core: run pipelines from their spec without the platform.

A test-only stand-in for the Manufacture core that executes **linear** pipelines
(one ``step`` per stage) with timeouts, leases, retries, ``optional`` steps, JSON
Merge Patch of outputs, ``concurrency.max_runs``, dedup keys and run cancellation, over
an in-memory broker with RabbitMQ semantics or over a real RabbitMQ. It speaks the
module protocol of contract 0.2.1; anything it cannot run faithfully (parallel,
foreach, routes, ``when``, input projections, schedules) is refused with
:class:`UnsupportedSpecError` rather than approximated.

It is not the core: no database, no API server, no blobs, no CEL. See
``docs/adr/0004-fake-core.md``.
"""

from manufacture.fakecore._bus import Bus, DeadLetter, MemoryBus, MemoryTransport
from manufacture.fakecore._engine import CoreEvent, FakeCore, ProtocolViolationError, merge_patch
from manufacture.fakecore._model import (
    STEP_NAMESPACE,
    Attempt,
    AttemptStatus,
    ErrorInfo,
    ModuleReplica,
    Rejection,
    Run,
    RunStatus,
    StaleResult,
    Step,
    StepStatus,
    Trigger,
    step_key,
)
from manufacture.fakecore._spec import UnsupportedSpecError

__all__ = [
    "STEP_NAMESPACE",
    "Attempt",
    "AttemptStatus",
    "Bus",
    "CoreEvent",
    "DeadLetter",
    "ErrorInfo",
    "FakeCore",
    "MemoryBus",
    "MemoryTransport",
    "ModuleReplica",
    "ProtocolViolationError",
    "Rejection",
    "Run",
    "RunStatus",
    "StaleResult",
    "Step",
    "StepStatus",
    "Trigger",
    "UnsupportedSpecError",
    "merge_patch",
    "step_key",
]

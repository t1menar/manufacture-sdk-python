"""What the fake core reports: runs, steps and attempts.

The shapes follow the core API (``openapi.yaml``: ``Run``, ``Step``, ``Attempt``) so a
test written against the fake core reads like one written against ``mfctl``/the
gateway. Fields and states of contract 0.3.0 are already present where they are
additive (``waiting_human``, ``visit``, ``step_key``, ``priority``, ``labels``,
``usage``); the 0.2.1 engine simply never produces the 0.3.0-only values.
"""

from __future__ import annotations

import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime as _datetime
from typing import TYPE_CHECKING, Any, Final, Literal

if TYPE_CHECKING:
    from datetime import datetime

__all__ = [
    "STEP_NAMESPACE",
    "TERMINAL_RUN_STATES",
    "Attempt",
    "AttemptStatus",
    "ErrorInfo",
    "ModuleReplica",
    "Rejection",
    "Run",
    "RunStatus",
    "StaleResult",
    "Step",
    "StepStatus",
    "Trigger",
    "TriggerKind",
    "step_key",
]

RunStatus = Literal[
    "queued", "running", "waiting_human", "succeeded", "failed", "needs_attention", "cancelled"
]
"""``waiting_human`` is a 0.3.0 state; the 0.2.1 engine never sets it."""

StepStatus = Literal[
    "pending", "dispatched", "running", "waiting", "succeeded", "failed", "skipped", "cancelled"
]

AttemptStatus = Literal[
    "scheduled",
    "dispatched",
    "running",
    "waiting",
    "succeeded",
    "failed",
    "timed_out",
    "lost",
    "cancelled",
]

TriggerKind = Literal["manual", "source", "api", "retry"]

TERMINAL_RUN_STATES: Final[frozenset[str]] = frozenset({"succeeded", "failed", "cancelled"})

STEP_NAMESPACE: Final = uuid.uuid5(uuid.NAMESPACE_URL, "https://manufacture.dev/steps")
"""``STEP_NS`` of contract 0.3.0 (messaging.md §4.5)."""


def step_key(run_id: str, stage: str, step_id: str, item_index: int | None, visit: int) -> str:
    """Stable id of one visit of a step: the same for every attempt of that visit.

    ``uuid5(STEP_NS, "<run_id>:<stage>:<step_id>:<item_index or ->:<visit>")``, as
    contract 0.3.0 defines it. Under 0.2.1 it is only reported, never sent.
    """
    item = "-" if item_index is None else str(item_index)
    return str(uuid.uuid5(STEP_NAMESPACE, f"{run_id}:{stage}:{step_id}:{item}:{visit}"))


def _jsonable(value: Any) -> Any:  # noqa: ANN401 (any JSON value)
    if isinstance(value, dict):
        return {("class" if k == "class_" else k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_jsonable(v) for v in value]
    if isinstance(value, _datetime):
        return value.isoformat()
    return value


@dataclass(frozen=True, slots=True)
class ErrorInfo:
    """``class`` is ``fatal | transient | attention`` from the module, or ``timeout | lost``
    set by the core. Core-made errors use ``module`` = ``None``."""

    class_: str
    code: str
    message: str

    def as_dict(self) -> dict[str, str]:
        return {"class": self.class_, "code": self.code, "message": self.message}


@dataclass(frozen=True, slots=True)
class Trigger:
    kind: TriggerKind
    by: str | None = None
    """Module name for ``source``; ``None`` for runs started through the fake core API."""
    trigger_id: str | None = None


@dataclass(slots=True)
class Attempt:
    attempt_id: str
    number: int
    status: AttemptStatus = "scheduled"
    replica_id: str | None = None
    progress: float | None = None
    message: str | None = None
    dispatched_at: datetime | None = None
    started_at: datetime | None = None
    """First sign of life (heartbeat or result) from a replica."""
    finished_at: datetime | None = None
    error: ErrorInfo | None = None
    output: dict[str, Any] | None = None


@dataclass(slots=True)
class Step:
    stage: str
    stage_index: int
    step_id: str
    module: str
    step_key: str
    item_index: int | None = None
    visit: int = 1
    status: StepStatus = "pending"
    optional: bool = False
    output: dict[str, Any] | None = None
    error: ErrorInfo | None = None
    attempts: list[Attempt] = field(default_factory=list)

    @property
    def attempt(self) -> Attempt | None:
        """The latest attempt."""
        return self.attempts[-1] if self.attempts else None


@dataclass(slots=True)
class Run:
    id: str
    pipeline: str
    revision: int
    trigger: Trigger
    data: dict[str, Any]
    created_at: datetime
    status: RunStatus = "queued"
    stage_index: int | None = None
    stage: str | None = None
    dedup_key: str | None = None
    priority: int = 0
    labels: dict[str, str] = field(default_factory=dict)
    started_at: datetime | None = None
    finished_at: datetime | None = None
    error: ErrorInfo | None = None
    steps: list[Step] = field(default_factory=list)
    usage: dict[str, Any] | None = None
    """Cumulative usage (0.3.0); always ``None`` under 0.2.1."""

    @property
    def done(self) -> bool:
        """Terminal, or parked for an operator (``needs_attention`` is not terminal in the
        core, but nothing happens to such a run in the fake core without a test acting)."""
        return self.status in TERMINAL_RUN_STATES or self.status == "needs_attention"

    def to_dict(self) -> dict[str, Any]:
        """JSON-ready view in the shape of the core API (``class`` instead of ``class_``,
        ISO timestamps)."""
        result = _jsonable(asdict(self))
        assert isinstance(result, dict)  # noqa: S101
        return result

    def step(self, step_id: str) -> Step:
        """The latest visit of ``step_id``."""
        for s in reversed(self.steps):
            if s.step_id == step_id:
                return s
        raise KeyError(step_id)


@dataclass(frozen=True, slots=True)
class ModuleReplica:
    """A replica the fake core has heard from (``module.online``/``module.heartbeat``)."""

    module: str
    replica_id: str
    status: Literal["online", "offline"]
    in_flight: int
    concurrency: int
    sdk: str | None
    last_seen: datetime


@dataclass(frozen=True, slots=True)
class StaleResult:
    """A result the core discarded (``mf_stale_results_total``)."""

    type: str
    attempt_id: str
    reason: Literal["not_current", "unknown_attempt", "run_finished"]


@dataclass(frozen=True, slots=True)
class Rejection:
    """A ``run.requested`` that did not create a run. Contract 0.3.0 sends ``run.rejected``
    to the module for these; under 0.2.1 the fake core only records them."""

    pipeline: str
    dedup_key: str | None
    code: Literal["unknown_pipeline", "invalid_data", "pipeline_deleted", "forbidden"]
    message: str
    producer: str

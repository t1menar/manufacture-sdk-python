"""Message contract between modules and the core: envelope v2 (contract 0.2.1).

This module is the only place in the SDK that knows the wire format and the broker
topology. The source of truth is the vendored core contract:
``_contract/schemas/envelope/v2/*.schema.json`` and ``docs/contract/messaging.md``.
Inbound messages are validated against the JSON Schema itself (exactly like the core),
then parsed into the typed models below. A few SDK-side choices that the contract does not
fix are listed in ``docs/adr/0003-module-runtime.md`` ("Open questions").
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Annotated, Any, Final, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from manufacture.contract import validate_envelope

__all__ = [
    "MAX_MESSAGE_BYTES",
    "MF_NAMESPACE",
    "SCHEMA_VERSION",
    "SDK_NAME",
    "AttemptRef",
    "CancelReason",
    "Envelope",
    "EnvelopeError",
    "ErrorClass",
    "MessageType",
    "ModuleHeartbeat",
    "ModuleOffline",
    "ModuleOnline",
    "RunRequested",
    "SdkInfo",
    "SourceTrigger",
    "StepCancel",
    "StepFailed",
    "StepHeartbeat",
    "StepRequested",
    "StepSucceeded",
    "Topology",
    "decode",
    "encode",
    "result_message_id",
    "run_request_message_id",
]

SCHEMA_VERSION: Final = 2
MAX_MESSAGE_BYTES: Final = 256 * 1024
"""Envelope size limit (messaging.md §6); bigger values go to blobs."""

MF_NAMESPACE: Final = uuid.UUID("32e7ee34-6c60-51c1-a912-93b117ba36cd")
"""``uuid5(NAMESPACE_URL, "https://manufacture.dev/messages")`` (messaging.md §4)."""

SDK_NAME: Final = "manufacture-sdk-python"

MessageType = Literal[
    "step.requested",
    "step.cancel",
    "source.trigger",
    "step.heartbeat",
    "step.succeeded",
    "step.failed",
    "run.requested",
    "module.online",
    "module.heartbeat",
    "module.offline",
]
ErrorClass = Literal["transient", "fatal", "attention"]
CancelReason = Literal["timeout", "lost", "cancelled", "superseded"]
Kind = Literal["step", "source"]

_MODEL = ConfigDict(extra="forbid", frozen=True)
_Name = Annotated[str, Field(pattern=r"^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$")]
_ReplicaId = Annotated[str, Field(min_length=1, max_length=128)]


class EnvelopeError(ValueError):
    """An inbound message violates the contract; it is dead-lettered, never retried."""


class AttemptRef(BaseModel):
    """One attempt of one step; ``id`` is unique per attempt."""

    model_config = _MODEL

    id: str
    number: Annotated[int, Field(ge=1)]


class Envelope(BaseModel):
    """Common wrapper of every message; ``payload`` is parsed per ``type``."""

    model_config = _MODEL

    message_id: uuid.UUID
    type: MessageType
    schema_version: Literal[2]
    occurred_at: datetime
    producer: str
    traceparent: str | None = None
    run_id: str | None = None
    attempt: AttemptRef | None = None
    payload: dict[str, Any]


# --- core -> module ---------------------------------------------------------------------------


class StepRequested(BaseModel):
    model_config = _MODEL

    pipeline: str
    revision: int
    stage: str
    step: str
    item_index: int | None = None
    input: dict[str, Any]
    config: dict[str, Any] = Field(default_factory=dict)
    deadline: datetime
    lease_seconds: int
    blob_prefix: str


class StepCancel(BaseModel):
    model_config = _MODEL

    reason: CancelReason


class SourceTrigger(BaseModel):
    model_config = _MODEL

    pipeline: str
    trigger_id: str
    data: dict[str, Any] | None = None


# --- module -> core ---------------------------------------------------------------------------


class StepHeartbeat(BaseModel):
    model_config = _MODEL

    progress: Annotated[float, Field(ge=0, le=1)] | None = None
    message: Annotated[str, Field(max_length=1024)] | None = None
    replica_id: _ReplicaId | None = None


class StepSucceeded(BaseModel):
    model_config = _MODEL

    output: dict[str, Any]
    replica_id: _ReplicaId | None = None
    duration_ms: Annotated[int, Field(ge=0)] | None = None


class StepFailed(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, populate_by_name=True)

    error_class: ErrorClass = Field(alias="class")  # ``class`` is a Python keyword
    code: Annotated[str, Field(max_length=128)] | None = None
    message: Annotated[str, Field(max_length=4096)]
    retry_after_seconds: Annotated[int, Field(ge=0)] | None = None
    replica_id: _ReplicaId | None = None
    duration_ms: Annotated[int, Field(ge=0)] | None = None


class RunRequested(BaseModel):
    model_config = _MODEL

    pipeline: _Name
    data: dict[str, Any]
    dedup_key: Annotated[str, Field(min_length=1, max_length=256)] | None = None
    trigger_id: str | None = None


class SdkInfo(BaseModel):
    model_config = _MODEL

    name: str = SDK_NAME
    version: str


class ModuleOnline(BaseModel):
    model_config = _MODEL

    module: _Name
    replica_id: _ReplicaId
    contract_version: str
    sdk: SdkInfo
    version: str | None = None
    concurrency: Annotated[int, Field(ge=1)] | None = None
    kinds: list[Kind] | None = None
    input_schema: dict[str, Any] | None = None
    output_schema: dict[str, Any] | None = None


class ModuleHeartbeat(BaseModel):
    model_config = _MODEL

    module: _Name
    replica_id: _ReplicaId
    in_flight: Annotated[int, Field(ge=0)] | None = None


class ModuleOffline(BaseModel):
    model_config = _MODEL

    module: _Name
    replica_id: _ReplicaId


# --- ids and encoding -------------------------------------------------------------------------


def result_message_id(
    attempt_id: str, type_: Literal["step.succeeded", "step.failed"]
) -> uuid.UUID:
    """``uuid5(NS, "<attempt_id>:<type>")`` (messaging.md §4)."""
    return uuid.uuid5(MF_NAMESPACE, f"{attempt_id}:{type_}")


def run_request_message_id(pipeline: str, dedup_key: str) -> uuid.UUID:
    """Stable id of ``run.requested`` so a re-sent request keeps its id (SDK choice)."""
    return uuid.uuid5(MF_NAMESPACE, f"run.requested:{pipeline}:{dedup_key}")


def encode(
    *,
    type_: MessageType,
    producer: str,
    payload: BaseModel,
    msg_id: uuid.UUID | None = None,
    run_id: str | None = None,
    attempt: AttemptRef | None = None,
    traceparent: str | None = None,
    now: datetime | None = None,
) -> tuple[uuid.UUID, bytes]:
    """Serialize an outbound envelope. Returns its ``message_id`` and the JSON body."""
    mid = msg_id or uuid.uuid4()
    env = Envelope(
        message_id=mid,
        type=type_,
        schema_version=SCHEMA_VERSION,
        occurred_at=now or datetime.now(UTC),
        producer=producer,
        traceparent=traceparent,
        run_id=run_id,
        attempt=attempt,
        payload=payload.model_dump(mode="json", by_alias=True, exclude_none=True),
    )
    return mid, env.model_dump_json(exclude_none=True).encode()


def decode(body: bytes) -> Envelope:
    """Check an inbound message against the contract schema and parse it."""
    if len(body) > MAX_MESSAGE_BYTES:
        raise EnvelopeError(f"message body is {len(body)} bytes, limit {MAX_MESSAGE_BYTES}")
    try:
        doc = json.loads(body)
    except (ValueError, UnicodeDecodeError) as exc:
        raise EnvelopeError("message body is not JSON") from exc
    if not isinstance(doc, dict):
        raise EnvelopeError("message body is not a JSON object")
    if errors := validate_envelope(doc):
        raise EnvelopeError("; ".join(str(e) for e in errors[:5]))
    try:
        return Envelope.model_validate(doc)
    except ValidationError as exc:  # pragma: no cover - the schema is stricter
        raise EnvelopeError(f"invalid envelope: {exc.error_count()} error(s)") from exc


# --- broker topology --------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Topology:
    """Exchange, queue and routing-key names (messaging.md §2).

    ``prefix`` isolates parallel test runs on one broker; production uses no prefix.
    """

    prefix: str = ""
    delivery_limit: int = 5

    @property
    def commands(self) -> str:
        """Direct exchange, core -> module work queues."""
        return f"{self.prefix}mf.commands"

    @property
    def control(self) -> str:
        """Topic exchange, core -> every replica (``step.cancel``)."""
        return f"{self.prefix}mf.control"

    @property
    def events(self) -> str:
        """Topic exchange, modules -> core; routing key = message type."""
        return f"{self.prefix}mf.events"

    @property
    def dlx(self) -> str:
        """Fanout dead-letter exchange."""
        return f"{self.prefix}mf.dlx"

    def work_queue(self, module: str) -> str:
        return f"{self.prefix}mf.module.{module}"

    @staticmethod
    def work_key(module: str) -> str:
        return f"module.{module}"

    def control_queue(self, module: str, replica_id: str) -> str:
        return f"{self.prefix}mf.control.{module}.{replica_id}"

    @staticmethod
    def control_key(module: str) -> str:
        return f"control.{module}"

    def work_queue_arguments(self) -> dict[str, Any]:
        """Arguments the core uses for ``mf.module.<name>``; redeclaring must match."""
        return {
            "x-queue-type": "quorum",
            "x-delivery-limit": self.delivery_limit,
            "x-dead-letter-exchange": self.dlx,
        }

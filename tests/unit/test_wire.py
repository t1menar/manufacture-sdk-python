from __future__ import annotations

import json
import uuid
from pathlib import Path

import pytest
from pydantic import BaseModel

from manufacture import _wire as wire
from manufacture.testing import ATTEMPT_ID, RUN_ID, step_cancel, step_request

VECTORS = Path(__file__).parent.parent / "contract" / "testdata" / "envelope"


@pytest.mark.parametrize("path", sorted((VECTORS / "valid").glob("*.json")), ids=lambda p: p.stem)
def test_decode_accepts_core_vectors(path: Path) -> None:
    env = wire.decode(path.read_bytes())
    models: dict[str, type[BaseModel]] = {
        "step.requested": wire.StepRequested,
        "step.cancel": wire.StepCancel,
        "source.trigger": wire.SourceTrigger,
        "step.heartbeat": wire.StepHeartbeat,
        "step.succeeded": wire.StepSucceeded,
        "step.failed": wire.StepFailed,
        "run.requested": wire.RunRequested,
        "module.online": wire.ModuleOnline,
        "module.heartbeat": wire.ModuleHeartbeat,
        "module.offline": wire.ModuleOffline,
    }
    model = models[env.type].model_validate(env.payload)
    # The typed model round-trips to exactly the vector payload.
    assert (
        model.model_dump(mode="json", by_alias=True, exclude_none=True)
        == json.loads(path.read_bytes())["payload"]
    )


@pytest.mark.parametrize("path", sorted((VECTORS / "invalid").glob("*.json")), ids=lambda p: p.stem)
def test_decode_rejects_core_vectors(path: Path) -> None:
    with pytest.raises(wire.EnvelopeError):
        wire.decode(path.read_bytes())


@pytest.mark.parametrize(
    "body",
    [b"not json", b"[]", b"x" * (wire.MAX_MESSAGE_BYTES + 1)],
    ids=["garbage", "array", "too-large"],
)
def test_decode_rejects_non_envelopes(body: bytes) -> None:
    with pytest.raises(wire.EnvelopeError):
        wire.decode(body)


def test_encode_round_trip() -> None:
    ref = wire.AttemptRef(id=ATTEMPT_ID, number=2)
    mid, body = wire.encode(
        type_="step.failed",
        producer="module:hello",
        payload=wire.StepFailed(error_class="fatal", code="x", message="m", retry_after_seconds=3),
        msg_id=wire.result_message_id(ATTEMPT_ID, "step.failed"),
        run_id=RUN_ID,
        attempt=ref,
    )
    env = wire.decode(body)
    assert env.message_id == mid
    assert env.attempt == ref
    assert env.payload == {"class": "fatal", "code": "x", "message": "m", "retry_after_seconds": 3}


def test_result_message_id_matches_contract() -> None:
    ns = uuid.uuid5(uuid.NAMESPACE_URL, "https://manufacture.dev/messages")
    assert ns == wire.MF_NAMESPACE
    assert wire.result_message_id(ATTEMPT_ID, "step.succeeded") == uuid.uuid5(
        ns, f"{ATTEMPT_ID}:step.succeeded"
    )
    assert wire.result_message_id(ATTEMPT_ID, "step.failed") != wire.result_message_id(
        ATTEMPT_ID, "step.succeeded"
    )


def test_builders_produce_contract_messages() -> None:
    assert wire.decode(step_request({"text": ""}, item_index=3)).type == "step.requested"
    assert wire.decode(step_cancel(reason="timeout")).type == "step.cancel"


def test_topology_names() -> None:
    t = wire.Topology()
    assert t.commands == "mf.commands"
    assert t.control == "mf.control"
    assert t.events == "mf.events"
    assert t.dlx == "mf.dlx"
    assert t.work_queue("hello") == "mf.module.hello"
    assert t.work_key("hello") == "module.hello"
    assert t.control_queue("hello", "r1") == "mf.control.hello.r1"
    assert t.control_key("hello") == "control.hello"
    assert t.work_queue_arguments() == {
        "x-queue-type": "quorum",
        "x-delivery-limit": 5,
        "x-dead-letter-exchange": "mf.dlx",
    }
    assert wire.Topology(prefix="it.").work_queue("m") == "it.mf.module.m"

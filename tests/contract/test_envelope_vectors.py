"""Core conformance vectors (``testdata/envelope``): accepted and rejected like in the core."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, cast

import pytest

from manufacture import _wire as wire
from manufacture.contract import validate_envelope

VECTORS = Path(__file__).parent / "testdata" / "envelope"


def vectors(kind: str) -> list[Path]:
    found = sorted((VECTORS / kind).glob("*.json"))
    assert found, f"no vectors under {kind}"
    return found


def load(path: Path) -> dict[str, Any]:
    return cast("dict[str, Any]", json.loads(path.read_text(encoding="utf-8")))


@pytest.mark.parametrize("path", vectors("valid"), ids=lambda p: p.stem)
def test_valid_vectors_pass(path: Path) -> None:
    assert validate_envelope(load(path)) == []


@pytest.mark.parametrize("path", vectors("invalid"), ids=lambda p: p.stem)
def test_invalid_vectors_are_rejected(path: Path) -> None:
    # The core also checks every ``$blob`` object of a payload (wire.decode does the same).
    with pytest.raises(wire.EnvelopeError):
        wire.decode(path.read_bytes())

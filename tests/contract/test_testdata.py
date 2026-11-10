"""The core testdata is accepted and rejected by the SDK exactly like by the core schemas."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, cast

import pytest
import yaml

from manufacture.contract import SpecError, check_resource, schema, validate_resource

TESTDATA = Path(__file__).parent / "testdata"
WANT = re.compile(r"^#\s*want:\s*(?P<path>[^|]+?)\s*\|\s*(?P<msg>.+)$", re.MULTILINE)


def load_documents(path: Path) -> list[dict[str, Any]]:
    text = path.read_text(encoding="utf-8")
    if path.suffix == ".json":
        return [cast("dict[str, Any]", json.loads(text))]
    return [cast("dict[str, Any]", d) for d in yaml.safe_load_all(text) if d is not None]


def files(sub: str) -> list[Path]:
    found = sorted(p for p in (TESTDATA / sub).rglob("*") if p.suffix in {".yaml", ".json"})
    assert found, f"no testdata under {sub}"
    return found


@pytest.mark.parametrize("path", files("valid"), ids=lambda p: p.name)
def test_valid_specs_pass(path: Path) -> None:
    for doc in load_documents(path):
        assert validate_resource(doc) == [], doc["metadata"]["name"]


@pytest.mark.parametrize("path", files("invalid/schema"), ids=lambda p: p.name)
def test_schema_invalid_specs_are_rejected_at_the_expected_paths(path: Path) -> None:
    errors = [e for doc in load_documents(path) for e in validate_resource(doc)]
    assert errors, f"{path.name} must be rejected"
    wanted = {m["path"] for m in WANT.finditer(path.read_text(encoding="utf-8"))}
    assert wanted, f"{path.name} has no '# want:' lines"
    assert wanted <= {e.path for e in errors}, errors


@pytest.mark.parametrize("path", files("invalid/rules"), ids=lambda p: p.name)
def test_rule_violations_are_schema_valid(path: Path) -> None:
    # Semantic rules belong to `manufacture validate`; the schema alone must accept these.
    for doc in load_documents(path):
        assert validate_resource(doc) == []


def test_unknown_kind() -> None:
    errors = validate_resource({"apiVersion": "manufacture.dev/v1", "kind": "Workflow"})
    assert [e.path for e in errors] == ["kind"]


def test_check_resource_raises_with_all_errors() -> None:
    doc = {"apiVersion": "manufacture.dev/v1", "kind": "Module", "metadata": {"name": "Bad"}}
    with pytest.raises(SpecError) as exc:
        check_resource(doc)
    # A missing property is reported at its parent, like in the core.
    assert {e.path for e in exc.value.errors} == {"", "metadata.name"}
    assert "<root>: 'spec' is a required property" in str(exc.value)


def test_schema_lookup() -> None:
    assert schema("v1/module.schema.json")["title"] == "Manufacture v1: Module"
    with pytest.raises(Exception, match="nope"):
        schema("v1/nope.schema.json")

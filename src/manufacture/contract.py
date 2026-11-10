"""Validation of Manufacture resources against the vendored contract schemas.

The core owns the contract; this module only applies the JSON Schemas it publishes,
so the SDK accepts and rejects specs exactly like ``manufacture validate`` does at the
schema level. Cross-field rules (CEL, route targets, loop limits) stay in the core.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from functools import cache
from typing import TYPE_CHECKING, Any, Final

from jsonschema import Draft202012Validator, ValidationError
from referencing import Registry, Resource
from referencing.jsonschema import DRAFT202012

from manufacture._contract import CONTRACT_COMMIT, CONTRACT_VERSION, schema_files

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping

__all__ = [
    "API_VERSION",
    "CONTRACT_COMMIT",
    "CONTRACT_VERSION",
    "KINDS",
    "ResourceError",
    "SpecError",
    "check_resource",
    "schema",
    "validate_envelope",
    "validate_resource",
]

API_VERSION: Final = "manufacture.dev/v1"
SCHEMA_BASE: Final = "https://manufacture.dev/schemas/"
ENVELOPE_SCHEMA: Final = "envelope/v2/envelope.schema.json"
KINDS: Final[Mapping[str, str]] = {
    "Module": "v1/module.schema.json",
    "Pipeline": "v1/pipeline.schema.json",
}


@dataclass(frozen=True, slots=True)
class ResourceError:
    """One schema violation. ``path`` uses the core notation: ``spec.stages[0].step.timeout``."""

    path: str
    message: str

    def __str__(self) -> str:
        return f"{self.path or '<root>'}: {self.message}"


class SpecError(ValueError):
    """A resource does not match the contract."""

    def __init__(self, errors: list[ResourceError]) -> None:
        self.errors = errors
        super().__init__("; ".join(str(e) for e in errors))


@cache
def _registry() -> Registry[Any]:
    resources: list[tuple[str, Resource[Any]]] = []
    for rel, file in schema_files().items():
        doc = json.loads(file.read_text(encoding="utf-8"))
        resources.append((SCHEMA_BASE + rel, Resource.from_contents(doc, DRAFT202012)))
    return Registry().with_resources(resources)


def schema(path: str) -> dict[str, Any]:
    """Return a vendored schema by its path relative to ``schemas/`` (``v1/module.schema.json``)."""
    contents = _registry().contents(SCHEMA_BASE + path)
    if not isinstance(contents, dict):
        raise TypeError(f"schema {path!r} is not a JSON object")
    return contents


@cache
def _validator(path: str) -> Draft202012Validator:
    return Draft202012Validator(schema(path), registry=_registry())


def _format_path(parts: Iterator[str | int]) -> str:
    out = ""
    for part in parts:
        if isinstance(part, int):
            out += f"[{part}]"
        else:
            out += f".{part}" if out else part
    return out


def _flatten(error: ValidationError) -> Iterator[ResourceError]:
    # Report the most specific causes of anyOf/oneOf failures instead of the umbrella error.
    if error.context:
        for sub in error.context:
            yield from _flatten(sub)
        return
    path = _format_path(iter(error.absolute_path))
    if error.validator == "additionalProperties" and isinstance(error.instance, dict):
        allowed = (
            set(error.schema.get("properties", {})) if isinstance(error.schema, dict) else set()
        )
        for name in sorted(set(error.instance) - allowed):
            field = f"{path}.{name}" if path else name
            yield ResourceError(field, f'unknown field "{name}"')
        return
    yield ResourceError(path, error.message)


def validate_resource(doc: Mapping[str, Any]) -> list[ResourceError]:
    """Validate one resource document (already parsed from YAML or JSON).

    Returns all violations sorted by path; an empty list means the document matches
    the schema of its ``kind``.
    """
    kind = doc.get("kind")
    if not isinstance(kind, str) or kind not in KINDS:
        known = ", ".join(sorted(KINDS))
        return [ResourceError("kind", f"unknown kind {kind!r}, expected one of {known}")]
    errors = {e for err in _validator(KINDS[kind]).iter_errors(doc) for e in _flatten(err)}
    return sorted(errors, key=lambda e: (e.path, e.message))


def check_resource(doc: Mapping[str, Any]) -> None:
    """Like :func:`validate_resource`, but raises :class:`SpecError` on violations."""
    if errors := validate_resource(doc):
        raise SpecError(errors)


def validate_envelope(doc: Mapping[str, Any]) -> list[ResourceError]:
    """Validate a broker message (envelope v2 and its typed payload) against the contract."""
    errors = {e for err in _validator(ENVELOPE_SCHEMA).iter_errors(doc) for e in _flatten(err)}
    return sorted(errors, key=lambda e: (e.path, e.message))

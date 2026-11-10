"""Vendored copy of the Manufacture contract (``manufacture-core/schemas``).

Do not edit the files under ``schemas/`` by hand: they are written by
``scripts/contract_sync.py`` from the commit pinned in ``contract.lock.json`` and the
drift test fails on any local change.
"""

from __future__ import annotations

import json
from functools import cache
from importlib import resources
from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    from importlib.resources.abc import Traversable

__all__ = ["CONTRACT_COMMIT", "CONTRACT_VERSION", "lock", "schema_files", "schemas_root"]


def schemas_root() -> Traversable:
    """Root of the vendored ``schemas/`` directory inside the installed package."""
    return resources.files(__name__).joinpath("schemas")


@cache
def lock() -> dict[str, Any]:
    """Parsed ``contract.lock.json``: pinned core commit, version and file hashes."""
    text = resources.files(__name__).joinpath("contract.lock.json").read_text(encoding="utf-8")
    return cast("dict[str, Any]", json.loads(text))


def schema_files() -> dict[str, Traversable]:
    """Vendored JSON Schemas keyed by their path relative to ``schemas/``."""
    found: dict[str, Traversable] = {}

    def walk(node: Traversable, prefix: str) -> None:
        for child in node.iterdir():
            rel = f"{prefix}{child.name}"
            if child.is_dir():
                walk(child, rel + "/")
            elif child.name.endswith(".json"):
                found[rel] = child

    walk(schemas_root(), "")
    return dict(sorted(found.items()))


CONTRACT_VERSION: str = schemas_root().joinpath("VERSION").read_text(encoding="utf-8").strip()
"""Semantic version of the vendored contract (``schemas/VERSION`` of the core)."""

CONTRACT_COMMIT: str = str(lock()["commit"])
"""Core commit the contract was vendored from."""

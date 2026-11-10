"""``mf-module``: run, inspect and test a module.

``TARGET`` is ``package.module:attribute`` (attribute defaults to ``app``) or a path to a
Python file, e.g. ``main.py`` or ``main.py:app``.
"""

from __future__ import annotations

import argparse
import asyncio
import importlib
import importlib.util
import json
import sys
from pathlib import Path
from typing import Any

from manufacture.app import Module

__all__ = ["load_module", "main"]


def load_module(target: str) -> Module[Any]:
    """Import ``TARGET`` and return the :class:`Module` instance it names."""
    ref, _, attr = target.partition(":")
    attr = attr or "app"
    if ref.endswith(".py"):
        path = Path(ref).resolve()
        # Several modules of one repo are often all called main.py: keep each one importable.
        name = path.stem if path.stem not in sys.modules else f"{path.stem}_{abs(hash(path))}"
        spec = importlib.util.spec_from_file_location(name, path)
        if spec is None or spec.loader is None:
            raise SystemExit(f"cannot import {path}")
        mod = importlib.util.module_from_spec(spec)
        sys.modules[name] = mod
        sys.path.insert(0, str(path.parent))
        spec.loader.exec_module(mod)
    else:
        sys.path.insert(0, str(Path.cwd()))
        mod = importlib.import_module(ref)
    obj = getattr(mod, attr, None)
    if not isinstance(obj, Module):
        raise SystemExit(f"{target}: {attr!r} is not a manufacture.Module")
    return obj


def _read_json(path: str) -> dict[str, Any]:
    text = sys.stdin.read() if path == "-" else Path(path).read_text(encoding="utf-8")
    doc = json.loads(text)
    if not isinstance(doc, dict):
        raise SystemExit(f"{path}: expected a JSON object")
    return doc


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="mf-module", description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)
    p_run = sub.add_parser("run", help="connect to the broker and serve (MF_* env)")
    p_run.add_argument("target")
    p_man = sub.add_parser("manifest", help="print the module manifest (JSON)")
    p_man.add_argument("target")
    p_test = sub.add_parser("test", help="call the step handler once without a broker")
    p_test.add_argument("target")
    p_test.add_argument("--input", required=True, help="JSON file with the step input, - for stdin")
    p_test.add_argument("--config", help="JSON file with the step config")
    args = parser.parse_args(argv)

    app = load_module(args.target)
    if args.command == "run":
        app.run()
        return 0
    if args.command == "manifest":
        print(json.dumps(app.manifest(), indent=2, ensure_ascii=False))  # noqa: T201
        return 0

    from manufacture.testing import call_step  # noqa: PLC0415 (only needed here)

    config = _read_json(args.config) if args.config else None
    result = asyncio.run(call_step(app, _read_json(args.input), config=config))
    print(json.dumps({"type": result.type, **result.payload}, indent=2, ensure_ascii=False))  # noqa: T201
    return 0 if result.succeeded else 1


if __name__ == "__main__":
    sys.exit(main())

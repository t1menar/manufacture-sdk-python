"""``mf-fakecore``: run a pipeline from its spec on the fake core and print the runs.

Example (Hello World, all modules in-process, in-memory broker)::

    mf-fakecore -f hello-world/pipeline.yaml hello-world/modules/*/main.py --run hello-world \\
        --data '{"text": ""}'

Without ``--run``/``--trigger`` it waits for ``--runs`` runs started by the modules
themselves (``on_start`` sources). Exit status 0 when every awaited run succeeded.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from typing import Any

from manufacture.cli import load_module
from manufacture.fakecore._engine import FakeCore

__all__ = ["main"]


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="mf-fakecore", description=__doc__.splitlines()[0])
    p.add_argument(
        "targets", nargs="*", metavar="TARGET", help="module code: path.py[:app] or pkg.mod[:app]"
    )
    p.add_argument("-f", "--spec", action="append", required=True, help="spec file (repeatable)")
    p.add_argument("--broker", choices=["memory", "amqp"], default="memory")
    p.add_argument("--amqp-url", help="RabbitMQ URL for --broker amqp (default $MF_AMQP_URL)")
    action = p.add_mutually_exclusive_group()
    action.add_argument("--run", metavar="PIPELINE", help="start a run directly (API)")
    action.add_argument("--trigger", metavar="PIPELINE", help="send source.trigger to the source")
    p.add_argument("--data", default=None, help="run data as a JSON object")
    p.add_argument("--runs", type=int, default=1, help="runs to wait for (default 1)")
    p.add_argument("--timeout", type=float, default=60.0, help="seconds to wait (default 60)")
    p.add_argument(
        "--no-module-schedules",
        action="store_true",
        help="do not run on_start/every sources of the modules",
    )
    return p


async def _main(args: argparse.Namespace) -> int:
    data: dict[str, Any] | None = json.loads(args.data) if args.data is not None else None
    modules = [load_module(t) for t in args.targets]
    async with FakeCore(
        args.spec,
        modules,
        broker=args.broker,
        amqp_url=args.amqp_url,
        module_schedules=not args.no_module_schedules,
    ) as core:
        if args.run:
            run = await core.start_run(args.run, data)
            runs = [await core.wait(run.id, timeout=args.timeout)]
        elif args.trigger:
            trigger_id = await core.trigger(args.trigger, data)
            runs = await core.wait_for_runs(
                args.trigger, trigger_id=trigger_id, count=args.runs, timeout=args.timeout
            )
        else:
            runs = await core.wait_for_runs(count=args.runs, timeout=args.timeout)
    print(json.dumps([r.to_dict() for r in runs], indent=2, ensure_ascii=False))  # noqa: T201
    return 0 if all(r.status == "succeeded" for r in runs) else 1


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        return asyncio.run(_main(args))
    except TimeoutError as exc:
        print(f"mf-fakecore: {exc}", file=sys.stderr)  # noqa: T201
        return 2

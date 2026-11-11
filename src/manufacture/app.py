"""``Module``: declare step and source handlers and run them against the core."""

from __future__ import annotations

import asyncio
import inspect
import re
from collections.abc import AsyncIterator, Callable, Coroutine, Mapping
from dataclasses import dataclass, field
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Final, Generic, Literal, TypeVar, cast, overload

from pydantic import BaseModel, ConfigDict, Field, field_validator

from manufacture._contract import CONTRACT_VERSION
from manufacture._version import __version__
from manufacture.context import AttemptHandle, Context, NoSettings, S, SourceContext

if TYPE_CHECKING:
    from manufacture.errors import StepError
    from manufacture.runtime import Runtime
    from manufacture.settings import RuntimeSettings
    from manufacture.transport import Transport

__all__ = [
    "Module",
    "RunListener",
    "RunRequest",
    "SourceHandler",
    "SourceSpec",
    "StepHandler",
    "StepSpec",
    "parse_duration",
]

InT = TypeVar("InT", bound=BaseModel)
OutT = TypeVar("OutT", bound=BaseModel)
_S = TypeVar("_S", bound=BaseModel)  # plain TypeVar: aliases below cannot use a defaulted one

_NAME: Final = re.compile(r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")
_DURATION: Final = re.compile(r"(\d+)(ms|s|m|h|d)")
_UNIT_S: Final = {"ms": 0.001, "s": 1.0, "m": 60.0, "h": 3600.0, "d": 86400.0}


def parse_duration(value: str | float | timedelta) -> float:
    """Seconds from ``30s``, ``1h30m`` (contract duration syntax), a number or a timedelta."""
    if isinstance(value, timedelta):
        seconds = value.total_seconds()
    elif isinstance(value, int | float):
        seconds = float(value)
    else:
        parts = _DURATION.findall(value)
        if not parts or "".join(n + u for n, u in parts) != value:
            raise ValueError(f"invalid duration {value!r}; use e.g. 30s, 5m, 1h30m")
        seconds = sum(int(n) * _UNIT_S[u] for n, u in parts)
    if seconds <= 0:
        raise ValueError("duration must be positive")
    return seconds


class RunRequest(BaseModel):
    """A request to start a run, yielded by a source handler."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    data: dict[str, Any] = Field(default_factory=dict)
    """Initial run data; must be JSON-serializable. Read-only views such as ``ctx.data``
    and ``ctx.config`` may be passed, at any depth: they are copied into plain dicts."""
    dedup_key: str | None = Field(default=None, min_length=1, max_length=256)
    """Business key; the core starts at most one run per key. Default: per trigger + index."""
    pipeline: str | None = None
    """Overrides the pipeline of the source declaration."""
    priority: int | None = Field(default=None, ge=-100, le=100)
    """Run priority (admission and dispatch order)."""
    labels: dict[str, str] = Field(default_factory=dict)
    """Free-form labels of the run (for example ``{"idea_ref": "..."}``), visible in notifications."""

    @field_validator("data", mode="before")
    @classmethod
    def _plain_data(cls, value: object) -> object:
        return _plain(value)


def _plain(value: object) -> object:
    """Copy nested mappings (``MappingProxyType`` included) into dicts, lists stay lists."""
    if isinstance(value, Mapping):
        return {k: _plain(v) for k, v in value.items()}
    if isinstance(value, list | tuple):
        return [_plain(v) for v in value]
    return value


RunListener = Callable[[str, Any], Coroutine[Any, Any, None]]
"""``async def on_event(run_id: str, event: RunStarted | RunFinished)``."""
StepHandler = Callable[[Context[_S], InT], Coroutine[Any, Any, OutT]]
SourceHandler = Callable[[SourceContext[_S]], AsyncIterator[RunRequest]]


@dataclass(frozen=True, slots=True)
class StepSpec(Generic[_S, InT, OutT]):
    name: str
    handler: StepHandler[_S, InT, OutT]
    input_model: type[InT]
    output_model: type[OutT]

    def manifest(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "input_schema": self.input_model.model_json_schema(),
            "output_schema": self.output_model.model_json_schema(),
        }


@dataclass(frozen=True, slots=True)
class SourceSpec(Generic[_S]):
    name: str
    pipeline: str
    handler: SourceHandler[_S]
    on_start: bool = False
    every_s: float | None = None

    def manifest(self) -> dict[str, Any]:
        doc: dict[str, Any] = {"name": self.name, "pipeline": self.pipeline}
        if self.on_start:
            doc["on_start"] = True
        if self.every_s is not None:
            doc["every_s"] = self.every_s
        return doc


@dataclass(slots=True)
class _Registry(Generic[_S]):
    step: StepSpec[_S, Any, Any] | None = None
    sources: list[SourceSpec[_S]] = field(default_factory=list)


class Module(Generic[S]):
    """A Manufacture module: one step handler and/or source handlers.

    Example::

        app = Module("hello-hello", version="0.1.0")


        @app.step(input=Text, output=Text)
        async def handle(ctx: Context, inp: Text) -> Text:
            return Text(text=inp.text + "hello")


        if __name__ == "__main__":
            app.run()
    """

    @overload
    def __init__(
        self: Module[NoSettings],
        name: str,
        *,
        version: str = "0.0.0",
        concurrency: int = 1,
        min_budget_usd: float | None = None,
    ) -> None: ...

    @overload
    def __init__(
        self: Module[S],
        name: str,
        *,
        version: str = "0.0.0",
        concurrency: int = 1,
        settings: type[S],
        min_budget_usd: float | None = None,
    ) -> None: ...

    def __init__(
        self,
        name: str,
        *,
        version: str = "0.0.0",
        concurrency: int = 1,
        settings: type[S] | None = None,
        min_budget_usd: float | None = None,
    ) -> None:
        if not _NAME.fullmatch(name) or len(name) > 63:
            raise ValueError(f"module name {name!r} must be a lowercase DNS label (1-63 chars)")
        if not 1 <= concurrency <= 1024:
            raise ValueError("concurrency must be within 1..1024")
        self.name = name
        self.version = version
        self.concurrency = concurrency
        self.min_budget_usd = min_budget_usd
        """Smallest run budget left for this module to be dispatched (sent in ``module.online``)."""
        self.run_listeners: dict[str, list[RunListener]] = {}
        self._runtime: Runtime[Any] | None = None
        self._settings_type: type[BaseModel] = settings or NoSettings
        self._settings: S | None = None
        self._registry: _Registry[S] = _Registry()

    # --- declaration ------------------------------------------------------------------------

    @property
    def step_spec(self) -> StepSpec[S, Any, Any] | None:
        return self._registry.step

    @property
    def sources(self) -> tuple[SourceSpec[S], ...]:
        return tuple(self._registry.sources)

    def step(
        self, *, input: type[InT], output: type[OutT], name: str | None = None
    ) -> Callable[[StepHandler[S, InT, OutT]], StepHandler[S, InT, OutT]]:
        """Register the step handler: ``async def handle(ctx, inp: In) -> Out``.

        A module has one step handler; the core sends every ``step.requested`` of this
        module to it. ``input`` is validated before the call (invalid input fails the step
        as ``fatal``), the returned value is validated against ``output``.
        """

        def register(fn: StepHandler[S, InT, OutT]) -> StepHandler[S, InT, OutT]:
            if not inspect.iscoroutinefunction(fn):
                raise TypeError("a step handler must be an async function")
            if self._registry.step is not None:
                raise ValueError(f"module {self.name} already has a step handler")
            self._registry.step = StepSpec(name or fn.__name__, fn, input, output)
            return fn

        return register

    def source(
        self,
        *,
        pipeline: str,
        on_start: bool = False,
        every: str | float | timedelta | None = None,
        name: str | None = None,
    ) -> Callable[[SourceHandler[S]], SourceHandler[S]]:
        """Register a source: an async generator yielding :class:`RunRequest`.

        It runs on ``source.trigger`` from the core (UI button, schedule, ``mfctl run``).

        ``on_start`` (once per replica start) and ``every`` (``"2h"``, seconds, timedelta;
        in each replica) are **temporary**: they run in every replica, so with several
        replicas they start several runs. Schedules are moving to the core, which will send
        ``source.trigger``; then both options will be removed. A warning is logged at start.
        """
        every_s = parse_duration(every) if every is not None else None

        def register(fn: SourceHandler[S]) -> SourceHandler[S]:
            if not inspect.isasyncgenfunction(fn):
                raise TypeError("a source handler must be an async generator (use `yield`)")
            spec = SourceSpec(name or fn.__name__, pipeline, fn, on_start, every_s)
            if any(s.pipeline == pipeline for s in self._registry.sources):
                raise ValueError(f"module {self.name} already has a source for {pipeline}")
            self._registry.sources.append(spec)
            return fn

        return register

    def on_run_started(self, fn: RunListener) -> RunListener:
        """Handle ``run.started`` (needs ``subscribe: [run.started]`` in the Module resource)."""
        return self._listen("run.started", fn)

    def on_run_finished(self, fn: RunListener) -> RunListener:
        """Handle ``run.finished`` (``succeeded|failed|cancelled``, with usage and final data)."""
        return self._listen("run.finished", fn)

    def _listen(self, event: str, fn: RunListener) -> RunListener:
        if not inspect.iscoroutinefunction(fn):
            raise TypeError("a run listener must be an async function")
        self.run_listeners.setdefault(event, []).append(fn)
        return fn

    async def complete(
        self,
        handle: AttemptHandle | str,
        output: BaseModel | Mapping[str, Any],
        *,
        cost_usd: float | None = None,
    ) -> None:
        """Finish an attempt that called ``ctx.wait`` (from any replica of this module).

        ``handle`` is ``str(ctx.attempt_handle)`` kept by the module. The output is validated
        against the step output model.
        """
        from manufacture import _wire as wire  # noqa: PLC0415

        spec = self._registry.step
        if spec is None:
            raise RuntimeError("module has no step handler")
        model = spec.output_model.model_validate(
            output.model_dump() if isinstance(output, BaseModel) else dict(output)
        )
        usage = wire.Usage(cost_usd=cost_usd) if cost_usd else None
        result = wire.StepSucceeded(output=model.model_dump(mode="json"), usage=usage)
        await self._running().finish(AttemptHandle.parse(handle), result)

    async def fail(
        self,
        handle: AttemptHandle | str,
        error: StepError,
    ) -> None:
        """Fail an attempt that called ``ctx.wait`` with a :class:`StepError`."""
        from manufacture.runtime import classify_exception  # noqa: PLC0415

        await self._running().finish(AttemptHandle.parse(handle), classify_exception(error))

    def _running(self) -> Runtime[Any]:
        if self._runtime is None:
            raise RuntimeError("the module is not running; start it with Module.serve()")
        return self._runtime

    # --- runtime ----------------------------------------------------------------------------

    @property
    def kinds(self) -> list[Literal["step", "source"]]:
        kinds: list[Literal["step", "source"]] = []
        if self._registry.step is not None:
            kinds.append("step")
        if self._registry.sources:
            kinds.append("source")
        return kinds

    @property
    def settings(self) -> S:
        """Module settings, loaded once (from the environment for ``BaseSettings``)."""
        if self._settings is None:
            self._settings = cast("S", self._settings_type())
        return self._settings

    def manifest(self) -> dict[str, Any]:
        """Manifest sent in ``module.online`` and printed by ``mf-module manifest``."""
        return {
            "module": self.name,
            "version": self.version,
            "sdk_version": __version__,
            "contract_version": CONTRACT_VERSION,
            "concurrency": self.concurrency,
            "kinds": self.kinds,
            "step": self._registry.step.manifest() if self._registry.step else None,
            "sources": [s.manifest() for s in self._registry.sources],
            "run_events": sorted(self.run_listeners),
        }

    async def serve(
        self,
        *,
        settings: RuntimeSettings | None = None,
        transport: Transport | None = None,
        install_signal_handlers: bool = True,
    ) -> None:
        """Run until SIGTERM/SIGINT (or :meth:`Runtime.request_shutdown`)."""
        from manufacture.runtime import Runtime  # noqa: PLC0415 (import cycle)

        await Runtime(self, settings=settings, transport=transport).serve(
            install_signal_handlers=install_signal_handlers
        )

    def run(self, *, settings: RuntimeSettings | None = None) -> None:
        """Blocking entry point for ``if __name__ == "__main__": app.run()``."""
        from manufacture._log import configure_logging  # noqa: PLC0415
        from manufacture.settings import RuntimeSettings  # noqa: PLC0415

        rs = settings or RuntimeSettings()
        configure_logging(rs.log_format, rs.log_level)
        asyncio.run(self.serve(settings=rs))

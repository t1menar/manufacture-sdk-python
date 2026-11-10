"""``Module``: declare step and source handlers and run them against the core."""

from __future__ import annotations

import asyncio
import inspect
import re
from collections.abc import AsyncIterator, Callable, Coroutine
from dataclasses import dataclass, field
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Final, Generic, Literal, TypeVar, cast, overload

from pydantic import BaseModel, ConfigDict, Field

from manufacture._contract import CONTRACT_VERSION
from manufacture._version import __version__
from manufacture.context import Context, NoSettings, S, SourceContext

if TYPE_CHECKING:
    from manufacture.settings import RuntimeSettings
    from manufacture.transport import Transport

__all__ = [
    "Module",
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
    dedup_key: str | None = Field(default=None, min_length=1, max_length=256)
    """Business key; the core starts at most one run per key. Default: per trigger + index."""
    pipeline: str | None = None
    """Overrides the pipeline of the source declaration."""


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
        self: Module[NoSettings], name: str, *, version: str = "0.0.0", concurrency: int = 1
    ) -> None: ...

    @overload
    def __init__(
        self: Module[S],
        name: str,
        *,
        version: str = "0.0.0",
        concurrency: int = 1,
        settings: type[S],
    ) -> None: ...

    def __init__(
        self,
        name: str,
        *,
        version: str = "0.0.0",
        concurrency: int = 1,
        settings: type[S] | None = None,
    ) -> None:
        if not _NAME.fullmatch(name) or len(name) > 63:
            raise ValueError(f"module name {name!r} must be a lowercase DNS label (1-63 chars)")
        if not 1 <= concurrency <= 1024:
            raise ValueError("concurrency must be within 1..1024")
        self.name = name
        self.version = version
        self.concurrency = concurrency
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

        It runs on ``source.trigger`` from the core (UI button, schedule, ``mfctl run``),
        once per replica start when ``on_start`` is true, and every ``every`` (``"2h"``,
        seconds, timedelta) in each replica.
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

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import timedelta

import pytest

from manufacture import Context, Module, RunRequest, SourceContext
from manufacture.app import parse_duration
from tests.unit.conftest import Text


def test_module_name_must_be_dns_label() -> None:
    with pytest.raises(ValueError, match="DNS label"):
        Module("Hello_World")
    with pytest.raises(ValueError, match="concurrency"):
        Module("ok", concurrency=0)


def test_one_step_handler_per_module() -> None:
    app = Module("m")

    @app.step(input=Text, output=Text)
    async def first(ctx: Context, inp: Text) -> Text:
        return inp

    with pytest.raises(ValueError, match="already has a step"):

        @app.step(input=Text, output=Text)
        async def second(ctx: Context, inp: Text) -> Text:
            return inp


def test_handlers_must_be_async() -> None:
    app = Module("m")
    with pytest.raises(TypeError, match="async"):
        app.step(input=Text, output=Text)(lambda ctx, inp: inp)  # type: ignore[arg-type,return-value]
    with pytest.raises(TypeError, match="async generator"):

        @app.source(pipeline="p")  # type: ignore[arg-type]
        async def not_a_generator(ctx: SourceContext) -> RunRequest:
            return RunRequest()


def test_one_source_per_pipeline() -> None:
    app = Module("m")

    @app.source(pipeline="p")
    async def a(ctx: SourceContext) -> AsyncIterator[RunRequest]:
        yield RunRequest()

    with pytest.raises(ValueError, match="already has a source"):

        @app.source(pipeline="p")
        async def b(ctx: SourceContext) -> AsyncIterator[RunRequest]:
            yield RunRequest()


def test_manifest() -> None:
    app = Module("hello-hello", version="1.2.3", concurrency=4)

    @app.step(input=Text, output=Text)
    async def handle(ctx: Context, inp: Text) -> Text:
        return inp

    @app.source(pipeline="hello-world", on_start=True, every="1h30m")
    async def start(ctx: SourceContext) -> AsyncIterator[RunRequest]:
        yield RunRequest()

    doc = app.manifest()
    assert doc["module"] == "hello-hello"
    assert doc["version"] == "1.2.3"
    assert doc["concurrency"] == 4
    assert doc["contract_version"] == "0.3.0"
    assert doc["kinds"] == ["step", "source"]
    assert doc["step"]["name"] == "handle"
    assert doc["step"]["input_schema"]["required"] == ["text"]
    assert doc["sources"] == [
        {"name": "start", "pipeline": "hello-world", "on_start": True, "every_s": 5400.0}
    ]


@pytest.mark.parametrize(
    ("value", "seconds"),
    [
        ("30s", 30.0),
        ("1h30m", 5400.0),
        ("250ms", 0.25),
        ("7d", 604800.0),
        (5, 5.0),
        (timedelta(minutes=2), 120.0),
    ],
)
def test_parse_duration(value: str | float | timedelta, seconds: float) -> None:
    assert parse_duration(value) == seconds


@pytest.mark.parametrize("value", ["", "1.5h", "10", "5x", "0s", -1])
def test_parse_duration_rejects(value: str | float) -> None:
    with pytest.raises(ValueError, match="duration"):
        parse_duration(value)

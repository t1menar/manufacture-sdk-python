"""``mf-fakecore`` with module files that share the name ``main.py``."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from manufacture.fakecore.cli import main

MODULE = """
from pydantic import BaseModel
from manufacture import Context, Module

class Text(BaseModel):
    text: str

app = Module("{name}")

@app.step(input=Text, output=Text)
async def handle(ctx: Context, inp: Text) -> Text:
    return Text(text=inp.text + "{suffix}")
"""

SOURCE = """
from manufacture import Module, RunRequest, SourceContext

app = Module("src")

@app.source(pipeline="p", on_start=True)
async def start(ctx: SourceContext):
    yield RunRequest(data={"text": ">"}, dedup_key=ctx.trigger_id)
"""

SPEC = {
    "apiVersion": "manufacture.dev/v1",
    "kind": "Pipeline",
    "metadata": {"name": "p"},
    "spec": {
        "trigger": {"source": "src"},
        "stages": [
            {"name": "one", "step": {"module": "a"}},
            {"name": "two", "step": {"module": "b"}},
        ],
    },
}


def module(name: str) -> dict[str, object]:
    return {
        "apiVersion": "manufacture.dev/v1",
        "kind": "Module",
        "metadata": {"name": name},
        "spec": {"image": f"x/{name}:1"},
    }


@pytest.fixture
def project(tmp_path: Path) -> tuple[Path, list[str]]:
    targets = []
    for name, code in {
        "a": MODULE.format(name="a", suffix="A"),
        "b": MODULE.format(name="b", suffix="B"),
        "src": SOURCE,
    }.items():
        (tmp_path / name).mkdir()
        (tmp_path / name / "main.py").write_text(code, encoding="utf-8")
        targets.append(str(tmp_path / name / "main.py"))
    spec = tmp_path / "spec.json"
    spec.write_text(json.dumps([module("a"), module("b"), module("src"), SPEC]), encoding="utf-8")
    return spec, targets


def test_run_prints_the_run(
    project: tuple[Path, list[str]], capsys: pytest.CaptureFixture[str]
) -> None:
    spec, targets = project
    code = main(
        ["-f", str(spec), *targets, "--no-module-schedules", "--run", "p", "--data", '{"text": ""}']
    )
    [run] = json.loads(capsys.readouterr().out)
    assert code == 0
    assert run["status"] == "succeeded"
    assert run["data"] == {"text": "AB"}
    assert run["steps"][0]["attempts"][0]["status"] == "succeeded"


def test_waits_for_runs_from_on_start(
    project: tuple[Path, list[str]], capsys: pytest.CaptureFixture[str]
) -> None:
    spec, targets = project
    assert main(["-f", str(spec), *targets]) == 0
    [run] = json.loads(capsys.readouterr().out)
    assert run["data"] == {"text": ">AB"}
    assert run["trigger"]["kind"] == "source"


def test_trigger_and_failure_exit_code(
    project: tuple[Path, list[str]], capsys: pytest.CaptureFixture[str]
) -> None:
    spec, targets = project
    out = spec.parent / "runs.json"
    args = ["-f", str(spec), *targets, "--no-module-schedules", "--trigger", "p", "-o", str(out)]
    assert main(args) == 0
    assert capsys.readouterr().out == ""
    [run] = json.loads(out.read_text(encoding="utf-8"))
    assert run["trigger"]["kind"] == "source"
    # without module b's code the memory broker refuses to start
    with pytest.raises(ValueError, match="no code for: b"):
        main(["-f", str(spec), targets[0], targets[2], "--run", "p"])

from __future__ import annotations

import json
from pathlib import Path

import pytest

from manufacture.cli import load_module, main

MODULE = """
from pydantic import BaseModel
from manufacture import Context, Module

class Text(BaseModel):
    text: str

app = Module("hello-exclaim", version="0.1.0")

@app.step(input=Text, output=Text)
async def handle(ctx: Context, inp: Text) -> Text:
    return Text(text=inp.text + "!")
"""


@pytest.fixture
def module_file(tmp_path: Path) -> Path:
    path = tmp_path / "exclaim_main.py"
    path.write_text(MODULE)
    return path


def test_manifest(module_file: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["manifest", str(module_file)]) == 0
    doc = json.loads(capsys.readouterr().out)
    assert doc["module"] == "hello-exclaim"
    assert doc["step"]["output_schema"]["title"] == "Text"


def test_test_command(
    module_file: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    inp = tmp_path / "in.json"
    inp.write_text('{"text": "hello world"}')
    assert main(["test", f"{module_file}:app", "--input", str(inp)]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["type"] == "step.succeeded"
    assert out["output"] == {"text": "hello world!"}

    inp.write_text('{"wrong": 1}')
    assert main(["test", str(module_file), "--input", str(inp)]) == 1
    assert json.loads(capsys.readouterr().out)["code"] == "input_invalid"


def test_load_module_errors(module_file: Path) -> None:
    with pytest.raises(SystemExit, match=r"not a manufacture\.Module"):
        load_module(f"{module_file}:Text")

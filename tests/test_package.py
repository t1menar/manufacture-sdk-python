from __future__ import annotations

import re
from importlib import resources

import manufacture


def test_version_is_pep440() -> None:
    assert re.fullmatch(r"\d+\.\d+\.\d+((a|b|rc)\d+)?(\.dev\d+)?", manufacture.__version__)


def test_package_is_typed() -> None:
    assert resources.files("manufacture").joinpath("py.typed").is_file()


def test_contract_is_shipped_in_the_package() -> None:
    root = resources.files("manufacture").joinpath("_contract", "schemas", "v1")
    assert root.joinpath("module.schema.json").is_file()
    assert root.joinpath("pipeline.schema.json").is_file()

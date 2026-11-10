from __future__ import annotations

import pytest
from pydantic import BaseModel, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

from manufacture import Context, Fatal, Module, RuntimeSettings, Secrets
from manufacture.testing import call_step
from tests.unit.conftest import Text


def test_secrets_are_hidden_and_required() -> None:
    secrets = Secrets({"TOKEN": "s3cr3t"})
    token = secrets.get("TOKEN")
    assert isinstance(token, SecretStr)
    assert "s3cr3t" not in repr(token)
    assert token.get_secret_value() == "s3cr3t"
    assert secrets.get("NOPE") is None
    with pytest.raises(Fatal) as exc:
        secrets.require("NOPE")
    assert exc.value.code == "secret_missing"


def test_runtime_settings_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MF_AMQP_URL", "amqp://u:p@rmq:5672/")
    monkeypatch.setenv("MF_CONCURRENCY", "8")
    monkeypatch.setenv("MF_HEALTH_ADDR", "127.0.0.1:9000")
    monkeypatch.setenv("MF_MODULE_NAME", "hello-hello")
    s = RuntimeSettings()
    assert s.amqp_url.get_secret_value() == "amqp://u:p@rmq:5672/"
    assert "u:p" not in repr(s)
    assert s.concurrency == 8
    assert s.health == ("127.0.0.1", 9000)
    assert s.module_name == "hello-hello"
    assert s.replica_id is None
    assert s.replica_for("hello-hello").startswith("hello-hello-")


@pytest.mark.parametrize(
    ("addr", "parsed"),
    [(":8081", ("0.0.0.0", 8081)), ("[::]:80", ("::", 80)), ("", None), ("off", None)],
)
def test_health_addr(addr: str, parsed: tuple[str, int] | None) -> None:
    assert RuntimeSettings(health_addr=addr).health == parsed


def test_bad_health_addr() -> None:
    with pytest.raises(ValueError, match="MF_HEALTH_ADDR"):
        RuntimeSettings(health_addr="8081")


async def test_module_settings_are_typed_and_loaded_from_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Settings(BaseSettings):
        model_config = SettingsConfigDict(env_prefix="HELLO_")
        suffix: str = "?"

    monkeypatch.setenv("HELLO_SUFFIX", " world")
    app = Module("hello-world", settings=Settings)

    @app.step(input=Text, output=Text)
    async def handle(ctx: Context[Settings], inp: Text) -> Text:
        return Text(text=inp.text + ctx.settings.suffix)

    assert (await call_step(app, {"text": "hello"})).output == {"text": "hello world"}


def test_invalid_module_settings_fail_fast() -> None:
    class Required(BaseModel):
        needed: int

    app = Module("needs", settings=Required)
    with pytest.raises(ValueError, match="needed"):
        _ = app.settings

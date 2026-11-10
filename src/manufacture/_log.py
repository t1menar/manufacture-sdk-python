"""Small structured logger: ``log.info("event", key=value)`` with bound step fields."""

from __future__ import annotations

import json
import logging
import sys
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Final, Literal

if TYPE_CHECKING:
    from collections.abc import Mapping

__all__ = ["StructLogger", "configure_logging", "get_logger"]

_FIELDS: Final = "mf_fields"


class StructLogger:
    """Wraps :class:`logging.Logger`; keyword arguments become structured fields."""

    __slots__ = ("_fields", "_logger")

    def __init__(self, logger: logging.Logger, fields: Mapping[str, object] | None = None) -> None:
        self._logger = logger
        self._fields: dict[str, object] = dict(fields or {})

    def bind(self, **fields: object) -> StructLogger:
        return StructLogger(self._logger, {**self._fields, **fields})

    def _log(self, level: int, event: str, fields: dict[str, object], exc: bool = False) -> None:
        if self._logger.isEnabledFor(level):
            self._logger.log(
                level,
                event,
                exc_info=exc,
                extra={_FIELDS: {**self._fields, **fields}},
                stacklevel=3,
            )

    def debug(self, event: str, **fields: object) -> None:
        self._log(logging.DEBUG, event, fields)

    def info(self, event: str, **fields: object) -> None:
        self._log(logging.INFO, event, fields)

    def warning(self, event: str, **fields: object) -> None:
        self._log(logging.WARNING, event, fields)

    def error(self, event: str, **fields: object) -> None:
        self._log(logging.ERROR, event, fields)

    def exception(self, event: str, **fields: object) -> None:
        self._log(logging.ERROR, event, fields, exc=True)


def get_logger(name: str = "manufacture", **fields: object) -> StructLogger:
    return StructLogger(logging.getLogger(name), fields)


class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        doc: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, UTC).isoformat(timespec="milliseconds"),
            "level": record.levelname.lower(),
            "logger": record.name,
            "msg": record.getMessage(),
        }
        doc.update(getattr(record, _FIELDS, {}))
        if record.exc_info:
            doc["exc"] = self.formatException(record.exc_info)
        return json.dumps(doc, default=str, ensure_ascii=False)


class _TextFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        base = super().format(record)
        fields = getattr(record, _FIELDS, {})
        if fields:
            base += " " + " ".join(f"{k}={v}" for k, v in fields.items())
        return base


def configure_logging(fmt: Literal["json", "text"] = "json", level: str = "INFO") -> None:
    """Configure the root logger for a module process (called by ``Module.run``)."""
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(
        _JsonFormatter()
        if fmt == "json"
        else _TextFormatter("%(asctime)s %(levelname)s %(message)s")
    )
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level.upper())
    for noisy in ("aio_pika", "aiormq"):
        logging.getLogger(noisy).setLevel(max(logging.WARNING, root.level))

"""Spec loading and the subset of the pipeline language the fake core executes.

Every document is checked against the vendored contract schemas first, exactly like
``manufacture validate`` at schema level. Then the fake core either builds a plan or
refuses with :class:`UnsupportedSpecError` naming the feature: it never silently runs a
pipeline differently from the core.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final

from jsonschema import Draft202012Validator

from manufacture.app import parse_duration
from manufacture.contract import ResourceError, SpecError, validate_resource

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

__all__ = [
    "ModulePlan",
    "PipelinePlan",
    "RetryPlan",
    "SpecSet",
    "StepPlan",
    "UnsupportedSpecError",
    "load_documents",
]

# Fake core defaults where the spec leaves a value out. The core has not published its
# own defaults for 0.2.1 yet (see ADR 0004, questions); these are deliberately visible.
DEFAULT_TIMEOUT_S: Final = 3600.0
DEFAULT_LEASE_S: Final = 30.0
DEFAULT_MAX_ATTEMPTS: Final = 3
"""Used when a ``retry`` block is present without ``max_attempts``."""
DEFAULT_RETRY_INITIAL_S: Final = 1.0
DEFAULT_RETRY_MAX_S: Final = 300.0
DEFAULT_RETRY_ON: Final = ("transient", "timeout", "lost")


class UnsupportedSpecError(ValueError):
    """The spec is valid, but uses a feature the fake core does not execute."""

    def __init__(self, problems: list[str]) -> None:
        self.problems = problems
        super().__init__("fake core cannot run this spec:\n  " + "\n  ".join(problems))


@dataclass(frozen=True, slots=True)
class RetryPlan:
    max_attempts: int = 1
    backoff: str = "exponential"
    initial_s: float = DEFAULT_RETRY_INITIAL_S
    max_s: float = DEFAULT_RETRY_MAX_S
    jitter: bool = True
    on_errors: tuple[str, ...] = DEFAULT_RETRY_ON

    def retries(self, error_class: str) -> bool:
        return error_class in self.on_errors

    def delay(self, failed_attempt: int) -> float:
        """Delay before the attempt after ``failed_attempt`` (1-based), without jitter."""
        if self.backoff == "fixed":
            return min(self.initial_s, self.max_s)
        return min(self.initial_s * 2.0 ** (failed_attempt - 1), self.max_s)


@dataclass(frozen=True, slots=True)
class StepPlan:
    stage: str
    stage_index: int
    step_id: str
    module: str
    config: Mapping[str, Any]
    timeout_s: float
    lease_s: int
    retry: RetryPlan
    merge: bool
    optional: bool


@dataclass(frozen=True, slots=True)
class PipelinePlan:
    name: str
    revision: int
    source: str | None
    max_runs: int | None
    context_schema: Mapping[str, Any] | None
    steps: tuple[StepPlan, ...]
    doc: Mapping[str, Any] = field(repr=False)

    def check_data(self, data: Mapping[str, Any]) -> list[str]:
        """Violations of ``spec.context.schema``; empty when the data fits."""
        if self.context_schema is None:
            return []
        validator = Draft202012Validator(self.context_schema)
        return [
            f"{'.'.join(map(str, e.absolute_path)) or '<data>'}: {e.message}"
            for e in sorted(validator.iter_errors(data), key=lambda e: list(map(str, e.path)))
        ]


@dataclass(frozen=True, slots=True)
class ModulePlan:
    name: str
    replicas: int
    concurrency: int | None
    config: Mapping[str, Any]
    runtime: str
    doc: Mapping[str, Any] = field(repr=False)


@dataclass(frozen=True, slots=True)
class SpecSet:
    modules: Mapping[str, ModulePlan]
    pipelines: Mapping[str, PipelinePlan]


def _read(source: str | Path) -> list[Any]:
    path = Path(source)
    text = path.read_text(encoding="utf-8")
    if path.suffix == ".json":
        loaded = json.loads(text)
        return loaded if isinstance(loaded, list) else [loaded]
    try:
        import yaml  # noqa: PLC0415 (optional dependency)
    except ImportError as exc:  # pragma: no cover - depends on the environment
        raise ImportError(
            "reading YAML specs needs PyYAML: pip install 'manufacture-sdk[fakecore]'"
        ) from exc
    return [d for d in yaml.safe_load_all(text) if d is not None]


def load_documents(
    spec: str | Path | Mapping[str, Any] | Iterable[str | Path | Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Documents from YAML/JSON files, mappings, or a mix of both."""
    items: Iterable[Any] = [spec] if isinstance(spec, str | Path | dict) else spec
    docs: list[dict[str, Any]] = []
    for item in items:
        if isinstance(item, str | Path):
            docs.extend(_read(item))
        else:
            docs.append(dict(item))
    return docs


def _seconds(value: str | float | None, default: float) -> float:
    return default if value is None else parse_duration(value)


def _retry(raw: Mapping[str, Any] | None) -> RetryPlan:
    if raw is None:
        return RetryPlan()
    return RetryPlan(
        max_attempts=int(raw.get("max_attempts", DEFAULT_MAX_ATTEMPTS)),
        backoff=str(raw.get("backoff", "exponential")),
        initial_s=_seconds(raw.get("initial"), DEFAULT_RETRY_INITIAL_S),
        max_s=_seconds(raw.get("max"), DEFAULT_RETRY_MAX_S),
        jitter=bool(raw.get("jitter", True)),
        on_errors=tuple(raw.get("on_errors", DEFAULT_RETRY_ON)),
    )


def _plan_module(doc: Mapping[str, Any]) -> ModulePlan:
    spec = doc.get("spec", {})
    replicas = spec.get("replicas", 1)
    if isinstance(replicas, dict):
        replicas = max(replicas.get("min", 1), 1)
    return ModulePlan(
        name=doc["metadata"]["name"],
        replicas=replicas,
        concurrency=spec.get("concurrency"),
        config=spec.get("config", {}),
        runtime=spec.get("runtime", "auto"),
        doc=doc,
    )


_UNSUPPORTED_STAGE: Final = ("parallel", "foreach", "routes", "when", "join", "max_parallel")


def _plan_pipeline(
    doc: Mapping[str, Any], modules: Mapping[str, ModulePlan], problems: list[str]
) -> PipelinePlan:
    name = doc["metadata"]["name"]
    spec = doc["spec"]
    defaults = spec.get("defaults", {})
    trigger = spec.get("trigger", {})
    if "schedule" in trigger:
        problems.append(f"pipeline {name}: trigger.schedule (start runs explicitly in tests)")
    steps: list[StepPlan] = []
    for i, stage in enumerate(spec["stages"]):
        where = f"pipeline {name}: stages[{i}] ({stage['name']})"
        problems.extend(f"{where}: {key}" for key in _UNSUPPORTED_STAGE if key in stage)
        step = stage.get("step")
        if step is None:
            continue
        problems.extend(f"{where}: step.{key}" for key in ("input", "when") if key in step)
        module = step["module"]
        if module not in modules:
            problems.append(f"{where}: module {module} is not in the spec")
        timeout = _seconds(step.get("timeout", defaults.get("timeout")), DEFAULT_TIMEOUT_S)
        lease = _seconds(
            step.get("lease", defaults.get("lease")), min(DEFAULT_LEASE_S, timeout / 2)
        )
        if lease >= timeout and ("lease" in step or "lease" in defaults):
            problems.append(f"{where}: lease must be shorter than timeout")
        base = dict(modules[module].config) if module in modules else {}
        steps.append(
            StepPlan(
                stage=stage["name"],
                stage_index=i,
                step_id=step.get("id", stage["name"]),
                module=module,
                config={**base, **step.get("config", {})},
                timeout_s=timeout,
                lease_s=max(1, round(lease)),
                retry=_retry(step.get("retry", defaults.get("retry"))),
                merge=step.get("merge", "data") == "data",
                optional=step.get("optional", False),
            )
        )
    source = trigger.get("source")
    if source is not None and source not in modules:
        problems.append(f"pipeline {name}: trigger.source module {source} is not in the spec")
    return PipelinePlan(
        name=name,
        revision=1,
        source=source,
        max_runs=spec.get("concurrency", {}).get("max_runs"),
        context_schema=spec.get("context", {}).get("schema"),
        steps=tuple(steps),
        doc=doc,
    )


def plan(docs: list[dict[str, Any]]) -> SpecSet:
    """Validate ``docs`` and build the execution plan; raise on any problem."""
    errors: list[ResourceError] = []
    for i, doc in enumerate(docs):
        errors.extend(
            ResourceError(f"documents[{i}].{e.path}" if e.path else f"documents[{i}]", e.message)
            for e in validate_resource(doc)
        )
    if errors:
        raise SpecError(errors)
    modules = {d["metadata"]["name"]: _plan_module(d) for d in docs if d["kind"] == "Module"}
    problems: list[str] = []
    pipelines = {
        d["metadata"]["name"]: _plan_pipeline(d, modules, problems)
        for d in docs
        if d["kind"] == "Pipeline"
    }
    if problems:
        raise UnsupportedSpecError(problems)
    return SpecSet(modules=modules, pipelines=pipelines)

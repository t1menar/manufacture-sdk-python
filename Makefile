# manufacture-sdk-python. Run `make help` for the targets.
UV  ?= uv
REF ?=

.DEFAULT_GOAL := help
.PHONY: help sync lint fmt typecheck test test-online check contract-sync contract-check build

help: ## show this help
	@awk 'BEGIN {FS = ":.*## "} /^[a-zA-Z_-]+:.*## / {printf "  %-16s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

sync: ## install the package and dev tools into .venv
	$(UV) sync --locked

lint: ## ruff lint + format check
	$(UV) run ruff check .
	$(UV) run ruff format --check .

fmt: ## format and autofix
	$(UV) run ruff format .
	$(UV) run ruff check --fix .

typecheck: ## mypy --strict
	$(UV) run mypy

test: ## unit tests (offline)
	$(UV) run pytest

test-online: ## tests incl. the drift test against GitHub
	MF_CONTRACT_ONLINE=1 $(UV) run pytest tests/contract

check: lint typecheck test ## everything CI runs offline

contract-sync: ## vendor the contract: make contract-sync REF=<core commit sha>
	@test -n "$(REF)" || (echo "usage: make contract-sync REF=<core commit sha>" && exit 2)
	python3 scripts/contract_sync.py --ref $(REF)

contract-check: ## compare the vendored contract with the pinned commit (REF= to compare with another)
	python3 scripts/contract_sync.py --check $(if $(REF),--ref $(REF),)

build: ## build wheel and sdist into dist/
	$(UV) build

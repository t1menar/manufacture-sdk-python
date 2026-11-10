# manufacture-sdk-python. Run `make help` for the targets.
UV  ?= uv
RELEASE ?=
REF ?=

.DEFAULT_GOAL := help
.PHONY: help sync lint fmt typecheck test test-online check contract-sync contract-check build test-integration

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

test-integration: ## runtime tests on a real RabbitMQ: make test-integration MF_TEST_AMQP_URL=amqp://...
	@test -n "$(MF_TEST_AMQP_URL)" || (echo "set MF_TEST_AMQP_URL=amqp://guest:guest@127.0.0.1:5672/" && exit 2)
	MF_TEST_AMQP_URL=$(MF_TEST_AMQP_URL) $(UV) run pytest tests/integration -m integration

check: lint typecheck test ## everything CI runs offline

contract-sync: ## vendor a contract release: make contract-sync RELEASE=contract-vX.Y.Z
	@test -n "$(RELEASE)" || (echo "usage: make contract-sync RELEASE=contract-vX.Y.Z" && exit 2)
	python3 scripts/contract_sync.py --release $(RELEASE)

contract-check: ## compare with the pinned release (REF=main to compare with a core branch)
	python3 scripts/contract_sync.py --check $(if $(REF),--ref $(REF),)

build: ## build wheel and sdist into dist/
	$(UV) build

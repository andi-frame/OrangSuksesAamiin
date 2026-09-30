# One entry point for build, test and every demo scenario.  `make help` lists targets.
SHELL := bash
.DEFAULT_GOAL := help
# every Go module in the repo (a directory containing go.mod)
GO_MODULES := $(patsubst %/go.mod,%,$(wildcard pkg/*/go.mod services/*/go.mod tools/*/go.mod))

# Windows + MSYS make hands recipes a stripped environment (no USERPROFILE/TEMP/LOCALAPPDATA), which breaks
# native go and docker. scripts/with-env.sh restores it when needed and is a no-op on Linux/macOS/CI.
RUN := bash $(CURDIR)/scripts/with-env.sh
GO := GOWORK=off $(RUN) go
COMPOSE ?= $(RUN) docker compose

.PHONY: help env up down wipe reset ps logs wait test test-race vet fmt lint secrets pvmbg-token \
        demo-p1 demo-p2 demo-p2-aggregator demo-mock-auth demo-p3 demo-p4 demo-p5 demo-isolation evidence test-postgres

help: ## show this help
	@grep -E '^[a-zA-Z0-9_-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*## "}{printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

env: ## create .env from .env.example if missing
	@test -f .env || { cp .env.example .env && echo "created .env from .env.example"; }

up: env ## build and start the whole system, wait until it is healthy
	$(COMPOSE) up -d --build
	@$(RUN) bash scripts/wait-healthy.sh 180

down: ## stop the system (data volumes are kept)
	$(COMPOSE) down

wipe: env
	$(COMPOSE) down -v --remove-orphans

reset: wipe up ## wipe containers AND volumes, rebuild, start from a pristine state

ps: ## container status
	$(COMPOSE) ps -a

logs: ## follow all logs (Ctrl-C to stop)
	$(COMPOSE) logs -f --tail=50

wait: ## wait until every HTTP service answers /health
	@$(RUN) bash scripts/wait-healthy.sh 120

# ---------------------------------------------------------------- quality ---
test: ## unit + integration tests of every Go module
	@set -e; for m in $(GO_MODULES); do echo "== $$m"; (cd $$m && $(GO) test -count=1 ./...); done

test-race: ## same, with the race detector
	@set -e; for m in $(GO_MODULES); do echo "== $$m"; (cd $$m && $(GO) test -count=1 -race ./...); done

test-postgres: ## Postgres adapter vs the repotest contract (throwaway container)
	@$(RUN) bash scripts/test-postgres.sh

vet: ## go vet on every module
	@set -e; for m in $(GO_MODULES); do echo "== $$m"; (cd $$m && $(GO) vet ./...); done

fmt: ## gofmt -l (fails when a file is not formatted)
	@out="$$(gofmt -l $(GO_MODULES))"; if [ -n "$$out" ]; then echo "not formatted:"; echo "$$out"; exit 1; fi

lint: fmt vet ## fmt + vet

secrets: ## scan the repo (incl. git history) for committed credentials
	@bash scripts/check-secrets.sh

pvmbg-token: ## mint a PVMBG gateway token from PVMBG_TOKEN_SECRET in .env
	@set -a; . ./.env; set +a; cd tools/pvmbg-token && $(GO) run .

# ------------------------------------------------------------------ demos ---
# Each demo is self-checking (exit code != 0 on failure) and writes its
# transcript to docs/evidence/pN/latest.log for the report.
demo-p1: ## P1 schema evolution: confidence_level appears live, no restart
	@mkdir -p docs/evidence/p1
	@bash -o pipefail -c 'bash scripts/demo/p1.sh 2>&1 | tee docs/evidence/p1/latest.log'

demo-p2: ## P2 concurrency / availability / graceful degradation
	@mkdir -p docs/evidence/p2
	@bash -o pipefail -c 'bash scripts/demo/p2.sh 2>&1 | tee docs/evidence/p2/latest.log'

demo-p2-aggregator: ## P2 ingest side: slow/dead PVMBG vs BMKG, stale_since, recovery without restart
	@mkdir -p docs/evidence/p2
	@bash -o pipefail -c 'bash scripts/demo/p2-aggregator.sh 2>&1 | tee docs/evidence/p2/aggregator-latest.log'

demo-mock-auth: ## credentials are per agency: cross-agency credentials rejected, secrets isolated per container
	@mkdir -p docs/evidence/p3
	@bash -o pipefail -c 'bash scripts/demo/mock-auth.sh 2>&1 | tee docs/evidence/p3/mock-auth.log'

demo-p3: ## P3 service-to-service auth and trust
	@mkdir -p docs/evidence/p3
	@bash -o pipefail -c 'bash scripts/demo/p3.sh 2>&1 | tee docs/evidence/p3/latest.log'

demo-p4: ## P4 independent services + flexible storage
	@mkdir -p docs/evidence/p4
	@bash -o pipefail -c 'bash scripts/demo/p4.sh 2>&1 | tee docs/evidence/p4/latest.log'

demo-p5: ## P5 pub/sub fan-out
	@mkdir -p docs/evidence/p5
	@bash -o pipefail -c 'bash scripts/demo/p5.sh 2>&1 | tee docs/evidence/p5/latest.log'

demo-isolation: ## prove the Canonical Store / broker are unreachable from other containers
	@mkdir -p docs/evidence/p4
	@bash -o pipefail -c 'bash scripts/demo/isolation.sh 2>&1 | tee docs/evidence/p4/isolation.log'

evidence: demo-p1 demo-p2 demo-p3 demo-p4 demo-p5 ## run every demo and collect the transcripts

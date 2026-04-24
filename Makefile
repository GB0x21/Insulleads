.DEFAULT_GOAL := help
.PHONY: help logs test docker-test stop build up install setup run admin migrate shell dashboard crawl discover-contracts analyze-contract

help:
	@perl -nle'print $$& if m{^[a-zA-Z_-]+:.*?## .*$$}' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-18s\033[0m %s\n", $$1, $$2}'

install: ## install Python dependencies (local dev)
	pip install -U pip
	pip install -r requirements/local.txt

setup: install ## install deps + migrate + bootstrap CRM
	python manage.py migrate --no-input
	python manage.py setup_crm

run: ## run the outreach daemon (interactive onboarding on first run)
	python manage.py rundaemon

admin: ## start the Django Admin / CRM web server on :8000
	@echo ""
	@echo "  Django Admin: http://localhost:8000/admin/"
	@echo "  CRM:          http://localhost:8000/"
	@echo "  No superuser? Run: python manage.py createsuperuser"
	@echo ""
	python manage.py runserver 0.0.0.0:8000

migrate: ## apply database migrations
	python manage.py makemigrations outreach crm
	python manage.py migrate

shell: ## open a Django shell
	python manage.py shell

dashboard: ## run the Streamlit MVP dashboard on :8501
	streamlit run dashboards/streamlit_app.py

crawl: ## run a Scrapy spider in isolation (usage: make crawl SPIDER=cslb_contractors ARGS='-a license_numbers=123456')
	@test -n "$(SPIDER)" || (echo "Usage: make crawl SPIDER=<spider_name> [ARGS='...']" && exit 1)
	scrapy crawl $(SPIDER) $(ARGS)

discover-contracts: ## discover open bids via configured contract_bids Sources (pass SRC=<key> for one source)
	@if [ -n "$(SRC)" ]; then \
		python manage.py discover_contracts --source $(SRC); \
	else \
		python manage.py discover_contracts; \
	fi

analyze-contract: ## analyze a single contract (ID=<pk>) or every pending row (PENDING=1)
	@if [ -n "$(ID)" ]; then \
		python manage.py analyze_contract --contract-id $(ID) $(if $(FORCE),--force,); \
	else \
		python manage.py analyze_contract --pending; \
	fi

test: ## run the test suite
	pytest -q

# ─── Docker targets ─────────────────────────────────────────────
logs: ## follow the docker compose logs
	docker compose -f local.yml logs -f

stop: ## stop docker compose services
	docker compose -f local.yml stop

build: ## build docker compose services
	docker compose -f local.yml build

up: ## run docker compose (rebuilds + follows logs)
	docker compose -f local.yml up --build -d
	docker compose -f local.yml logs -f

docker-test: ## run the test suite inside docker
	docker compose -f local.yml run --rm app pytest -q

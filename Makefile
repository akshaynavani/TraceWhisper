.PHONY: up down logs venv test baseline chaos-list chaos investigate mcp clean

PY ?= python3
VENV ?= .venv
BIN := $(VENV)/bin
SCENARIO ?= payment-outage

up:            ## build and start the shop + observability stack
	docker compose up -d --build

down:          ## stop everything (keeps volumes)
	docker compose down

logs:
	docker compose logs -f catalog checkout orders

venv:          ## local env for the analyzer, agent, MCP server and load tools
	$(PY) -m venv $(VENV)
	$(BIN)/pip install -U pip
	$(BIN)/pip install -e "./tracewhisper[dev]"

test:          ## unit tests (no Docker, no API key)
	cd tracewhisper && ../$(BIN)/pytest -q

baseline:      ## 60s of healthy traffic so analyze_trace has a baseline
	$(BIN)/python -m loadgen.load --rps 8 --duration 60

chaos-list:
	$(BIN)/python -m loadgen.chaos list

chaos:         ## make chaos SCENARIO=slow-catalog-db
	$(BIN)/python -m loadgen.chaos run $(SCENARIO)

investigate:   ## chaos scenario followed by an LLM investigation (needs ANTHROPIC_API_KEY)
	$(BIN)/python -m loadgen.chaos run $(SCENARIO) --investigate

mcp:           ## run the MCP server on stdio
	$(BIN)/tracewhisper serve-mcp

clean:
	docker compose down -v
	rm -rf $(VENV)

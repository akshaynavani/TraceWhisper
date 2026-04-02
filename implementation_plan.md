# TraceWhisper — Implementation Plan

AI-assisted root-cause analysis for microservices. A small e-commerce backend
(catalog, checkout, orders) is instrumented end-to-end with OpenTelemetry, traces
land in Jaeger, metrics in Prometheus/Grafana, and an MCP server exposes the
telemetry to an LLM agent that correlates spans across service boundaries and
writes a root-cause summary.

## Key design decisions

| Concern | Decision |
|---|---|
| Language | Python 3.11 everywhere (services, analyzer, MCP server) |
| Services | FastAPI + uvicorn; SQLite for per-service storage (keeps the demo self-contained) |
| Messaging | Apache Kafka (KRaft, single broker) via `aiokafka`; checkout publishes `orders.placed`, orders consumes it |
| Tracing | OpenTelemetry SDK -> OTLP -> OTel Collector -> Jaeger. W3C trace context is injected into Kafka headers so a trace spans the async hop |
| Metrics | `prometheus_client` `/metrics` on each service + collector `spanmetrics` connector; Grafana provisioned with a dashboard |
| Failure modes | Runtime fault injection (`/admin/faults`) per service: latency, error rate, dependency failure — so RCA has something real to find |
| Analysis | Deterministic correlation layer first (span tree, critical path, error origin, cross-service edges, latency vs baseline), then the LLM reasons over those compact findings instead of raw span dumps |
| LLM | Anthropic Claude (`claude-opus-5-5`) through the official `anthropic` SDK, manual tool-use loop |
| MCP | `mcp` Python SDK (FastMCP) server exposing Jaeger/Prometheus/correlation tools; the agent is an MCP client over stdio, so the same server also plugs into Claude Desktop / Claude Code |

## Phases

### Phase 1 — E-commerce services skeleton
Deliverables: shared library, three FastAPI services with business logic, Kafka wiring, Dockerfile, docker-compose for the app + Kafka.
Files:
- `services/shopcommon/{__init__,config,db,faults,kafka}.py`, `services/shopcommon/pyproject.toml`
- `services/catalog/app.py`, `services/checkout/app.py`, `services/orders/app.py`
- `services/Dockerfile`, `services/requirements.txt`
- `docker-compose.yml`, `.env.example`

### Phase 2 — End-to-end observability
Deliverables: OTel tracing (FastAPI, httpx, SQLite spans, Kafka producer/consumer with context propagation), Prometheus metrics, OTel Collector, Jaeger, Prometheus, Grafana provisioning + dashboard.
Files:
- `services/shopcommon/telemetry.py`, `services/shopcommon/metrics.py`, updates to services + `kafka.py`
- `observability/otel-collector.yaml`, `observability/prometheus.yml`
- `observability/grafana/provisioning/{datasources,dashboards}/*.yaml`, `observability/grafana/dashboards/shop.json`
- `docker-compose.yml` (observability stack)

### Phase 3 — Trace correlation engine
Deliverables: `tracewhisper` package with Jaeger + Prometheus clients, trace model, correlation (span tree, error origin, critical path, service dependency edges, baseline latency comparison) and a compact findings report.
Files:
- `tracewhisper/pyproject.toml`, `tracewhisper/tracewhisper/{__init__,config,models,jaeger,prometheus,correlate,report}.py`

### Phase 4 — MCP server + LLM root-cause agent
Deliverables: FastMCP server with tools (`list_services`, `find_traces`, `analyze_trace`, `compare_to_baseline`, `service_metrics`, `service_dependencies`); Claude agent that connects to the MCP server over stdio, runs an investigation loop and emits a structured root-cause summary; CLI (`tracewhisper investigate|analyze|serve-mcp`).
Files:
- `tracewhisper/tracewhisper/{mcp_server,agent,prompts,cli}.py`
- `.env.example` (ANTHROPIC_API_KEY etc.)

### Phase 5 — Load, chaos scenarios and tests
Deliverables: async load generator, chaos scenario runner (slow catalog DB, payment gateway errors, orders consumer failure), unit tests over recorded Jaeger fixtures, Makefile.
Files:
- `loadgen/{load,chaos}.py`
- `tracewhisper/tests/{conftest,test_models,test_correlate,test_report,test_agent_tools}.py`, `tracewhisper/tests/fixtures/*.json`
- `Makefile`

Then: `.gitignore`, `README.md` with architecture diagram, run + test instructions.

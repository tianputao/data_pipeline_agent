# Data Pipeline Ingestion Agent

Config-driven ingestion agent that turns natural language or YAML/JSON specs into runnable PySpark ETL jobs on Azure Databricks. The agent is designed to plug into the Microsoft Agent Framework as a tool/agent node while remaining usable from a CLI.

## Layout
- `src/ingestion_agent/core`: settings, config models, LLM client, Databricks client, Jinja2 renderer
- `src/ingestion_agent/agent`: orchestration service and Microsoft Agent Framework adapter
- `src/ingestion_agent/ui`: Streamlit front-end (optional)
- `src/ingestion_agent/templates`: PySpark generation templates
- `src/ingestion_agent/examples`: starter configs

## Quick start
1) Environment  
   - Copy `.env.example` to `.env` and fill Azure OpenAI + Databricks credentials (LLM is only needed for natural-language input; YAML/JSON configs work offline).  
   - Optional: set `LOG_LEVEL=DEBUG` while iterating.

2) Install  
   - CLI/Core only: `pip install -e .`  
   - With Streamlit UI: `pip install -e .[ui]`

3) Sample config  
   - `python -m ingestion_agent.main sample-config --path examples/orders.yaml`

4) Generate/submit from CLI  
   - Render only: `python -m ingestion_agent.main generate --config examples/orders.yaml --render-only --output /tmp/orders.py`  
   - Submit to Databricks: `python -m ingestion_agent.main generate --config examples/orders.yaml --submit`  
   - Natural language: `python -m ingestion_agent.main generate --nl "From MySQL orders..." --render-only`

5) Streamlit UI  
   - Start: `streamlit run src/ingestion_agent/ui/streamlit_app.py`  
   - Choose “Natural language” or “YAML/JSON config”, select “Render only” or “Render + Submit to Databricks”, then click Generate.  
   - For NL mode you must have a working LLM endpoint in `.env`; YAML mode can run without LLM.

## Microsoft Agent Framework wiring
`ingestion_agent.agent.maf_adapter.MicrosoftAgentFrameworkAdapter` exposes a `build_agent()` helper that returns an Agent with a single `submit_ingestion_job` tool. The tool accepts either:
- Natural language (string)
- JSON payload compatible with `IngestionJobConfig`

If the Microsoft Agent Framework package is not installed, the adapter logs a warning and returns `None`; install the official SDK from the Microsoft repository and adjust imports if the package name changes.

## Configuration model
See `src/ingestion_agent/examples/orders.yaml` for the schema. Key fields:
- `source`: type (`postgres|mysql|sqlserver`), `jdbc_url`, `table`, `increment_field`, `frequency`
- `transformations`: select, rename, convert, aggregate(group_by, metrics)
- `sink`: type (`delta` default), `path` or `table`, `layer`, `mode`

## Notes
- Streaming (Kafka/Event Hubs) hooks are reserved; current template targets batch JDBC -> Delta.
- Secrets should be stored in Key Vault/Databricks Secret Scope; `.env` is only for local development.
- The generated PySpark script includes placeholders for incremental watermarks and can be extended for Silver/Gold curation logic.

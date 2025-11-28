from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Optional

import typer
import yaml
from rich import print as rprint

from ingestion_agent.agent.service import IngestionAgentService
from ingestion_agent.core.config_models import IngestionJobConfig, JobRequest
from ingestion_agent.core.settings import get_settings

app = typer.Typer(help="Ingestion agent CLI for generating/submitting Spark ETL jobs.")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")


def _load_config_from_file(path: Path) -> IngestionJobConfig:
    with path.open() as f:
        data = yaml.safe_load(f)
    return IngestionJobConfig.parse_obj(data)


@app.command()
def generate(
    nl: Optional[str] = typer.Option(None, "--nl", help="Natural language requirement."),
    config_path: Optional[Path] = typer.Option(None, "--config", help="Path to YAML/JSON config."),
    render_only: bool = typer.Option(False, "--render-only", help="Only render the script, do not submit."),
    submit: bool = typer.Option(False, "--submit", help="Submit to Databricks after rendering."),
    output: Optional[Path] = typer.Option(None, "--output", help="Where to write the generated script."),
):
    """Generate PySpark ingestion code (and optionally submit to Databricks)."""
    if not nl and not config_path:
        raise typer.BadParameter("Provide either --nl or --config")

    config = _load_config_from_file(config_path) if config_path else None
    settings = get_settings()
    service = IngestionAgentService(settings=settings)
    request = JobRequest(natural_language=nl, config=config, render_only=render_only)

    if submit:
        outcome = service.submit(request)
    else:
        outcome = service.plan(request)

    if output:
        output.write_text(outcome.script)
        rprint(f"[bold green]Script written to {output}")

    rprint({"job": outcome.job_config.job_name, "run_id": outcome.run_id, "script_path": outcome.script_path})
    if render_only or not submit:
        rprint("\n[bold]Preview (first 400 chars):[/bold]\n")
        rprint(outcome.script[:400])


@app.command()
def sample_config(path: Path = typer.Option("examples/orders.yaml", "--path", help="Where to write sample config")):
    """Write a starter YAML config for testing."""
    example = {
        "job_name": "extract_orders_mysql",
        "source": {
            "type": "mysql",
            "jdbc_url": "jdbc:mysql://mysql:3306/sales",
            "table": "orders",
            "increment_field": "updated_at",
            "frequency": "daily",
        },
        "transformations": {
            "select": ["order_id", "customer_id", "amount", "updated_at"],
            "rename": {"amount": "amount_usd", "updated_at": "ts"},
            "convert": {"amount_usd": "float"},
            "aggregate": {"group_by": ["customer_id"], "metrics": {"amount_usd": "sum"}},
        },
        "sink": {
            "type": "delta",
            "path": "abfss://bronze/delta/orders_daily",
            "layer": "gold",
            "mode": "append",
        },
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(example))
    rprint(f"[green]Sample config written to {path}")


if __name__ == "__main__":
    app()

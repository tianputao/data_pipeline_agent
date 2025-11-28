from __future__ import annotations

import logging
from pathlib import Path

from jinja2 import Environment, FileSystemLoader, StrictUndefined

from ingestion_agent.core.config_models import IngestionJobConfig

logger = logging.getLogger(__name__)


class SparkTemplateRenderer:
    """Render PySpark scripts from ingestion configs using Jinja2 templates."""

    def __init__(self, template_dir: Path | None = None):
        template_path = template_dir or Path(__file__).resolve().parent.parent / "templates"
        self.env = Environment(
            loader=FileSystemLoader(str(template_path)),
            autoescape=False,
            undefined=StrictUndefined,
            trim_blocks=True,
            lstrip_blocks=True,
        )

    def render_batch(self, config: IngestionJobConfig) -> str:
        template = self.env.get_template("batch_ingest.py.j2")
        logger.info("Rendering batch template for job %s", config.job_name)
        return template.render(config=config)

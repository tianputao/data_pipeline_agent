from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

from ingestion_agent.core.config_models import IngestionJobConfig, JobRequest
from ingestion_agent.core.databricks_client import DatabricksJobClient
from ingestion_agent.core.nlu_parser import NLUParser
from ingestion_agent.core.settings import Settings, get_settings
from ingestion_agent.core.template_renderer import SparkTemplateRenderer

logger = logging.getLogger(__name__)


@dataclass
class JobOutcome:
    job_config: IngestionJobConfig
    script: str
    run_id: Optional[str] = None
    script_path: Optional[str] = None


class IngestionAgentService:
    """Orchestrates NL parsing, code generation, and Databricks execution."""

    def __init__(self, settings: Optional[Settings] = None):
        self.settings = settings or get_settings()
        self.parser = NLUParser(self.settings)
        self.renderer = SparkTemplateRenderer()
        self._db_client: Optional[DatabricksJobClient] = None

    def _db(self) -> DatabricksJobClient:
        if not self._db_client:
            self._db_client = DatabricksJobClient(self.settings)
        return self._db_client

    def plan(self, request: JobRequest) -> JobOutcome:
        job_config = self.parser.parse_request(request)
        script = self.renderer.render_batch(job_config)
        return JobOutcome(job_config=job_config, script=script)

    def submit(self, request: JobRequest, persist_script: bool = True) -> JobOutcome:
        outcome = self.plan(request)
        path = None
        if persist_script:
            import time
            # Use a unique filename to avoid Databricks/DBFS caching issues
            script_name = f"{outcome.job_config.job_name}_{int(time.time())}"
            path = self._db().upload_script(outcome.script, script_name)
        run_id = self._db().submit_python_task(
            job_name=outcome.job_config.job_name,
            python_file=path or "",
            tags=outcome.job_config.tags,
        )
        outcome.run_id = run_id
        outcome.script_path = path
        return outcome

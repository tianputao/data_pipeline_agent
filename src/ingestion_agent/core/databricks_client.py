from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict, Optional

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs as jobs_svc

from ingestion_agent.core.settings import Settings

logger = logging.getLogger(__name__)


class DatabricksJobClient:
    """Thin wrapper around Databricks SDK for submitting Spark jobs."""

    def __init__(self, settings: Settings):
        if not settings.databricks_host or not settings.databricks_token:
            raise ValueError("Databricks host/token must be configured via environment variables")
        self.settings = settings
        self.client = WorkspaceClient(
            host=settings.databricks_host,
            token=settings.databricks_token,
        )

    def upload_script(self, code: str, job_name: str) -> str:
        """Upload generated code to DBFS and return the dbfs path."""
        folder = "dbfs:/FileStore/ingestion-agent"
        path = f"{folder}/{job_name}.py"
        logger.info("Uploading script to %s", path)
        self.client.dbfs.mkdirs(folder)
        # dbfs.put expects base64-encoded content as a string.
        import base64

        encoded = base64.b64encode(code.encode("utf-8")).decode("utf-8")
        self.client.dbfs.put(path=path, contents=encoded, overwrite=True)
        return path

    def submit_python_task(
        self,
        job_name: str,
        python_file: str,
        cluster_id: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None,
    ) -> str:
        """Submit a one-off run using jobs.submit with a SparkPythonTask."""
        cluster = cluster_id or self.settings.default_cluster_id
        if not cluster:
            raise ValueError("Cluster id required to submit job")

        task = jobs_svc.SubmitTask(
            task_key="ingest",
            existing_cluster_id=cluster,
            spark_python_task=jobs_svc.SparkPythonTask(python_file=python_file),
        )
        logger.info("Submitting Databricks run for job=%s on cluster=%s", job_name, cluster)
        run = self.client.jobs.submit(
            run_name=job_name,
            tasks=[task],
        )
        logger.info("Databricks run submitted: run_id=%s", run.run_id)
        return str(run.run_id)

    def get_run_status(self, run_id: str) -> str:
        run = self.client.jobs.get_run(run_id=run_id)
        return run.state.life_cycle_state

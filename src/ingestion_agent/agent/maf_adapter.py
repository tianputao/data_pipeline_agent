from __future__ import annotations

import json
import logging
from typing import Any, Callable, Dict, Optional

from ingestion_agent.agent.service import IngestionAgentService, JobOutcome
from ingestion_agent.core.config_models import IngestionJobConfig, JobRequest

logger = logging.getLogger(__name__)

try:
    # The Microsoft Agent Framework SDK is in preview; adjust imports if the package name changes.
    from microsoft.agent import Agent, Tool  # type: ignore

    HAS_MAF = True
except Exception:  # pragma: no cover - guard for environments without MAF
    HAS_MAF = False
    Agent = None  # type: ignore
    Tool = None  # type: ignore


def _build_submit_tool(service: IngestionAgentService) -> Callable[..., Dict[str, Any]]:
    def submit_tool(
        input: str,
        render_only: bool = False,
        **_: Any,
    ) -> Dict[str, Any]:
        """
        Tool callable compatible with MAF: takes either natural language or JSON config text.
        - If `input` parses as JSON, it is treated as a config payload matching IngestionJobConfig.
        - Otherwise it is treated as natural language instructions.
        """
        try:
            maybe_config = json.loads(input)
        except json.JSONDecodeError:
            maybe_config = None

        job_request = JobRequest(
            natural_language=None if isinstance(maybe_config, dict) else input,
            config=IngestionJobConfig.parse_obj(maybe_config) if isinstance(maybe_config, dict) else None,
            render_only=render_only,
        )
        if render_only:
            outcome: JobOutcome = service.plan(job_request)
        else:
            outcome = service.submit(job_request)

        return {
            "job_name": outcome.job_config.job_name,
            "run_id": outcome.run_id,
            "script_path": outcome.script_path,
            "script_preview": outcome.script[:4000],
        }

    return submit_tool


class MicrosoftAgentFrameworkAdapter:
    """Factory to expose this ingestion service as a Microsoft Agent Framework node/tool."""

    def __init__(self, service: Optional[IngestionAgentService] = None):
        self.service = service or IngestionAgentService()

    def build_agent(self) -> Optional[Any]:
        if not HAS_MAF:
            logger.warning("Microsoft Agent Framework SDK not installed; returning None")
            return None

        ingestion_tool = Tool(
            name="submit_ingestion_job",
            description="Generate PySpark code for ETL/ELT and submit it to Databricks. "
            "Input can be natural language or a JSON payload that matches the IngestionJobConfig schema.",
            func=_build_submit_tool(self.service),
        )
        return Agent(
            name="ingestion-agent",
            instructions=(
                "You are a data ingestion agent that turns user asks into runnable PySpark/Delta jobs on Databricks. "
                "Prefer incremental reads when possible; default sink is Delta Bronze/Silver/Gold as requested."
            ),
            tools=[ingestion_tool],
        )

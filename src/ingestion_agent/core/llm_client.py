from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional

import httpx
from openai import AzureOpenAI

from ingestion_agent.core.settings import Settings

logger = logging.getLogger(__name__)


class LLMClient:
    def __init__(self, settings: Settings):
        self.settings = settings
        self._client = None
        if settings.llm_provider == "azure_openai":
            self._client = AzureOpenAI(
                api_key=settings.azure_openai_key,
                azure_endpoint=settings.azure_openai_endpoint,
                api_version="2024-05-01-preview",
            )

    def chat_completion(self, messages: List[Dict[str, str]], response_format: Optional[Dict[str, Any]] = None) -> str:
        """Route to Azure OpenAI or a local OpenAI-compatible endpoint."""
        if self.settings.llm_provider == "azure_openai":
            response = self._client.chat.completions.create(
                model=self.settings.azure_openai_deployment,
                messages=messages,
                response_format=response_format,
            )
            return response.choices[0].message.content

        payload = {
            "model": self.settings.local_llm_model,
            "messages": messages,
        }
        if response_format:
            payload["response_format"] = response_format

        with httpx.Client(timeout=30.0) as client:
            resp = client.post(f"{self.settings.local_llm_endpoint}", json=payload)
            resp.raise_for_status()
            data = resp.json()
            return data["choices"][0]["message"]["content"]

    def nl_to_config(self, nl: str) -> Dict[str, Any]:
        """Convert natural language spec into an ingestion config dict via the LLM."""
        system = (
            "You are a data engineering assistant for Azure Databricks ETL. "
            "Convert user requirements into structured JSON with keys: job_name, source, transformations, sink. "
            "\n\n**CRITICAL VALIDATION RULES:**\n"
            "1. Source database MUST include: type (postgres/mysql/sqlserver), hostname/URL, database name, schema.table, username, password\n"
            "2. If ANY required source info is missing, return: {\"validation_error\": \"missing_fields\", \"missing\": [list of missing fields], \"prompt\": \"ask user for these fields\"}\n"
            "3. Recognize database types from keywords: 'pgsql'/'postgresql'->postgres, 'mysql', 'sql server'/'sqlserver', 'azure sql'->sqlserver\n"
            "4. Extract credentials from patterns: '用户名：xxx 密码：xxx', 'username: xxx password: xxx', 'user xxx pwd xxx'\n"
            "5. Parse hostname:port if provided, otherwise use defaults (postgres:5432, mysql:3306, sqlserver:1433)\n"
            "6. Split schema.table format: 'public.orders' -> schema='public', table='orders'\n"
            "7. Sink defaults: type='delta', catalog='uc_tarhone', mode='overwrite', layer='bronze'\n"
            "8. Sink MUST have database (schema) and table. If missing, ask user.\n"
            "9. Mode keywords: 'overwrite'/'覆盖'->overwrite, 'append'/'追加'->append\n"
            "\n**Return valid JSON only. No markdown, no explanations.**"
        )
        user = (
            f"User requirement:\n{nl}\n\n"
            "Extract all information. If critical fields are missing (source: hostname, database, table, username, password OR sink: schema, table), "
            "return validation_error JSON to prompt user."
        )
        content = self.chat_completion(
            messages=[{"role": "system", "content": system}, {"role": "user", "content": user}],
            response_format={"type": "json_object"},
        )
        try:
            return json.loads(content)
        except json.JSONDecodeError:
            logger.warning("LLM did not return JSON; wrapping as text")
            return {"_raw": content}

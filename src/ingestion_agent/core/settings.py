from __future__ import annotations

import os
from functools import lru_cache
from typing import Optional

from dotenv import load_dotenv
from pydantic import BaseModel


class Settings(BaseModel):
    azure_openai_endpoint: Optional[str] = None
    azure_openai_key: Optional[str] = None
    azure_openai_deployment: Optional[str] = None
    llm_provider: str = "azure_openai"  # azure_openai | local
    local_llm_endpoint: str = "http://localhost:8000/v1/chat/completions"
    local_llm_model: str = "qwen2.5"

    databricks_host: Optional[str] = None
    databricks_token: Optional[str] = None
    default_cluster_id: Optional[str] = None
    default_warehouse_id: Optional[str] = None
    default_unity_catalog: Optional[str] = None

    agent_storage_path: str = "/tmp/ingestion-agent"
    log_level: str = "INFO"

    class Config:
        allow_mutation = False


@lru_cache()
def get_settings() -> Settings:
    load_dotenv()
    return Settings(
        azure_openai_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
        azure_openai_key=os.getenv("AZURE_OPENAI_KEY"),
        azure_openai_deployment=os.getenv("AZURE_OPENAI_DEPLOYMENT"),
        llm_provider=os.getenv("LLM_PROVIDER", "azure_openai"),
        local_llm_endpoint=os.getenv("LOCAL_LLM_ENDPOINT", "http://localhost:8000/v1/chat/completions"),
        local_llm_model=os.getenv("LOCAL_LLM_MODEL", "qwen2.5"),
        databricks_host=os.getenv("AZURE_DATABRICKS_HOST"),
        databricks_token=os.getenv("AZURE_DATABRICKS_TOKEN"),
        default_cluster_id=os.getenv("DEFAULT_DATABRICKS_CLUSTER_ID"),
        default_warehouse_id=os.getenv("DEFAULT_DATABRICKS_WAREHOUSE_ID"),
        default_unity_catalog=os.getenv("DEFAULT_UNITY_CATALOG", "uc_tarhone"),
        agent_storage_path=os.getenv("AGENT_STORAGE_PATH", "/tmp/ingestion-agent"),
        log_level=os.getenv("LOG_LEVEL", "INFO"),
    )

from __future__ import annotations

import logging
import re
from typing import Dict, Optional, Tuple

from pydantic import ValidationError

from ingestion_agent.core.config_models import IngestionJobConfig, JobRequest, SourceType
from ingestion_agent.core.llm_client import LLMClient
from ingestion_agent.core.settings import Settings

logger = logging.getLogger(__name__)

DEFAULT_UC_BASE_PATH = "abfss://uctarhone@tarhonemetastore.dfs.core.chinacloudapi.cn/tarhoneroot1"
DEFAULT_LAYER = "bronze"
DEFAULT_POSTGRES_PORT = 5432
DEFAULT_POSTGRES_DB = "postgres"
DEFAULT_JOB_NAME = "ingestion_job"


class NLUParser:
    """Convert user natural language to structured ingestion configuration."""

    def __init__(self, settings: Settings):
        self.settings = settings
        self.llm = LLMClient(settings)

    def parse_request(self, request: JobRequest) -> IngestionJobConfig:
        if request.config:
            logger.info("Using provided structured configuration for job %s", request.config.job_name)
            return request.config

        if not request.natural_language:
            raise ValueError("Either natural_language or config must be provided")

        raw_config = self.llm.nl_to_config(request.natural_language)
        logger.debug("LLM returned raw config: %s", raw_config)

        if not isinstance(raw_config, dict) or "_raw" in raw_config:
            raise ValueError(
                "LLM could not produce a valid ingestion plan from the prompt. "
                "Please provide a clearer ETL description or switch to YAML/JSON config mode."
            )

        # Ensure top-level keys exist for augmentation.
        raw_config.setdefault("source", {})
        raw_config.setdefault("sink", {})
        raw_config.setdefault("transformations", {})
        # Provide a placeholder job name early to avoid validation failures.
        if not raw_config.get("job_name"):
            raw_config["job_name"] = DEFAULT_JOB_NAME

        # Check for validation errors from LLM
        if "validation_error" in raw_config:
            missing_fields = raw_config.get("missing", [])
            error_msg = "❌ 缺少必要信息，请提供：\n" + "\n".join(f"  • {field}" for field in missing_fields)
            error_msg += "\n\n请重新输入包含以上信息的完整描述。"
            raise ValueError(error_msg)

        if not isinstance(raw_config.get("source"), dict):
            raise ValueError("LLM could not produce a valid ingestion plan: source section is invalid.")
        if not isinstance(raw_config.get("sink"), dict):
            raise ValueError("LLM could not produce a valid ingestion plan: sink section is invalid.")
        if not isinstance(raw_config.get("transformations"), dict):
            # Transformations are optional; if LLM returns a list/str, fall back to empty dict.
            logger.info("Transformations not provided as dict; defaulting to empty transformations.")
            raw_config["transformations"] = {}

        # Fill missing pieces from the natural language text (host/table/sink path).
        text = request.natural_language or ""
        
        # Pre-fill from text FIRST to parse schema.table format
        self._prefill_from_text(raw_config, text)
        self._fill_jdbc_and_sink_defaults(raw_config, text)
        
        # Validate required fields AFTER pre-filling
        self._validate_required_fields(raw_config, text)
        
        self._normalize_transformations(raw_config)

        try:
            config = IngestionJobConfig.parse_obj(raw_config)
        except ValidationError as exc:
            logger.warning("LLM output failed validation: %s", exc)
            fallback = self._build_from_text(request.natural_language or "")
            if fallback:
                logger.info("Falling back to heuristic config derived from NL text.")
                config = IngestionJobConfig.parse_obj(fallback)
            else:
                raise ValueError(
                    "LLM could not produce a valid ingestion plan from the prompt. "
                    "Please describe the data source, fields, transformations, and sink explicitly, or paste a YAML/JSON config."
                ) from exc
        credentials = self._extract_inline_credentials(request.natural_language or "")
        if credentials:
            logger.info("Injecting inline credentials into source options.")
            # Spark JDBC expects 'user'/'password' keys.
            config.source.options.update(credentials)

        augmented = self._augment_from_text(config, request.natural_language or "")
        if augmented:
            config = augmented
        logger.info("Parsed config from NL; job=%s source=%s sink=%s", config.job_name, config.source.type, config.sink.type)
        return config

    def _extract_inline_credentials(self, text: str) -> Dict[str, str]:
        if not text:
            return {}
        creds: Dict[str, str] = {}
        patterns = {
            "user": [
                r"(?:用户名|username|user|账号)\s*(?:=|:|：|is|为|是)\s*([^\s,;，。]+)",
                r"(?:user|username)\s+([^\s,;，。]+)",
            ],
            "password": [
                r"(?:密码|password|pwd|口令)\s*(?:=|:|：|is|为|是)\s*([^\s,;，。]+)",
                r"(?:password|pwd)\s+([^\s,;，。]+)",
            ],
        }
        for key, regex_list in patterns.items():
            for pattern in regex_list:
                match = re.search(pattern, text, flags=re.IGNORECASE)
                if match:
                    creds[key if key != "user" else "user"] = match.group(1)
                    break
        return creds

    def _validate_required_fields(self, raw_config: Dict, text: str) -> None:
        """Validate that all required fields are present."""
        missing = []
        source = raw_config.get("source", {})
        sink = raw_config.get("sink", {})
        
        # Source validation
        if not source.get("type"):
            missing.append("源数据库类型 (postgres/mysql/sqlserver)")
        if not source.get("jdbc_url") and not self._parse_host_db(text)[0]:
            missing.append("源数据库主机地址 (hostname)")
        if not source.get("table"):
            missing.append("源表名称 (必须明确指定，例如: 表名为vwtable1 或 表=schema.table)")
        
        # Check credentials
        creds = self._extract_inline_credentials(text)
        if not creds.get("user") and not source.get("options", {}).get("user"):
            missing.append("数据库用户名 (username)")
        if not creds.get("password") and not source.get("options", {}).get("password"):
            missing.append("数据库密码 (password)")
        
        # Sink validation
        if not sink.get("database") and not sink.get("table"):
            missing.append("目标表名称 (schema.table)")
        elif sink.get("table") and "." not in sink.get("table", "") and not sink.get("database"):
            missing.append("目标schema名称")
        
        if missing:
            error_msg = "❌ 缺少必要信息，请提供：\n" + "\n".join(f"  • {field}" for field in missing)
            error_msg += "\n\n💡 示例：从 postgres 地址为xxx 数据库名称为xxx 表名为vwtable1 用户名：xxx 密码：xxx 抽取数据，写入表 test.table1"
            raise ValueError(error_msg)

    def _prefill_from_text(self, raw_config: Dict, text: str) -> None:
        """
        Mutate raw_config to add defaults for source/sink before Pydantic validation,
        based on the natural language text.
        """
        source = raw_config["source"]
        sink = raw_config["sink"]

        # Default sink structure to allow augmentation later.
        sink.setdefault("type", "delta")
        sink.setdefault("layer", sink.get("layer") or DEFAULT_LAYER)
        sink.setdefault("mode", sink.get("mode") or "append")

        # Try to derive source table; prefer explicit mention in NL over LLM guess.
        tbl = self._parse_source_table(text)
        if tbl:
            source["table"] = tbl
        
        # For source table, if no schema prefix and database type is known, add default schema
        if source.get("table") and "." not in source.get("table", ""):
            # Check if user explicitly mentioned a source schema
            src_text = self._get_source_segment(text)
            schema_match = re.search(r"(?:源|source).*?(?:schema|模式|架构)\s*(?:=|:|：|is|为|是)\s*([A-Za-z0-9_]+)", src_text, flags=re.IGNORECASE)
            if schema_match:
                source["table"] = f"{schema_match.group(1)}.{source['table']}"
            elif source.get("type") == "postgres":
                # PostgreSQL defaults to 'public' schema
                source["table"] = f"public.{source['table']}"
            elif source.get("type") == "mysql":
                # MySQL doesn't use schema concept in the same way, use database name
                pass
            elif source.get("type") == "sqlserver":
                # SQL Server defaults to 'dbo' schema
                source["table"] = f"dbo.{source['table']}"

        # If source is postgres and jdbc_url missing, we will fill later in augment.
        # Ensure source type defaults to postgres if hostname is mentioned.
        if not source.get("type"):
            if "postgres" in text.lower() or "pgsql" in text.lower():
                source["type"] = SourceType.postgres.value
        if not source.get("frequency"):
            source["frequency"] = "daily"

        # Try to set sink.table if absent.
        sink_tbl = self._parse_sink_table(text)
        if sink_tbl:
            # Split schema.table format immediately
            if "." in sink_tbl:
                schema, table = sink_tbl.split(".", 1)
                sink["database"] = schema
                sink["table"] = table
            else:
                sink["table"] = sink_tbl
        
        # Try to extract schema and table separately if not already set
        if not sink.get("database"):
            schema_match = re.search(r"(?:schema|模式|架构)\s*(?:=|:|：|is|为|是)\s*([A-Za-z0-9_]+)", text, flags=re.IGNORECASE)
            if schema_match:
                sink["database"] = schema_match.group(1)
        
        if not sink.get("table"):
            # Look for table name near sink/target section
            sink_section = self._get_sink_segment(text)
            table_patterns = [
                r"(?:表名为|表名是|表为|table\s*(?:name\s*)?(?:=|:|：|is|为|是))\s*([A-Za-z0-9_]+)",
            ]
            for pattern in table_patterns:
                table_match = re.search(pattern, sink_section, flags=re.IGNORECASE)
                if table_match:
                    sink["table"] = table_match.group(1)
                    break

        # Try to set sink.path if absent.
        if not sink.get("path"):
            base_path = self._parse_uc_path(text) or DEFAULT_UC_BASE_PATH
            # Use the split values if available
            schema = sink.get("database", "default")
            table = sink.get("table")
            if table:
                sink["path"] = f"{base_path}/{sink.get('layer', DEFAULT_LAYER)}/{schema}/{table}"
            else:
                sink["path"] = None

        # Default job_name using sink or source table.
        if not raw_config.get("job_name"):
            if sink.get("table"):
                raw_config["job_name"] = f"ingest_{sink['table'].replace('.', '_')}"
            elif source.get("table"):
                raw_config["job_name"] = f"ingest_{source['table']}"
            else:
                raw_config["job_name"] = DEFAULT_JOB_NAME

    def _fill_jdbc_and_sink_defaults(self, raw_config: Dict, text: str) -> None:
        source = raw_config["source"]
        sink = raw_config["sink"]

        # JDBC URL for postgres; prefer NL-derived host/db even if LLM provided defaults.
        if source.get("type") == SourceType.postgres.value:
            host, port, db = self._parse_host_db(text)
            if host:
                source["jdbc_url"] = f"jdbc:postgresql://{host}:{port}/{db or DEFAULT_POSTGRES_DB}"

        # Split sink table if it contains schema.table format
        if sink.get("table") and "." in sink["table"] and not sink.get("database"):
            schema, table = sink["table"].split(".", 1)
            sink["database"] = schema
            sink["table"] = table

        # Source table is REQUIRED - do not derive from sink table!
        # Validation will catch missing source table and prompt user

        # Sink path from sink.table if missing.
        if not sink.get("path") and sink.get("table"):
            base_path = self._parse_uc_path(text) or DEFAULT_UC_BASE_PATH
            schema = sink.get("database", "default")
            table = sink.get("table")
            layer = sink.get("layer", DEFAULT_LAYER)
            sink["path"] = f"{base_path}/{layer}/{schema}/{table}"
        # Remove path from options to avoid conflicts with writer.save(path)
        if sink.get("options") and isinstance(sink["options"], dict):
            sink["options"].pop("path", None)
        
        # Target table is also REQUIRED - do not derive from source table!
        # Validation will catch missing target table and prompt user

    def _normalize_transformations(self, raw_config: Dict) -> None:
        tf = raw_config.get("transformations") or {}
        # Handle select as dict with "columns".
        sel = tf.get("select")
        if isinstance(sel, dict) and "columns" in sel:
            tf["select"] = sel.get("columns") or []
        # Ensure select is list.
        if sel is None:
            tf["select"] = []
        # Aggregate metrics as dict; if list, drop to empty dict.
        agg = tf.get("aggregate")
        if isinstance(agg, dict):
            metrics = agg.get("metrics")
            if isinstance(metrics, list):
                agg["metrics"] = {}
        elif agg is None:
            tf["aggregate"] = None
        else:
            tf["aggregate"] = None
        raw_config["transformations"] = tf

    def _augment_from_text(self, config: IngestionJobConfig, text: str) -> Optional[IngestionJobConfig]:
        updated = False

        # Build JDBC URL if missing for postgres.
        if config.source.type == SourceType.postgres and not config.source.jdbc_url:
            host, port, db = self._parse_host_db(text)
            if host:
                jdbc_url = f"jdbc:postgresql://{host}:{port}/{db}"
                logger.info("Constructed JDBC URL from NL: %s", jdbc_url)
                config.source.jdbc_url = jdbc_url
                updated = True
            else:
                raise ValueError(
                    "LLM could not produce a valid ingestion plan: missing JDBC connection info (host/db). "
                    "Provide source host/database or use YAML/JSON config mode."
                )

        # Sink table/path defaults.
        sink_table = config.sink.table
        if not sink_table:
            sink_table = self._parse_sink_table(text)
            if sink_table:
                # Parse sink table into database and table components
                if "." in sink_table:
                    schema, table = sink_table.split(".", 1)
                    config.sink.database = schema
                    config.sink.table = table
                else:
                    config.sink.table = sink_table
                updated = True

        # Set default Unity Catalog if not specified
        if not config.sink.catalog and self.settings.default_unity_catalog:
            config.sink.catalog = self.settings.default_unity_catalog
            updated = True

        if not config.sink.path:
            base_path = self._parse_uc_path(text) or DEFAULT_UC_BASE_PATH
            # Use the already parsed database and table values
            schema = config.sink.database or "default"
            table = config.sink.table or "unknown"
            layer = config.sink.layer or DEFAULT_LAYER
            config.sink.path = f"{base_path}/{layer}/{schema}/{table}"
            config.sink.layer = config.sink.layer or DEFAULT_LAYER
            updated = True

        if not config.sink.type:
            config.sink.type = "delta"
            updated = True

        return config if updated else None

    def _parse_host_db(self, text: str) -> Tuple[Optional[str], int, str]:
        host_patterns = [
            r"(?:hostname|host|地址|主机)\s*(?:=|:|：|为|是)\s*([^\s,，。]+)",
            r"(?:地址为|主机为)\s*([^\s,，。]+)",
        ]
        db_patterns = [
            r"(?:database|db|数据库(?:名称)?)\s*(?:=|:|：|为|是)\s*([^\s,，。]+)",
            r"(?:数据库为|数据库名为|数据库名是|数据库名称为|数据库名称是)\s*([^\s,，。]+)",
        ]
        port = DEFAULT_POSTGRES_PORT
        
        host = None
        for pattern in host_patterns:
            host_match = re.search(pattern, text, flags=re.IGNORECASE)
            if host_match:
                host = host_match.group(1)
                break

        db = None
        for pattern in db_patterns:
            db_match = re.search(pattern, text, flags=re.IGNORECASE)
            if db_match:
                db = db_match.group(1)
                break
        db = db or DEFAULT_POSTGRES_DB
        if host and ":" in host:
            host_only, maybe_port = host.split(":", 1)
            host = host_only
            if maybe_port.isdigit():
                port = int(maybe_port)
        return host, port, db

    def _parse_sink_table(self, text: str) -> Optional[str]:
        sink_text = self._get_sink_segment(text)
        # 支持多种表达方式，按优先级匹配
        patterns = [
            # 明确的目标表关键词 + schema.table格式
            r"(?:目标表名为|目标表名是|目标表为|目标表是|写入表|写入|抽取到|导入到)\s*([A-Za-z0-9_]+\.[A-Za-z0-9_]+)",
            # "的表"前面的 schema.table
            r"([A-Za-z0-9_]+\.[A-Za-z0-9_]+)\s*的?\s*表",
            # 独立的"表 schema.table"格式
            r"表\s+([A-Za-z0-9_]+\.[A-Za-z0-9_]+)",
            # 最后才尝试匹配孤立的 schema.table（避免误匹配主机名）
            r"(?:^|\s)([A-Za-z0-9_]+\.[A-Za-z0-9_]+)(?:\s|$|中|的)",
        ]
        for pattern in patterns:
            match = re.search(pattern, sink_text)
            if match:
                table_name = match.group(1)
                # 过滤掉主机名（包含连字符或多个点的不是表名）
                if "-" not in table_name and table_name.count(".") == 1:
                    return table_name
        
        # Fallback: 单个表名 + schema推导
        tbl_match = re.search(r"([A-Za-z0-9_]+)\s*表", sink_text)
        if tbl_match:
            schema = self._parse_schema_token(sink_text)
            tbl = tbl_match.group(1)
            return f"{schema or 'default'}.{tbl}"
        return None

    def _parse_source_table(self, text: str) -> Optional[str]:
        src_text = self._get_source_segment(text)
        # 支持多种表达方式
        patterns = [
            r"(?:表名为|表名是|源表为|源表是)\s*([A-Za-z0-9_]+(?:\.[A-Za-z0-9_]+)?)",
            r"表\s*([A-Za-z0-9_]+(?:\.[A-Za-z0-9_]+)?)",
        ]
        for pattern in patterns:
            match = re.search(pattern, src_text)
            if match:
                token = match.group(1)
                if token.lower() not in {"databricks", "delta"}:
                    if "." not in token:
                        schema = self._parse_schema_token(src_text)
                        if schema:
                            return f"{schema}.{token}"
                    return token
        # Fallback: capture "<name>表"
        match = re.search(r"([A-Za-z0-9_]+)\s*表", src_text)
        if match:
            token = match.group(1)
            if token.lower() not in {"databricks", "delta"}:
                schema = self._parse_schema_token(src_text)
                if "." not in token and schema:
                    return f"{schema}.{token}"
                return token
        return None

    def _parse_uc_path(self, text: str) -> Optional[str]:
        match = re.search(r"(abfss://[^\s,，]+)", text)
        if match:
            return match.group(1).rstrip("/")
        return None

    def _parse_schema_token(self, text: str) -> Optional[str]:
        match = re.search(r"(?:schema|模式)\s*(?:=|:|为)\s*([A-Za-z0-9_]+)", text, flags=re.IGNORECASE)
        if match:
            return match.group(1)
        return None

    def _get_sink_segment(self, text: str) -> str:
        # 识别目标段落的多种表达
        match = re.search(r"(写入|抽取到|导入到|sink|databricks|目标|delta\s*table)", text, flags=re.IGNORECASE)
        if match:
            return text[match.start():]
        return text

    def _get_source_segment(self, text: str) -> str:
        match = re.search(r"(写入|抽取到|导入到|sink|databricks|目标)", text, flags=re.IGNORECASE)
        if match:
            return text[: match.start()]
        return text

    def _build_from_text(self, text: str) -> Optional[Dict]:
        """
        Build a minimal ingestion config purely from the NL text when LLM output is insufficient.
        Requires host and table at minimum; sink table derived if present.
        """
        host, port, db = self._parse_host_db(text)
        src_table = self._parse_source_table(text)
        sink_table = self._parse_sink_table(text)
        base_path = self._parse_uc_path(text) or DEFAULT_UC_BASE_PATH
        layer = self._parse_layer(text) or DEFAULT_LAYER
        creds = self._extract_inline_credentials(text)

        if not host or not src_table:
            return None

        sink_path = None
        if sink_table:
            schema, table = sink_table.split(".", 1) if "." in sink_table else ("default", sink_table)
            sink_path = f"{base_path}/{layer}/{schema}/{table}"

        config = {
            "job_name": f"ingest_{(sink_table or src_table).replace('.', '_')}",
            "source": {
                "type": SourceType.postgres.value,
                "jdbc_url": f"jdbc:postgresql://{host}:{port}/{db or DEFAULT_POSTGRES_DB}",
                "table": src_table,
                "frequency": "daily",
                "options": creds,
            },
            "transformations": {},
            "sink": {
                "type": "delta",
                "table": sink_table or f"default.{src_table}",
                "path": sink_path,
                "layer": layer,
                "mode": "append",
            },
        }
        return config

    def _parse_layer(self, text: str) -> Optional[str]:
        match = re.search(r"layer\s*(?:=|:|是)\s*([A-Za-z0-9_]+)", text, flags=re.IGNORECASE)
        if match:
            return match.group(1).lower()
        return None

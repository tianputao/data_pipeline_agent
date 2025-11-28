from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

import streamlit as st
import yaml

from ingestion_agent.agent.service import IngestionAgentService
from ingestion_agent.core.config_models import IngestionJobConfig, JobRequest
from ingestion_agent.core.settings import get_settings


@st.cache_resource(show_spinner=False)
def get_service() -> IngestionAgentService:
    settings = get_settings()
    return IngestionAgentService(settings=settings)


@st.cache_data(show_spinner=False)
def load_sample_yaml(db_type: str = "postgres") -> str:
    """Load sample YAML based on database type."""
    examples_dir = Path(__file__).resolve().parents[1] / "examples"
    
    # Map database type to example file
    file_map = {
        "postgres": "postgres_orders.yaml",
        "mysql": "orders.yaml",
        "sqlserver": "sqlserver_sales.yaml"
    }
    
    sample_file = file_map.get(db_type, "postgres_orders.yaml")
    sample_path = examples_dir / sample_file
    
    if sample_path.exists():
        return sample_path.read_text()
    return ""


def parse_config_text(text: str) -> Dict[str, Any]:
    try:
        return yaml.safe_load(text)
    except yaml.YAMLError:
        return json.loads(text)


def render_preview(outcome) -> None:
    st.success("Plan ready" if outcome.run_id is None else f"Submitted. Run ID: {outcome.run_id}")
    st.write(
        {
            "job_name": outcome.job_config.job_name,
            "run_id": outcome.run_id,
            "script_path": outcome.script_path,
        }
    )
    st.subheader("Script preview")
    st.code(outcome.script, language="python")


def main() -> None:
    st.set_page_config(page_title="Ingestion Agent", layout="wide")
    st.title("Ingestion Agent (Databricks / PySpark)")
    st.caption(
        "Turn natural language or YAML/JSON configs into PySpark ETL code, optionally submit to Azure Databricks."
    )

    mode = st.radio("Input mode", ["Natural language", "Form (表单)", "YAML/JSON config"], horizontal=True)
    action = st.selectbox("Action", ["Render only", "Render + Submit to Databricks"], index=0)
    submit = action == "Render + Submit to Databricks"

    service = get_service()

    if mode == "Natural language":
        nl = st.text_area(
            "Describe your ingestion task",
            value="我要从pgsql抽取数据，数据库为migrationTarget，表名为vwtable1, 用户名：tarhone, 密码：xxxxxx,  地址为cn3-postgre-test.postgres.database.chinacloudapi.cn。抽取到test.pgsqltest1的表中",
            height=180,
        )
        if st.button("Generate", type="primary"):
            if not nl.strip():
                st.warning("Please enter a requirement.")
                return
            with st.spinner("Generating plan and code..."):
                try:
                    request = JobRequest(natural_language=nl, render_only=not submit)
                    outcome = service.submit(request) if submit else service.plan(request)
                    render_preview(outcome)
                except ValueError as exc:  # pragma: no cover - UI path
                    st.warning(
                        f"{exc}\n\n"
                        "Tip: describe an ingestion task (source URL/table, columns, transformations, sink path/table) "
                        "or switch to YAML/JSON config mode."
                    )
                except Exception as exc:  # pragma: no cover - UI path
                    st.error("Failed to generate/submit job.")
                    st.exception(exc)
    elif mode == "Form (表单)":
        st.subheader("📝 填写数据抽取表单")
        
        with st.expander("📥 Source (源数据库)", expanded=True):
            col1, col2 = st.columns(2)
            with col1:
                # Database type selection with friendly names
                db_type_map = {"PostgreSQL": "postgres", "MySQL": "mysql", "SQL Server": "sqlserver"}
                db_type_display = st.selectbox("数据库类型", list(db_type_map.keys()), index=0)
                src_type = db_type_map[db_type_display]
                
                src_host = st.text_input("主机地址 (Hostname)", placeholder="例如: mydb.postgres.database.azure.com")
                src_db = st.text_input("数据库名称", placeholder="例如: production")
            with col2:
                # Default port based on database type
                default_port = 5432 if src_type == "postgres" else (3306 if src_type == "mysql" else 1433)
                src_port = st.number_input("端口号", min_value=1, max_value=65535, value=default_port)
                
                # Default schema based on database type
                default_schema = "public" if src_type == "postgres" else ("dbo" if src_type == "sqlserver" else "")
                src_schema = st.text_input("Schema名称", value=default_schema, placeholder="例如: public")
                src_table = st.text_input("表名称", placeholder="例如: orders")
        
        with st.expander("🔐 Credentials (数据库凭证)", expanded=True):
            col1, col2 = st.columns(2)
            with col1:
                src_user = st.text_input("用户名", placeholder="数据库用户名")
            with col2:
                src_pwd = st.text_input("密码", type="password", placeholder="数据库密码")
            st.warning("⚠️ 安全建议：生产环境请使用 Azure Key Vault 或 Databricks Secrets 管理密码，不要明文输入。")
        
        with st.expander("📤 Sink (目标 - Databricks)", expanded=True):
            col1, col2 = st.columns(2)
            with col1:
                sink_catalog = st.text_input("Catalog名称", value="uc_tarhone", help="Unity Catalog名称")
                sink_schema = st.text_input("Schema名称 (必填)", placeholder="例如: test")
            with col2:
                sink_table = st.text_input("表名称 (必填)", placeholder="例如: vwtable1")
                sink_mode = st.selectbox("写入模式", ["overwrite", "append"], index=0)
        
        with st.expander("🔧 Transformations (可选)", expanded=False):
            transform_select = st.text_input("选择字段 (逗号分隔，留空=全选)", placeholder="例如: id, name, amount")
            st.caption("更多转换选项可在 YAML 模式中配置")
        
        if st.button("Generate from Form", type="primary"):
            # Validate required fields
            errors = []
            if not src_host: errors.append("源数据库主机地址")
            if not src_db: errors.append("源数据库名称")
            if not src_table: errors.append("源表名称")
            if not src_user: errors.append("用户名")
            if not src_pwd: errors.append("密码")
            if not sink_schema: errors.append("目标Schema名称")
            if not sink_table: errors.append("目标表名称")
            
            if errors:
                st.error("❌ 缺少必要信息：\n" + "\n".join(f"  • {e}" for e in errors))
                return
            
            # Build config (port already has default value from number_input)
            # Map internal type to JDBC protocol (postgres -> postgresql)
            jdbc_protocol = "postgresql" if src_type == "postgres" else ("mysql" if src_type == "mysql" else "sqlserver")
            jdbc_url = f"jdbc:{jdbc_protocol}://{src_host}:{src_port}/{src_db}"
            
            # Build source options with credentials and database-specific settings
            source_options = {"user": src_user, "password": src_pwd}
            # PostgreSQL requires SSL mode for Azure
            if src_type == "postgres":
                source_options["sslmode"] = "require"
            
            config_dict = {
                "job_name": f"ingest_{src_schema}_{src_table}_to_{sink_schema}_{sink_table}",
                "description": f"ETL from {src_type} {src_schema}.{src_table} to Databricks {sink_schema}.{sink_table}",
                "source": {
                    "type": src_type,
                    "jdbc_url": jdbc_url,
                    "table": f"{src_schema}.{src_table}" if src_schema else src_table,
                    "frequency": "daily",
                    "options": source_options
                },
                "transformations": {
                    "select": transform_select.split(",") if transform_select.strip() else ["*"]
                },
                "sink": {
                    "type": "delta",
                    "catalog": sink_catalog,
                    "database": sink_schema,
                    "table": sink_table,
                    "mode": sink_mode,
                    "layer": "bronze",
                    "options": {}
                }
            }
            
            with st.spinner("Generating code from form..."):
                try:
                    job_cfg = IngestionJobConfig.parse_obj(config_dict)
                    request = JobRequest(config=job_cfg, render_only=not submit)
                    outcome = service.submit(request) if submit else service.plan(request)
                    render_preview(outcome)
                except Exception as exc:
                    st.error("Failed to generate/submit job.")
                    st.exception(exc)
    else:
        st.subheader("📄 YAML/JSON 配置模式")
        
        # Database type selector for loading different examples
        col1, col2 = st.columns([3, 1])
        with col1:
            db_example_map = {"PostgreSQL": "postgres", "MySQL": "mysql", "SQL Server": "sqlserver"}
            db_example_display = st.selectbox(
                "选择数据库类型示例", 
                list(db_example_map.keys()), 
                index=0,
                key="yaml_db_selector"
            )
            db_example_type = db_example_map[db_example_display]
        with col2:
            st.write("")  # Spacer
            st.write("")  # Spacer
            if st.button("加载示例", key="load_example_btn"):
                st.session_state.yaml_text = load_sample_yaml(db_example_type)
                st.rerun()
        
        # Initialize session state for text area
        if "yaml_text" not in st.session_state:
            st.session_state.yaml_text = load_sample_yaml("postgres")
        
        uploaded = st.file_uploader("Upload YAML/JSON config", type=["yaml", "yml", "json"])
        
        # If file uploaded, use its content
        if uploaded:
            st.session_state.yaml_text = uploaded.read().decode("utf-8")
        
        text = st.text_area("Config", value=st.session_state.yaml_text, height=300, key="yaml_editor")

        if st.button("Generate", type="primary"):
            if not text.strip():
                st.warning("Please provide a config.")
                return
            with st.spinner("Parsing config and generating code..."):
                try:
                    cfg_dict = parse_config_text(text)
                    job_cfg = IngestionJobConfig.parse_obj(cfg_dict)
                    request = JobRequest(config=job_cfg, render_only=not submit)
                    outcome = service.submit(request) if submit else service.plan(request)
                    render_preview(outcome)
                except ValueError as exc:  # pragma: no cover - UI path
                    st.warning(str(exc))
                except Exception as exc:  # pragma: no cover - UI path
                    st.error("Failed to generate/submit job.")
                    st.exception(exc)

    st.markdown(
        """
---  
**Notes**
- 🔐 **安全建议**: 生产环境请使用 Azure Key Vault 或 Databricks Secrets 管理敏感信息，避免明文存储密码
- 🗂️ **Schema默认值**: 
  - PostgreSQL: 如果不指定源表schema，默认使用 `public` schema
  - SQL Server: 默认使用 `dbo` schema
  - MySQL: 不需要schema，直接使用表名
- 🤖 Natural language mode requires a configured LLM (Azure OpenAI or local OpenAI-compatible endpoint via `.env`)
- 📝 Form mode provides a guided interface for quick config generation
- 📄 YAML/JSON mode allows full control over all configuration options
- ☁️ Submitting to Databricks requires valid host/token/cluster in `.env`
- 🎯 Default target: Unity Catalog `uc_tarhone`, layer `bronze`, mode `overwrite`
- 💡 **表名表达**: 支持 `schema.table` 或分开指定 "schema是xxx, 表名为xxx"
"""
    )


if __name__ == "__main__":
    main()

# Data Pipeline Ingestion Agent (数据抽取智能Agent)

智能数据抽取Agent，支持自然语言、表单或YAML/JSON配置，自动生成PySpark ETL代码并提交到Azure Databricks。

## 核心特性

- 🤖 **智能自然语言解析**: 支持中文和英文描述，自动识别源/目标数据库信息
- 📝 **可视化表单**: 引导式填写，降低使用门槛
- 📄 **YAML/JSON配置**: 适合高级用户和自动化场景
- 🎯 **自动验证**: 缺少必要信息时会提示用户补充
- ☁️ **Azure Databricks集成**: 一键提交到Databricks cluster执行

## 快速开始

### 1. 环境配置
```bash
cp .env.example .env
# 编辑 .env 文件
```

必填环境变量：
```bash
AZURE_DATABRICKS_HOST=https://adb-xxx.azuredatabricks.net
AZURE_DATABRICKS_TOKEN=dapi***
DEFAULT_DATABRICKS_CLUSTER_ID=xxx-xxx-xxx
DEFAULT_UNITY_CATALOG=xxxxx
```

### 2. 安装
```bash
pip install -e .[ui]
```

### 3. 启动UI
```bash
streamlit run src/ingestion_agent/ui/streamlit_app.py
```

## 使用方式

### 方式1: 自然语言
```
从 postgres hostname=mydb.postgres.database.azure.com 
数据库=production 表=public.orders 
用户名=admin 密码=pass123 
抽取数据，写入表 test.orders
```

自动识别：
- ✅ 数据库类型 (postgres/mysql/sqlserver)
- ✅ 连接信息 (hostname:port/database)
- ✅ 表名 (schema.table)
- ✅ 凭证 (username/password)
- ✅ 目标表 (catalog.schema.table)

### 方式2: 表单填写 （推荐）
1. 选择"Form (表单)"模式
2. 填写源数据库信息
3. 填写凭证（用户名密码）
4. 填写目标表信息
5. 点击"Generate from Form"

### 方式3: YAML配置
```yaml
job_name: ingest_pgsql_to_databricks
description: "Extract data from PostgreSQL and load to Databricks Unity Catalog bronze layer"

source:
  type: postgres                 # Options: postgres, mysql, sqlserver
  jdbc_url: jdbc:postgresql://[填写主机名].postgres.database.chinacloudapi.cn:5432/[填写数据库名]
  table: public.orders           # Format: schema.table (PostgreSQL defaults to 'public' schema)
  frequency: daily
  # increment_field: updated_at  # Optional: for incremental extraction
  options:
    user: [填写用户名]
    password: [填写密码]
    sslmode: require             # Required for Azure PostgreSQL
sink:
  type: delta                    # Always use delta for Unity Catalog
  catalog: uc_tarhone            # Unity Catalog name (must be created in workspace)
  database: test                 # Schema name in Unity Catalog
  table: orders                  # Table name
  layer: bronze                  # Options: bronze, silver, gold
  mode: overwrite                # Options: overwrite, append
  options: {}                    # Additional Delta Lake options (usually empty for managed tables)
  # path: abfss://container@storage.dfs.core.chinacloudapi.cn/bronze/test/orders  # Auto-generated for managed tables
```

## 必要信息

### 源数据库
- ✅ 数据库类型
- ✅ 主机地址
- ✅ 数据库名
- ✅ 表名 (schema.table)
- ✅ 用户名
- ✅ 密码

### 目标 (Databricks)
- ✅ Schema名称
- ✅ 表名称
- ✅ Catalog
- ✅ 模式: 默认 overwrite

## 🔐 安全提示

**当前**: 支持明文密码（仅开发/测试）

**生产环境**（详见 SECURITY.md）:
1. Azure Key Vault
2. Databricks Secrets
3. Managed Identity

## 常见问题

**Q: Catalog not found?**
```sql
CREATE CATALOG IF NOT EXISTS uc_tarhone;
CREATE SCHEMA IF NOT EXISTS uc_tarhone.test;
```

**Q: 如何保护密码?**
参考 `SECURITY.md`

**Q: 支持哪些数据库?**
PostgreSQL, MySQL, SQL Server/Azure SQL

## 文档

- [English README](README.md)
- [安全最佳实践](SECURITY.md)
- [示例配置](src/ingestion_agent/examples/)

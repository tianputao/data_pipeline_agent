# Data Pipeline Ingestion Agent (数据抽取智能Agent)

智能数据抽取Agent，支持自然语言、表单或YAML/JSON配置，自动生成PySpark ETL代码并提交到Azure Databricks。

## 核心特性

- 🤖 **智能自然语言解析**: 支持中文和英文描述，自动识别源/目标数据库信息
- 📝 **可视化表单**: 引导式填写，降低使用门槛
- 📄 **YAML/JSON配置**: 适合高级用户和自动化场景
- 🎯 **自动验证**: 缺少必要信息时会提示用户补充
- ☁️ **Azure Databricks集成**: 一键提交到Databricks cluster执行
- 🔐 **安全建议**: 内置安全提示，支持Key Vault集成

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
DEFAULT_UNITY_CATALOG=uc_tarhone
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

### 方式1: 自然语言（推荐）
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

### 方式2: 表单填写
1. 选择"Form (表单)"模式
2. 填写源数据库信息
3. 填写凭证（用户名密码）
4. 填写目标表信息
5. 点击"Generate from Form"

### 方式3: YAML配置
```yaml
source:
  type: postgres
  jdbc_url: jdbc:postgresql://host:5432/db
  table: public.orders
  options:
    user: admin
    password: pass123
sink:
  catalog: uc_tarhone
  database: test
  table: orders
  mode: overwrite
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
- Catalog: 默认 uc_tarhone
- 模式: 默认 overwrite

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

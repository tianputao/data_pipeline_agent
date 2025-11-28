# 安全最佳实践 (Security Best Practices)

## 当前状态 (Current State)

当前版本支持在自然语言或表单中直接输入数据库密码，这仅适用于**开发和测试环境**。

## 🔐 生产环境安全建议

### 方案1：使用 Azure Key Vault（推荐）

```python
# 1. 在 Azure Key Vault 中存储密码
# 2. 修改代码使用 Azure SDK 获取密码

from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

def get_db_password(secret_name: str) -> str:
    credential = DefaultAzureCredential()
    vault_url = "https://your-keyvault.vault.azure.cn/"
    client = SecretClient(vault_url=vault_url, credential=credential)
    return client.get_secret(secret_name).value

# 使用
password = get_db_password("postgres-password")
```

### 方案2：使用 Databricks Secrets

```python
# 1. 在 Databricks CLI 中创建 secret scope
# databricks secrets create-scope --scope jdbc_credentials

# 2. 添加密码
# databricks secrets put --scope jdbc_credentials --key postgres_password

# 3. 在生成的 PySpark 代码中使用
jdbc_options["password"] = dbutils.secrets.get(scope="jdbc_credentials", key="postgres_password")
```

### 方案3：环境变量 + .env 文件

```bash
# .env 文件 (添加到 .gitignore)
POSTGRES_USER=myuser
POSTGRES_PASSWORD=secret123
MYSQL_USER=admin
MYSQL_PASSWORD=pass456
```

```python
# 代码中读取
import os
from dotenv import load_dotenv

load_dotenv()
password = os.getenv("POSTGRES_PASSWORD")
```

## 实施计划

### 短期改进 (1-2周)
1. ✅ 在 UI 中添加安全警告
2. 在表单模式中添加"使用 Key Vault"选项
3. 添加环境变量支持

### 中期改进 (1个月)
1. 集成 Azure Key Vault SDK
2. 支持 Databricks Secrets API
3. 添加密码加密存储

### 长期改进 (3个月)
1. 支持 Service Principal 认证（无密码）
2. 支持 Managed Identity
3. 审计日志记录

## 当前临时措施

如果必须在当前版本使用密码：

1. ⚠️ **不要提交包含密码的配置文件到 Git**
2. 使用 `.gitignore` 忽略敏感文件
3. 限制 Streamlit 应用的访问权限
4. 定期轮换密码

## 相关资源

- [Azure Key Vault 文档](https://learn.microsoft.com/zh-cn/azure/key-vault/)
- [Databricks Secrets 文档](https://docs.databricks.com/security/secrets/index.html)
- [Azure Managed Identity](https://learn.microsoft.com/zh-cn/azure/active-directory/managed-identities-azure-resources/)

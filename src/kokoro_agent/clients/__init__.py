"""外部 public contract 的最小客户端协议。

这里仅放 GA 需要的窄接口；具体连接（Capability、Storage、Studio、Billing、Model）由 worker
或部署适配器提供，Agent/Feature 不依赖外部仓库的数据库模型。
"""

from kokoro_agent.clients.mcp import McpClient, McpClientError
from kokoro_agent.clients.skills import (
    NoSkillsClient,
    ResolvedSkill,
    SkillClient,
    SkillClientError,
    SkillReader,
)
from kokoro_agent.clients.storage import (
    DeliveryClient,
    DeliveryReceipt,
    DeliveryRequest,
    StorageClientError,
)

__all__ = [
    "McpClient",
    "McpClientError",
    "NoSkillsClient",
    "DeliveryClient",
    "DeliveryReceipt",
    "DeliveryRequest",
    "StorageClientError",
    "SkillClient",
    "SkillClientError",
    "ResolvedSkill",
    "SkillReader",
]

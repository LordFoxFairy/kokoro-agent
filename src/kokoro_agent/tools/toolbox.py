"""进程级内置工具箱：恒挂底座工具的唯一出口。

内置工具按"参数来自哪根轴"分三类，各归各位：
  无参常量（ask_user）           → registry 常量，类型工厂作 core_tools 恒挂
  进程配置态（web_search/web_fetch）→ 启动构建一次入箱（配置缺失即缺席，不挂空壳）
wire 点名的注册表工具与 MCP 外接工具不在箱内——它们由 wire 逐 run 决定。

ADR-013 M0 已从生产工具箱移除旧 Mongo store-backed memory。namespace 参数保留在
方法边界，供后续窄 MemoryPort 工具在 Root 合同激活后接入；当前不得据此恢复旧工具。
"""

from __future__ import annotations

from dataclasses import dataclass

from langchain_core.tools import BaseTool

from kokoro_agent.tools.web_fetch import make_web_fetch_tool
from kokoro_agent.tools.web_search import (
    SearchProviderSettings,
    make_search_provider,
    make_web_search_tool,
)


@dataclass(frozen=True, slots=True)
class ProcessToolbox:
    """worker 启动构建一次，逐请求出恒挂底座。"""

    # 进程配置态工具（当前=联网工具 web_search/web_fetch；web=互联网，非 kokoro-web）。
    configured: tuple[BaseTool, ...]

    def tools_for(self, namespace: str) -> tuple[BaseTool, ...]:
        """当前恒挂底座只有已配置工具；旧 store-backed memory 不可生产装配。"""
        # 保持已有 keyword-compatible API；未来窄 MemoryPort 可消费同一 opaque namespace。
        _ = namespace
        return self.configured


def build_toolbox(
    *, fetch_allow_private: bool, search: SearchProviderSettings | None
) -> ProcessToolbox:
    tools: list[BaseTool] = [make_web_fetch_tool(allow_private=fetch_allow_private)]
    # search 配置即挂载：无 provider 不挂空壳。
    if search is not None:
        tools.append(make_web_search_tool(make_search_provider(search)))
    return ProcessToolbox(configured=tuple(tools))

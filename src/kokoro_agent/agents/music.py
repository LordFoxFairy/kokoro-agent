"""Music Agent：可独立运行、也可被其他 Feature 复用的完整音乐能力。"""

from kokoro_agent.agents.definition import Agent
from kokoro_agent.tools.ask_user_question import ASK_USER_TOOL, ASK_USER_TOOL_NAME

MUSIC_PROMPT = """你是 Music Agent。理解音乐创作需求，协助用户完成构思、编曲、歌词和制作计划。
需要生成或交付外部媒体时，先明确需求，再通过 Feature 声明的工具和客户端执行。"""

MUSIC_AGENT = Agent(
    key="music",
    prompt=MUSIC_PROMPT,
    tools=(ASK_USER_TOOL,),
    skills=("music",),
    delivery=True,
    pause_tools=frozenset({ASK_USER_TOOL_NAME}),
)

__all__ = ["MUSIC_AGENT", "MUSIC_PROMPT"]

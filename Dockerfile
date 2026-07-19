# kokoro-agent 生产镜像（Python 3.11 + uv）。worker 进程（kokoro-agent-worker），非 HTTP 服务。
# 依赖：redis / mongo / litellm（KOKORO_LITELLM_BASE_URL）；env 运行时注入。
FROM python:3.11-slim
WORKDIR /app

# uv 装依赖管理器；git 供部分源码依赖（如有）。
RUN pip install --no-cache-dir uv

# 依赖层（不含本项目）——先拷 lock 命中缓存。
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-install-project --no-dev

# 项目源 + 安装本包（提供 kokoro-agent-worker 入口）。
COPY . .
RUN uv sync --frozen --no-dev

RUN useradd --system --uid 1001 kokoro && chown -R kokoro:kokoro /app
USER kokoro
ENV PYTHONUNBUFFERED=1
# worker：从 redis 取 dispatch、跑 run、发事件。无端口。
CMD ["uv", "run", "kokoro-agent-worker"]

# kokoro-agent 生产镜像（Python 3.11 + uv）。worker 进程（kokoro-agent-worker），非 HTTP 服务。
# 依赖：redis / postgresql；LiteLLM 是可选的外置 OpenAI-compatible gateway，env 运行时注入。
# Pin the multi-architecture Python base to an immutable OCI index digest.
FROM python:3.11-slim@sha256:9534e5a8e315485d4061ed659af0fd78a284c015f9b73661b41d6bab25604534
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
# 系统用户无家目录 → uv 默认缓存 ~/.cache/uv 不可写(EACCES)。指到 /app(已 chown kokoro)下可写目录。
ENV UV_CACHE_DIR=/app/.uv-cache
# worker：从 redis 取 dispatch、跑 run、发事件。无端口。依赖已在 build 期 uv sync 烘焙,
# --no-sync 免运行时再联网 sync(生产离线也能起)。
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 CMD ["python", "-c", "import os; raise SystemExit(0 if os.path.exists('/proc/1/cmdline') else 1)"]
CMD ["uv", "run", "--no-sync", "kokoro-agent-worker"]

ARG PYTHON_IMAGE=python:3.11-slim-bookworm@sha256:d29f48a31a8b408ed19272ca1e7b10ebae13b240a27e862d3d4217c528e2e0c3

FROM ${PYTHON_IMAGE} AS build
WORKDIR /app

# The build tool is pinned and never copied into the production stage. The first sync keeps
# dependency layers stable; the second installs the project as a non-editable wheel-style package.
RUN python -m pip install --no-cache-dir uv==0.9.4
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev --no-install-project
COPY src ./src
RUN uv sync --frozen --no-dev --no-editable \
    && test -x /app/.venv/bin/kokoro-agent-worker \
    && test -x /app/.venv/bin/kokoro-agent-evidence \
    && test -x /app/.venv/bin/kokoro-agent-presentation

FROM ${PYTHON_IMAGE} AS runtime
WORKDIR /app

RUN rm -rf /usr/local/lib/python3.11/site-packages/pip \
        /usr/local/lib/python3.11/site-packages/pip-* \
        /usr/local/bin/pip /usr/local/bin/pip3 /usr/local/bin/pip3.11 \
    && groupadd --system --gid 10001 kokoro \
    && useradd --system --uid 10001 --gid 10001 --no-create-home --home-dir /nonexistent \
       --shell /usr/sbin/nologin kokoro

COPY --from=build --chown=10001:10001 /app/.venv /app/.venv

ENV PATH="/app/.venv/bin:${PATH}" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    HOME=/nonexistent

USER 10001:10001
EXPOSE 8443 8444
STOPSIGNAL SIGTERM

# Each role exposes its own dependency-aware `--readiness` exec command in the owner inventory.
# A generic image-level probe would conflate role dependencies and liveness, so none is inherited.
HEALTHCHECK NONE

RUN test -x /app/.venv/bin/kokoro-agent-worker \
    && test -x /app/.venv/bin/kokoro-agent-evidence \
    && test -x /app/.venv/bin/kokoro-agent-presentation

ENTRYPOINT []
CMD ["kokoro-agent-worker"]

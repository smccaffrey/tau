# Tau daemon image.
#   docker build -t tau-pipelines .
#   docker run -p 8400:8400 -e TAU_API_KEY=change-me tau-pipelines
FROM python:3.15-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

RUN pip install --no-cache-dir "uv>=0.5,<1"

WORKDIR /app
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev --all-extras --no-install-project
COPY src ./src
RUN uv sync --frozen --no-dev --all-extras

RUN useradd --create-home --uid 1000 tau && mkdir -p /data /pipelines && chown tau:tau /data /pipelines
USER tau

ENV PATH="/app/.venv/bin:$PATH" \
    TAU_HOME=/data \
    TAU_DATABASE_URL=sqlite+aiosqlite:////data/tau.db \
    TAU_PIPELINES_DIR=/data/pipelines \
    TAU_ARTIFACTS_DIR=/data/artifacts

WORKDIR /data
VOLUME ["/data"]
EXPOSE 8400

HEALTHCHECK --interval=15s --timeout=3s --start-period=10s \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8400/health')" || exit 1

CMD ["taud", "--host", "0.0.0.0", "--port", "8400"]

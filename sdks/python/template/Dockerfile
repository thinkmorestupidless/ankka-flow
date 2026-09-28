# The process container: only the streamlet's code. No ports are exposed; the sidecar in the same
# pod dials 127.0.0.1:$FLOW_PROCESS_PORT.
FROM python:3.12-slim
RUN pip install --no-cache-dir "uv>=0.11,<0.12"
WORKDIR /app
COPY pyproject.toml uv.lock* ./
COPY src ./src
RUN uv sync --no-dev --frozen || uv sync --no-dev
ENV PATH="/app/.venv/bin:$PATH"
CMD ["python", "-m", "{{module}}.main"]

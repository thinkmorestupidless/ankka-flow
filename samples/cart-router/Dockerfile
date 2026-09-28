# The cart router's process image: the streamlet and the SDK, nothing else. No ports are exposed;
# the sidecar in the same pod dials 127.0.0.1:$FLOW_PROCESS_PORT.
#
# Build from the repository root, so the SDK can be installed from its source:
#   docker build -f samples/cart-router/Dockerfile -t sample-cart-router .
FROM python:3.12-slim
# uv from PyPI rather than its container registry, which not every machine can pull from.
RUN pip install --no-cache-dir "uv>=0.11,<0.12"
ENV UV_LINK_MODE=copy UV_COMPILE_BYTECODE=1

WORKDIR /src
COPY sdks/python/ sdks/python/
# Generate the SDK's protobuf code from its committed copy of the protocol, then drop the tooling.
RUN cd sdks/python && rm -rf .venv src/ankka_flow/_proto \
    && uv run --frozen python scripts/proto.py && rm -rf .venv

COPY samples/cart-router/pyproject.toml samples/cart-router/uv.lock samples/cart-router/
COPY samples/cart-router/src samples/cart-router/src
WORKDIR /src/samples/cart-router
RUN uv sync --frozen --no-dev

ENV PATH="/src/samples/cart-router/.venv/bin:$PATH" PYTHONUNBUFFERED=1
CMD ["python", "-m", "cart_router.main"]

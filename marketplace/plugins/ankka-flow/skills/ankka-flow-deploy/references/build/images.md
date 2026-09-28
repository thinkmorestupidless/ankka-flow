# Build an image

> Package a streamlet as a container image that holds only its process — no Kafka client, no exposed ports — and make it available to a cluster.

Source: https://flow.ankka.cloud/build/images/
A streamlet's image holds its process and nothing else. The platform adds the sidecar to every pod, and
the sidecar owns everything Kafka, so the image needs no Kafka client, no broker address, no
credentials, no exposed ports and no health checks. The process listens on
`127.0.0.1:$FLOW_PROCESS_PORT`, which the platform sets to `9010`, and the sidecar in the same pod
dials it over loopback.

## A Python streamlet's Dockerfile

The sample cart router's Dockerfile installs the SDK from its source in the repository and the
streamlet from its lock file, and runs the entry point that calls `serve`:

```dockerfile
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
```

Build it from the repository root:

```bash
docker build -f samples/cart-router/Dockerfile -t sample-cart-router .
```

What to keep from it in your own streamlet's image:

- **No `EXPOSE` and no `HEALTHCHECK`.** Nothing outside the pod talks to the process. The sidecar
  reports readiness and liveness for the pod.
- **`PYTHONUNBUFFERED=1`.** The process's log is where the sidecar's refusals appear: when the
  descriptor does not match, the sidecar sends every problem to the process to log before it refuses to start.
- **Only the streamlet's code.** Tests, the descriptor file and development dependencies stay out of
  the image; `uv sync --no-dev` leaves out the dependency group that holds pytest.
- **The same declaration as the committed descriptor.** The sidecar compares what the running process
  declares with the descriptor the pipeline was deployed with, and refuses to start on any difference.
  Build the image from the same commit as the `flow/descriptor.json` you deploy.

## Tags

An image is named per streamlet at deploy time with `flow generate --image <streamlet>=<reference>` or a
file passed to `--images`; nothing in the blueprint names an image. [Deploy a
pipeline](../deploy/deploy-a-pipeline.md) describes both.

A Docker tag may not contain `+`. A version string that has one, such as an sbt snapshot version,
needs it replaced, conventionally with `-`; the ankka-flow build tags its own images that way.

## Getting the image to a cluster

A cluster pulls images from a registry: push the image, and give `flow generate` the full reference.

A kind cluster can take an image straight from your local Docker instead:

```bash
kind load docker-image --name ankka sample-cart-router:latest
```

The operator renders every container with `imagePullPolicy: IfNotPresent`, so an image loaded into
the cluster's nodes is used as it is and never pulled.

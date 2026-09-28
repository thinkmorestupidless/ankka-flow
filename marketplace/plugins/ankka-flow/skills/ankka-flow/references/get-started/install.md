# Build the tools

> Install what ankka-flow's build needs, then build the flow CLI, the sidecar and operator images, and the sample streamlet's image from source.

Source: https://flow.ankka.cloud/get-started/install/
ankka-flow is built from its repository. Nothing is installed from a package manager: the `flow` CLI,
the sidecar image and the operator image all come out of one sbt build, and the Python SDK is used
from its source directory.

## What you need

| Tool | Used for |
|---|---|
| Docker | building images, and the Kafka and sidecar containers of the laptop loop |
| JDK 21 and sbt | building the CLI, the sidecar and the operator |
| uv, with Python 3.12 or later | the Python SDK and the sample streamlet |
| kind and kubectl | a local Kubernetes cluster, for deploying a pipeline |
| just (optional) | short names for the commands on this page; every recipe is one command you can run yourself |

Clone the repository and work from its root:

```bash
git clone https://github.com/thinkmorestupidless/ankka-flow.git
cd ankka-flow
```

## The `flow` CLI

`flow` verifies a blueprint, writes the `AnkkaFlow` resource a cluster runs, and requests resets. It
runs on the JVM. Build it with sbt, then put its directory on your `PATH`:

```bash
sbt cli/stage
export PATH="$PWD/cli/target/universal/stage/bin:$PATH"
flow version
```

`just cli` runs the same build and prints the `export` line for you. `flow version` prints the CLI's
version and the protocol version it writes, for example `flow 0.1.0, protocol 1.0`.

## The images

```bash
sbt docker:publishLocal sampleImage
```

This builds three images into your local Docker:

| Image | What it is |
|---|---|
| `ankka-flow-sidecar` | the container the platform adds beside every streamlet; it owns everything Kafka |
| `ankka-flow-operator` | the Kubernetes operator that runs `AnkkaFlow` resources |
| `sample-cart-router` | the sample Python streamlet, holding only its own code and the SDK |

Each is tagged with the build's version and with `latest`. A snapshot version contains a `+`, which a
Docker tag may not, so the tag has a `-` in its place. `just images` is the same command.

The laptop loop needs only the sidecar image: `sbt sidecar/docker:publishLocal` builds that one alone.

## The Python SDK

The SDK is in `sdks/python`. A streamlet project depends on it by path, so `uv sync` in the project
installs it; nothing needs building first. To run the SDK's own checks:

```bash
cd sdks/python && uv sync && uv run python scripts/proto.py && uv run mypy && uv run pytest -q
```

## What to do with them

- [Your first streamlet](first-streamlet.md) runs the sample on a laptop, with Kafka and the sidecar in
  containers.
- [Deploy to a local cluster](deploy-locally.md) runs the same streamlet on kind with the operator.

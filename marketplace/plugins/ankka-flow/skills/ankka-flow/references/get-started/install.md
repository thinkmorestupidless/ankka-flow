# Install the tools

> Install the flow CLI with Homebrew or from a release archive, pull or build the sidecar, operator and sample images, and set up the Python SDK.

Source: https://flow.ankka.cloud/get-started/install/
The `flow` CLI verifies a blueprint, writes the `AnkkaFlow` resource a cluster runs, and requests
resets. It is one native executable for macOS (Apple silicon and Intel) and Linux (x64 and arm64),
with no JVM to install. The sidecar and operator images are published to a registry with every release.

## The `flow` CLI

With Homebrew, on macOS or Linux:

```bash
brew install thinkmorestupidless/tap/ankka-flow
flow version
```

`flow version` prints the CLI's version and the protocol version it writes, for example
`flow 0.1.0, protocol 1.0`. The tap is the one ankka's CLI ships through, so
`brew install thinkmorestupidless/tap/ankka` installs `ankka` beside `flow`; the two coexist, and
`brew upgrade` moves each to its latest release.

### From a release archive

Every [release](https://github.com/thinkmorestupidless/ankka-flow/releases) attaches one archive per
platform, with a checksum beside it:

| Platform | Archive |
|---|---|
| macOS, Apple silicon | `ankka-flow-cli-<version>-macos-arm64.tar.gz` |
| macOS, Intel | `ankka-flow-cli-<version>-macos-x64.tar.gz` |
| Linux, arm64 | `ankka-flow-cli-<version>-linux-arm64.tar.gz` |
| Linux, x64 | `ankka-flow-cli-<version>-linux-x64.tar.gz` |

Download the archive and its `.sha256`, verify the checksum, unpack the one file inside, and put it on
your `PATH`:

```bash
version=0.1.0
platform=macos-arm64                                         # or macos-x64, linux-arm64, linux-x64
base=https://github.com/thinkmorestupidless/ankka-flow/releases/download/v$version
curl -fsSLO "$base/ankka-flow-cli-$version-$platform.tar.gz"
curl -fsSLO "$base/ankka-flow-cli-$version-$platform.tar.gz.sha256"
shasum -a 256 -c "ankka-flow-cli-$version-$platform.tar.gz.sha256"
tar -xzf "ankka-flow-cli-$version-$platform.tar.gz"          # unpacks one file: flow
install -m 755 flow /usr/local/bin/flow                      # or any directory on your PATH
flow version
```

A checksum that does not match means the download is not the release's file; download it again rather
than run it.

On macOS, Gatekeeper may refuse to run a binary that was downloaded by a browser. Allow it under
System Settings → Privacy & Security → Open Anyway, or clear the quarantine mark:

```bash
xattr -d com.apple.quarantine flow
```

A `flow` downloaded with `curl` carries no quarantine mark, and neither does Homebrew's.

A platform outside these four builds the CLI from source, which runs on a JVM;
[Build ankka-flow from source](../contributing/building.md) covers it.

## The images

Each release publishes the platform's images, and the samples', to `ghcr.io/thinkmorestupidless` with
the release's version as the tag:

| Image | What it is |
|---|---|
| `ankka-flow-sidecar` | the container the platform adds beside every streamlet; it owns everything Kafka |
| `ankka-flow-operator` | the Kubernetes operator that runs `AnkkaFlow` resources |
| `sample-cart-router-scala` | the sample Scala streamlet, holding its code, the SDK and a Java runtime |
| `sample-cart-router` | the sample Python streamlet, holding only its own code and the SDK |

```bash
docker pull ghcr.io/thinkmorestupidless/ankka-flow-sidecar:0.1.0
docker pull ghcr.io/thinkmorestupidless/ankka-flow-operator:0.1.0
docker pull ghcr.io/thinkmorestupidless/sample-cart-router-scala:0.1.0
docker pull ghcr.io/thinkmorestupidless/sample-cart-router:0.1.0
```

[Install the platform](../deploy/install.md) puts the operator and sidecar images on a cluster of your
own. The two tutorials that follow work from a clone of the repository and build the images they need
with sbt, so they can run the sample's code as you change it; [Build ankka-flow from
source](../contributing/building.md) lists what that takes.

## What you need for the tutorials

| Tool | Used for |
|---|---|
| Docker | the Kafka and sidecar containers of the laptop loop, and building the sample's image |
| uv, with Python 3.12 or later | the Python SDK and sample, and the scripts that send and check the tutorial's events |
| JDK 21 and sbt | the Scala SDK and sample, building the sidecar image for the laptop loop, and the images `just up` loads into kind |
| kind and kubectl | a local Kubernetes cluster, for deploying a pipeline |
| just (optional) | short names for the commands on the pages; every recipe is one command you can run yourself |

Clone the repository and work from its root:

```bash
git clone https://github.com/thinkmorestupidless/ankka-flow.git
cd ankka-flow
```

## The Scala SDK

A Scala streamlet project depends on the SDK from Maven Central. It needs Scala 3.3 or later, a JDK 21
or later, and sbt:

```scala
libraryDependencies += "com.thinkmorestupidless" %% "ankka-flow-sdk" % "<version>"
```

The version is the ankka-flow release whose sidecar runs the streamlet. In the repository the SDK is
`sdks/scala`, and the Scala sample builds against it directly. To run the SDK's own checks and the
conformance suite against it:

```bash
sbt sdk/test sdkConformance
```

## The Python SDK

The SDK is in `sdks/python`. A streamlet project depends on it by path, so `uv sync` in the project
installs it; nothing needs building first. To run the SDK's own checks:

```bash
cd sdks/python && uv sync && uv run python scripts/proto.py && uv run mypy && uv run pytest -q
```

## What to do with them

`flow init` starts a streamlet project of your own, in Scala or Python, with its test, descriptor,
blueprint, image build and laptop setup:

```bash
flow init greeter                 # or: flow init greeter -l python
```

- [Your first streamlet](first-streamlet.md) starts from `flow init`, runs the project on a laptop with
  Kafka and the sidecar in containers, and then shows the sidecar at work with the sample.
- [Deploy to a local cluster](deploy-locally.md) runs the same streamlet on kind with the operator.

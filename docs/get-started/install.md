---
title: Install the tools
description: Install the flow CLI with Homebrew or from a release archive, pull the operator and sample images, and add the SDK of each engine as one dependency.
kind: tutorial
related: [get-started/first-streamlet.md, get-started/deploy-locally.md, reference/cli.md, concepts/engines.md]
---

# Install the tools

The `flow` CLI verifies a blueprint, writes the settings that run streamlets on a laptop, writes the
`AnkkaFlow` resource a cluster runs, and requests resets. It is one native executable for macOS
(Apple silicon and Intel) and Linux (x64 and arm64), with no JVM to install. The operator's image is
published to a registry with every release, and each engine's SDK to Maven Central.

## The `flow` CLI

With Homebrew, on macOS or Linux:

```bash
brew install thinkmorestupidless/tap/ankka-flow
flow version
```

`flow version` prints the CLI's version and the descriptor version it reads, as
`flow <version>, descriptor 2.0`. The tap is the one ankka's CLI ships through, so
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
version="<version>"                                         # the release, such as 1.2.0
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

A platform outside these four has no released binary; [Build ankka-flow from
source](../contributing/building.md) is for it.

## The images

Each release publishes the operator's image, and the sample's two streamlets' images, to
`ghcr.io/thinkmorestupidless` with the release's version as the tag:

| Image | What it is |
|---|---|
| `ankka-flow-operator` | the Kubernetes operator that runs `AnkkaFlow` resources |
| `sample-cart-cleaner` | the sample's Pekko Streams streamlet: a Java runtime with its classes |
| `sample-cart-counter` | the sample's Flink streamlet: Flink's own image with its jar |

```bash
docker pull ghcr.io/thinkmorestupidless/ankka-flow-operator:<version>
```

There is no image to add beside a streamlet: each streamlet's own image is all that runs it.
[Install the platform](../deploy/install.md) puts the operator on a cluster of your own, with the
Flink Kubernetes Operator it needs for Flink streamlets.

## The SDKs

A streamlet is written against one engine, and its project depends on that engine's SDK, at the
release of `flow` that verifies and deploys it.

A Pekko Streams streamlet, in Scala 3.3 or later on Java 21:

<!-- languages: scala; the Pekko Streams SDK is a Scala library -->
```scala
libraryDependencies += "com.thinkmorestupidless" %% "ankka-flow-pekko-streams" % "<version>"
```

A Flink streamlet, in Java 17 or later, with Flink's own modules `provided` by the image it runs in:

<!-- languages: scala; an sbt build line, the build the samples use; a Maven build is on the Flink streamlet page -->
```scala
libraryDependencies += "com.thinkmorestupidless" % "ankka-flow-flink" % "<version>"
```

Its test harness is `com.thinkmorestupidless:ankka-flow-flink-testkit`, in the test scope.
[Write a Pekko Streams streamlet](../build/pekko-streams-streamlet.md) and [Write a Flink
streamlet](../build/flink-streamlet.md) set up each project in full.

## What you need for the tutorials

| Tool | Used for |
|---|---|
| Docker | the Kafka container of the laptop walkthrough, and building the sample's images |
| JDK 21 and sbt | building, testing and running the sample's two streamlets |
| kind and kubectl | a local Kubernetes cluster, for deploying the pipeline |
| just (optional) | short names for the commands on the pages; every recipe is one command you can run yourself |

Clone the repository and work from its root:

```bash
git clone https://github.com/thinkmorestupidless/ankka-flow.git
cd ankka-flow
```

## What to do with them

The sample in `samples/cart-pipeline` is the place to start: a pipeline of a Pekko Streams streamlet
and a Flink streamlet, with their tests, descriptors, blueprint and image builds.

- [Your first streamlet](first-streamlet.md) runs the sample on a laptop, with Kafka in a container
  and each streamlet as a command on the host.
- [Deploy to a local cluster](deploy-locally.md) runs the same pipeline on kind with both operators.

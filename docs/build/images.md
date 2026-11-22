---
title: Build an image
description: Package a streamlet as a container image for its engine — a Java runtime for a Pekko Streams streamlet, Flink's own image for a Flink streamlet — and make it available to a cluster.
kind: guide
languages: [scala, java]
related: [build/pekko-streams-streamlet.md, build/flink-streamlet.md, deploy/deploy-a-pipeline.md]
---

# Build an image

A streamlet's image holds its code and what its engine needs to run it. The platform gives it its
settings, a file mounted into the pod, and nothing else: the image needs no broker address, no
credentials and no exposed ports.

## A Pekko Streams streamlet

The image is a Java runtime with the streamlet's classes and dependencies, started by the main
class that calls `PekkoStreamlets.run`. With sbt-native-packager's `JavaAppPackaging` and
`DockerPlugin`, the cleaner's build is:

<!-- languages: scala; a Pekko Streams streamlet is written in Scala, and built with sbt -->
```scala
lazy val cartCleaner = project
  .in(file("samples/cart-pipeline/cleaner"))
  .enablePlugins(JavaAppPackaging, DockerPlugin)
  .settings(
    dockerBaseImage      := "eclipse-temurin:21-jre",
    Docker / packageName := "sample-cart-cleaner",
    Compile / mainClass  := Some("cart.cleaner.Main")
  )
```

```bash
sbt cartCleaner/docker:publishLocal     # sample-cart-cleaner:<version> and :latest
```

The operator runs it as a Deployment with one container, `streamlet`. Its readiness and liveness
are files the SDK writes, so the image needs no health check of its own.

## A Flink streamlet

The image is Flink's own, at the Flink version the streamlet was built against, with the streamlet's
jar at `/opt/flink/usrlib/streamlet.jar` and every dependency Flink does not provide beside it in
`usrlib/`. Flink puts `usrlib/` on the job's classpath; the Flink Kubernetes Operator starts the job
from the jar and the streamlet's entry class. The counter's Dockerfile:

<!-- include: samples/cart-pipeline/counter/Dockerfile -->
```dockerfile
# The counter's image: Flink's own, with the streamlet's jar and the dependencies Flink does not
# provide in usrlib, which Flink puts on the job's classpath. `sbt cartCounter/image` stages the
# jars in target/image and builds this file there.
ARG FLINK_IMAGE
FROM ${FLINK_IMAGE}
COPY --chown=flink:flink usrlib/ /opt/flink/usrlib/
```

The build stages the jars into a directory beside the Dockerfile and builds there:

```bash
sbt cartCounter/image     # sample-cart-counter:<version>
```

Flink's own modules, `flink-streaming-java` and the connectors' Flink runtime among them, are
`provided`: the image has them, and a second copy in `usrlib/` breaks class loading. The SDK and the
Kafka connector it uses are not part of Flink's distribution, so they go in `usrlib/`.

## Both

- **A log that reaches the pod's output at once.** An slf4j binding writing to standard output:
  Logback in the cleaner; Flink's own in its image.
- **Only the streamlet's code.** Tests and development dependencies stay out of the image.
- **The same declaration as the committed descriptor.** The streamlet refuses to start when its
  settings do not match its declared ports and parameters. Build the image from the same commit as the
  descriptor you deploy.

## Tags

An image is named per streamlet at deploy time with `flow generate --image <streamlet>=<reference>` or a
file passed to `--images`; nothing in the blueprint names an image. [Deploy a
pipeline](../deploy/deploy-a-pipeline.md) describes both.

A Docker tag may not contain `+`. A version string that has one, such as an sbt snapshot version,
needs it replaced, conventionally with `-`; the ankka-flow build tags its own images that way.

## Getting the image to a cluster

A cluster pulls images from a registry: push the image, and give `flow generate` the full reference.

A kind cluster can take an image straight from your local Docker instead. With Docker's containerd
image store, save one platform and load the archive:

```bash
docker save --platform linux/arm64 sample-cart-counter:latest | kind load image-archive --name ankka /dev/stdin
```

The operator renders every container with `imagePullPolicy: IfNotPresent`, so an image loaded into
the cluster's nodes is used as it is and never pulled.

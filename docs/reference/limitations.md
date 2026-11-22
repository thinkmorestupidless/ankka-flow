---
title: Limitations
description: What ankka-flow does not do — delivery per engine, engines, state, contracts, credentials, ankka's limits, ingress, brokers and tooling — stated plainly so a design does not depend on it.
kind: reference
related: [concepts/delivery.md, concepts/engines.md, concepts/contracts.md, concepts/pipelines.md]
---

# Limitations

What ankka-flow does not do, stated plainly so a design does not depend on it.

## Delivery

- **A Pekko Streams streamlet is at least once.** A record whose results were written but whose
  offset was not yet committed is read again after a failure or a rebalance, and its results are
  written again. Its logic, and every reader after it, must tolerate repeats.
- **A Flink streamlet counts each record once between managed topics, and is at least once into
  ankka.** Its outlets write in transactions only to topics the pipeline manages. Into a topic it
  does not manage, an ankka topic among them, a restart may write again what it wrote since its
  last checkpoint.
- **Downstream of a Flink streamlet, records arrive one checkpoint late.** A reader of a managed
  topic sees only committed records, and a Flink streamlet's records commit with its checkpoint.
- **No dead-letter topic and no skipping.** A record that fails every time is read again every
  time, and its partition makes no progress; skipping a record is the streamlet's own decision.

See [Delivery and failure](../concepts/delivery.md).

## Engines

- **Two engines: Pekko Streams and Flink.** No Spark and no other engine, and no layer that runs one
  streamlet's logic on more than one.
- **Flink streamlets in Java.** No Flink SQL, no streamlet that is only a query, and no Flink
  streamlet in Python. Scala on Flink's Java API may work and is neither documented nor tested.
- **Pekko Streams streamlets in Scala.** The SDK is a Scala library for Scala 3.3 or later. There is
  no Java API for it, and no artifact for Scala 2.13.
- **No windows, timers or state in a Pekko Streams streamlet.** A stage that needs any of them is a
  Flink streamlet.
- **The Flink Kubernetes Operator is a prerequisite.** ankka-flow's operator neither installs nor
  upgrades it, and refuses a pipeline with a Flink streamlet where it is missing.
- **No templates.** A streamlet starts from the sample pipeline, not from a generated project.

## State

- **A reset leaves the earlier state in the store.** A reset Flink streamlet starts at a new state
  epoch with no state; the files of the earlier epoch stay until the store's own lifecycle rules
  remove them.
- **A new image keeps state only where Flink can carry it.** A POJO state type may gain or lose
  fields; a retyped or renamed field, or a changed type Flink stores with Kryo, leaves the job unable
  to read its state.
- **One state store per installation.** Every Flink streamlet checkpoints to the store the Secret
  `flink-state` names.

## Kafka and ankka

- **A streamlet holds the credentials of its own topics.** Each streamlet's container reads the
  connection settings, credentials included, of the topics its own ports are bound to, and of no
  other.
- **Kafka only.** No other broker, and no single topic spread over several Kafka clusters.
- **Existing topics are never changed, their cleanup policy included.** The operator creates a
  managed topic once; a different partition count, replication or configuration on an existing topic
  is reported, not applied.
- **ankka limits a machine's tokens and connections.** ankka answers a client id's token requests
  at most twelve times a minute and caps connections per address, so many replicas starting at once
  may be told to slow down, and retry.
- **Fields ankka encrypts stay envelopes.** A streamlet reading an ankka topic sees an encrypted
  field as ankka wrote it; nothing decrypts it.
- **No call into an ankka service.** A streamlet reaches ankka through topics only, not over a
  granted route or gRPC method.
- **Nothing is registered in ankka from `flow`.** The machine, its grants and the project's topic
  declarations are made with `ankka`.

## Contracts

- **JSON contracts only.** Avro and Protobuf contracts, schema registries and compatibility rules
  between schema versions are not supported, and nothing checks a record against its schema. A new
  contract version is a new contract name.

## Operating

- **No metrics endpoint.** Lag is on the pipeline's status; a Flink streamlet has Flink's own
  reporters, and nothing is scraped from a Pekko Streams streamlet. Nothing scales a streamlet by
  its lag.
- **No HTTP or gRPC ingress into a pipeline.** Records enter a pipeline through a Kafka topic.
- **No connectors beyond Kafka and ankka in the SDKs.** A Pekko Streams streamlet may use any
  library its author adds.
- **No user interface and no hosted control plane.** A pipeline is a Kubernetes resource, operated
  with `kubectl` and the `flow` CLI.
- **Four platforms for the native CLI.** `flow` ships as a native binary for macOS (Apple silicon
  and Intel) and Linux (x64 and arm64), through Homebrew or a release archive. Anywhere else it is
  built from source and runs on a JVM. There is no Windows build and no apt, dnf or winget package.

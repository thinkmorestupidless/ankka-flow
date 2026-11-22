---
title: ankka-flow
description: Streaming pipelines beside ankka — streamlets written against Pekko Streams or Apache Flink, wired by a blueprint over Kafka topics whose contracts are checked before anything runs.
kind: concept
related: [concepts/pipelines.md, get-started/first-streamlet.md, reference/limitations.md]
---

# ankka-flow

ankka-flow runs streaming pipelines beside [ankka](https://docs.ankka.cloud/). A pipeline is a graph of
**streamlets**, each with named, typed inlets and outlets, connected by Kafka **topics** according to a
**blueprint**. A streamlet is written against one engine's own API, [Pekko Streams](build/pekko-streams-streamlet.md)
or [Apache Flink](build/flink-streamlet.md), and shipped as an image of its own; the platform runs it
on that engine, creates its topics and reports its state and lag.

```text
shop.cart-events.v1 ──► cleaner ──► carts.clean-carts ──► counter ──► carts.cart-counts
                      Pekko Streams                       Flink
```

## What it promises

- **Contracts are checked before anything runs.** Every port declares a contract, and `flow verify`
  refuses a blueprint whose connected ports disagree, with every problem listed in one pass, whatever
  the engines on either side.
- **Each engine keeps its own promise.** A Pekko Streams streamlet commits a record only once
  everything it produced is written: at least once. A Flink streamlet counts each record once between
  topics the pipeline manages, its state in checkpoints that survive a pod's failure and a new image.
- **Nothing is skipped behind your back.** A record that fails is retried from the last commit or
  checkpoint until it succeeds. Dropping one is the streamlet's own decision.
- **A streamlet holds only what it needs.** Its settings carry the credentials of its own topics,
  and nothing else.
- **ankka's topics are first-class.** A blueprint names an ankka project's topic by project and name,
  checks its contracts against the project's declarations, and reaches it as a machine the project
  grants it to.

## When to use it

A single service reacting to a topic is an ankka consumer and should stay one. ankka-flow is the layer
above, for a graph of stages with typed outlets, topics the platform owns, per-key ordering through a
chain, windows and state, lag per stage, and a rebuild from the start of the inputs.
[Pipelines and streamlets](concepts/pipelines.md) says where the line falls.

## Where to start

| To | Read |
|---|---|
| run a streamlet on a laptop in ten minutes | [Your first streamlet](get-started/first-streamlet.md) |
| run a pipeline on a local Kubernetes cluster | [Deploy to a local cluster](get-started/deploy-locally.md) |
| choose an engine for a stage | [Choosing an engine](concepts/engines.md) and [Delivery and failure](concepts/delivery.md) |
| write a streamlet | [Write a Pekko Streams streamlet](build/pekko-streams-streamlet.md) or [a Flink streamlet](build/flink-streamlet.md) |
| read and write an ankka project's topics | [Read and write ankka topics](build/ankka-topics.md) |
| wire streamlets together | [Write a blueprint](build/blueprints.md) |
| deploy and operate pipelines | [Deploy a pipeline](deploy/deploy-a-pipeline.md) and [Observe a pipeline](deploy/observe.md) |
| look a fact up | [CLI](reference/cli.md), [AnkkaFlow resource](reference/resource.md), [Descriptor](reference/descriptor.md) |
| know what it does not do | [Limitations](reference/limitations.md) |
| give this documentation to a coding agent | [Work with a coding agent](get-started/coding-agents.md) |

ankka-flow descends from Lightbend's [Cloudflow](https://github.com/lightbend/cloudflow) by way of a
fork that moved it to Apache Pekko. Neither is a dependency.

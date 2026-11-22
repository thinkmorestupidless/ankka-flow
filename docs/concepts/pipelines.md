---
title: Pipelines and streamlets
description: What a pipeline is — streamlets with typed inlets and outlets, wired by a blueprint over Kafka topics — and when a design needs one rather than an ankka consumer.
kind: concept
related: [concepts/engines.md, concepts/contracts.md, concepts/topics.md, build/blueprints.md]
---

# Pipelines and streamlets

A **pipeline** is a graph of **streamlets** connected by Kafka **topics**. Each streamlet has named,
typed **inlets** and **outlets**, together called its ports. A **blueprint** says which streamlets a
pipeline uses and which topic connects which ports. The platform checks the blueprint before anything
runs.

```text
shop.cart-events.v1 ──► cleaner ──► carts.clean-carts ──► counter ──► carts.cart-counts
                      Pekko Streams                       Flink
```

## Where ankka ends and ankka-flow begins

An ankka consumer already reads a Kafka topic, and can produce to one. A single service reacting to a
topic is an ankka consumer and should stay one.

ankka-flow is the layer above. Reach for it when a design needs several of these:

- a graph of stages whose contracts are checked against each other before anything runs;
- several typed outlets per stage, with fan-out;
- topics the platform creates and owns;
- per-key ordering preserved through a chain of stages;
- a pipeline rebuilt from the start of its inputs;
- lag attributed to each stage;
- windows and state kept across records, in an engine built for them.

If a design needs none of those, it is not a flow.

## A streamlet

A streamlet is code written against one engine's own API, in its own container image:
[Pekko Streams](../build/pekko-streams-streamlet.md) for a stage that cleans, reshapes or routes
each record, [Apache Flink](../build/flink-streamlet.md) for one that keeps windows or state.
[Choosing an engine](engines.md) says which need selects which. A streamlet declares its ports,
each with a [contract](contracts.md), and its typed parameters; the engine reads its inlets'
topics and writes its outlets' topics itself.

The streamlet's SDK writes a **descriptor** from its declaration when the streamlet is built. The
descriptor is the streamlet as the platform sees it: its name, its engine, its ports, contracts and
parameters. A blueprint is checked against descriptors, never against code, so checking a pipeline
needs no image, no network and no engine. The format is on
[Descriptor](../reference/descriptor.md).

A blueprint gives each streamlet a name of its own and names the descriptor it runs, so one streamlet
can appear twice in a pipeline under two names.

## Topics connect ports

A topic in a blueprint lists the outlets that produce to it and the inlets that consume from it. Every
inlet is connected to exactly one topic; an outlet is connected to at most one, and an unconnected
outlet is allowed. The outlets and inlets on one topic must carry equal contracts.

A topic is either **managed**, created and owned by the pipeline, **unmanaged**, owned by something
else and only read, or an **ankka topic**, declared by an ankka project and read or written as a
machine the project grants it to. [Topics and Kafka clusters](topics.md) explains both, how a topic's Kafka name is
chosen, and where its connection settings come from.

## From blueprint to cluster

![ankka-flow on Kubernetes: a developer or CI job writes an AnkkaFlow resource with flow generate and applies it. The operator, in namespace ankka-flow, watches AnkkaFlow resources in every namespace, reads the kafka-cluster Secrets, creates the pipeline's managed topics in Kafka, and runs each streamlet by its engine: a Pekko Streams streamlet as a Deployment, a Flink streamlet as a FlinkDeployment the Flink Kubernetes Operator runs, its checkpoints in a state store. Each streamlet pod reads and writes Kafka itself, with the credentials of its own topics only, and the operator writes the pipeline's status back.](../assets/diagrams/platform.svg)

1. The SDK writes each streamlet's descriptor.
2. `flow verify` checks the blueprint against the descriptors and reports every problem in one pass.
3. `flow generate` writes the `AnkkaFlow` resource: every descriptor, image, binding and topic, with
   deploy-time configuration already merged in.
4. The operator creates the managed topics and runs each streamlet by its engine: a Pekko Streams
   streamlet as a Deployment, a Flink streamlet as a FlinkDeployment that the Flink Kubernetes
   Operator runs.

[Write a blueprint](../build/blueprints.md) walks through steps 1 and 2;
[Deploy a pipeline](../deploy/deploy-a-pipeline.md) covers 3 and 4.

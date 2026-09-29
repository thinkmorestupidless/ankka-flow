# ankka-flow

> Streaming pipelines beside ankka — streamlets in any language, wired by a blueprint over Kafka topics, with a sidecar in every pod that owns everything Kafka.

Source: https://flow.ankka.cloud/
ankka-flow runs streaming pipelines beside [ankka](https://docs.ankka.cloud/). A pipeline is a graph of
**streamlets**, each with named, typed inlets and outlets, connected by Kafka **topics** according to a
**blueprint**. A streamlet's logic is written in any language and shipped as an image that holds only
that code. A **sidecar** the platform adds to every pod owns everything Kafka: subscribing, batching,
producing, committing only after the write, consumer groups, lag and resets.

```text
shop.cart-events.v1 ──► router ──valid──► cart.valid-carts
                                └review─► cart.review-carts
```

## What it promises

- **Contracts are checked before anything runs.** Every port declares a contract, and `flow verify`
  refuses a blueprint whose connected ports disagree, with every problem listed in one pass.
- **Offsets are committed only after the write.** A batch's offsets are committed once every record it
  emitted is confirmed by the broker, so nothing is lost. Delivery is at least once.
- **Nothing is skipped behind your back.** A failed batch is redelivered from the last commit until it
  succeeds. Skipping a record is the streamlet's own decision.
- **Your container holds only your code.** It has no Kafka client, no ports, no probes and no secrets;
  Kafka credentials reach only the sidecar.
- **Any language.** A streamlet speaks a small gRPC protocol on the pod's loopback interface. The Python
  SDK implements it; any other language can, and proves it with the conformance suite.
- **Generic work ships with the platform.** A built-in streamlet runs inside the sidecar with no image
  of its own: the [Neo4j merge sink](reference/neo4j-merge-sink.md) turns a topic of graph deltas into a
  graph that is the same however often, or in whatever order, the deltas arrive.

## When to use it

A single service reacting to a topic is an ankka consumer and should stay one. ankka-flow is the layer
above, for a graph of stages with typed outlets, topics the platform owns, per-key ordering through a
chain, lag per stage, and a rebuild from the start of the inputs.
[Pipelines and streamlets](concepts/pipelines.md) says where the line falls.

## Where to start

| To | Read |
|---|---|
| run a streamlet on a laptop in ten minutes | [Your first streamlet](get-started/first-streamlet.md) |
| run a pipeline on a local Kubernetes cluster | [Deploy to a local cluster](get-started/deploy-locally.md) |
| understand how a pod behaves | [The sidecar](concepts/sidecar.md) and [Delivery and failure](concepts/delivery.md) |
| write a streamlet | [Write a streamlet in Python](build/python-streamlet.md) |
| wire streamlets together | [Write a blueprint](build/blueprints.md) |
| deploy and operate pipelines | [Deploy a pipeline](deploy/deploy-a-pipeline.md) and [Observe a pipeline](deploy/observe.md) |
| look a fact up | [CLI](reference/cli.md), [AnkkaFlow resource](reference/resource.md), [Streamlet protocol](reference/protocol.md) |
| know what it does not do | [Limitations](reference/limitations.md) |
| give this documentation to a coding agent | [Work with a coding agent](get-started/coding-agents.md) |

ankka-flow descends from Lightbend's [Cloudflow](https://github.com/lightbend/cloudflow) by way of a
fork that moved it to Apache Pekko. Neither is a dependency.

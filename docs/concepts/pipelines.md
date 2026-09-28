# Pipelines

A **pipeline** is a graph of **streamlets** connected by Kafka **topics**. Each streamlet has
named, typed **inlets** and **outlets**. A **blueprint** says which streamlets a pipeline uses and
which topic connects which ports. The platform checks the blueprint before anything runs.

```
shop.cart-events.v1 ──► router ──valid──► cart.valid-carts ──► sink
                                └review─► cart.review-carts
```

## Where ankka ends and ankka-flow begins

An ankka consumer already reads a Kafka topic, and can produce to one. A single service reacting to
a topic is an ankka consumer and should stay one.

ankka-flow is the layer above. Reach for it when you need several of these:

- a graph of stages whose contracts are checked against each other before anything runs;
- several typed outlets per stage, with fan-out;
- topics the platform creates and owns;
- per-key ordering preserved through a chain of stages;
- a pipeline rebuilt from the start of its inputs;
- lag attributed to each stage;
- sinks that batch and commit only after writing somewhere else.

If a design needs none of those, it is not a flow.

## A streamlet

A streamlet is your code, in any language, in its own container image. It declares its ports and
their contracts, and a function that takes a batch of records and emits records to outlets. Its
SDK writes a **descriptor** from that declaration at build time. The descriptor is what the
blueprint is checked against. The Python SDK is in `sdks/python`; see [the Python SDK](../sdk/python.md).

## Topics

A topic is **managed** when the pipeline owns it: the operator creates it with the declared
partitions and replication. An existing managed topic is left as it is; a difference is reported as
a warning, never applied.

A topic is **unmanaged** when something else owns it, such as an ankka service's event topic. The
platform only reads it: it never creates, alters or deletes it, and a blueprint may not produce to
it. An unmanaged topic names its brokers or a Kafka cluster.

A managed topic's Kafka name defaults to `<pipeline>.<topic id>`, so two pipelines with a
`valid-carts` topic do not collide. `topic.name` overrides it.

## From blueprint to cluster

1. The SDK writes each streamlet's descriptor (`uv run descriptor`).
2. `flow verify` checks the blueprint against the descriptors: every problem, in one pass.
3. `flow generate` writes the `AnkkaFlow` resource: every descriptor, image, binding and topic.
4. The operator creates the managed topics and runs each streamlet as a pod, with the sidecar beside
   your container.

See [the sidecar](sidecar.md) for what happens in each pod, and [contracts](contracts.md) for how
ports are checked.

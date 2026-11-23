# Glossary

> Every term ankka-flow uses — streamlet, engine, port, blueprint, contract, descriptor, state store, machine, grant and the rest — each defined in a sentence with the page that explains it.

Source: https://flow.ankka.cloud/reference/glossary/
The documentation uses each of these terms in exactly this sense.

### AnkkaFlow

The Kubernetes resource that is one deployed pipeline: its streamlets, their images and settings, and
its topics. `flow generate` writes it and the operator runs it. See
[AnkkaFlow resource](resource.md).

### ankka consumer

An ankka service's component that reads a topic and acts on each message. A single service reacting to
a topic is one, not a pipeline. See [Choosing an engine](../concepts/engines.md).

### ankka topic

A topic an ankka project declares, named in a blueprint as `ankka = "<project>/<name>"`. The pipeline
reads or writes it as a machine and never creates it. See
[Read and write ankka topics](../build/ankka-topics.md).

### At least once

A record may be delivered more than once and is never lost. What a Pekko Streams streamlet promises,
and what a Flink streamlet promises into a topic the pipeline does not manage. See
[Delivery and failure](../concepts/delivery.md).

### Blueprint

The HOCON file that names a pipeline's streamlets and the topics that connect their ports. See
[Write a blueprint](../build/blueprints.md).

### Checkpoint

A Flink streamlet's state and its read positions, written to the state store at an interval. A failed
pod resumes from the last one. See [Write a Flink streamlet](../build/flink-streamlet.md).

### Client id

The Kafka client id of a port, `<pipeline>.<streamlet>.<port>`. See
[Topics and Kafka clusters](../concepts/topics.md).

### Commit after the write

A Pekko Streams streamlet's rule: an offset is committed only once every record produced from it is
confirmed by the broker. See [Delivery and failure](../concepts/delivery.md).

### Condition

A named fact on a pipeline's status, such as `TopicNotGranted` or `StateUnreadable`, with a reason and
a message saying what to do. See [Observe a pipeline](../deploy/observe.md).

### Consumer group

The Kafka consumer group of an inlet, `<pipeline>.<streamlet>.<inlet>`, prefixed with the machine's
prefix on a machine's cluster. See [Topics and Kafka clusters](../concepts/topics.md).

### Contract

What a port's records are: a name and, optionally, a schema's fingerprint. Two ports on a topic must
carry agreeing contracts. See [Contracts](../concepts/contracts.md).

### Declarations

An ankka project's topics with their contracts, as `ankka projects topics list -o json` prints them,
which `--ankka-topics` checks a blueprint against. See
[Read and write ankka topics](../build/ankka-topics.md).

### Deploy-time configuration

Settings given when a pipeline is deployed rather than written in its blueprint: replicas,
parallelism, Flink's resources, parameters, topic settings. See
[Configure at deploy time](../deploy/configuration.md).

### Descriptor

The JSON file a streamlet's SDK writes from its declaration: its name, engine, ports, contracts and
parameters. See [Descriptor](descriptor.md).

### Engine

What runs a streamlet: Pekko Streams or Apache Flink. A streamlet is written against its engine's own
API. See [Choosing an engine](../concepts/engines.md).

### Fingerprint

`sha256:` and the hex SHA-256 of a contract's schema in RFC 8785 canonical form, as ankka computes it.
See [Contracts](../concepts/contracts.md).

### Flink operator

The Apache Flink Kubernetes Operator, which runs each Flink streamlet's FlinkDeployment. See
[Install the platform](../deploy/install.md).

### Flink streamlet

A streamlet written against Apache Flink's DataStream API, for windows and state. See
[Write a Flink streamlet](../build/flink-streamlet.md).

### Grant

A project's permission for a machine to consume or produce one of its topics. See
[Read and write ankka topics](../build/ankka-topics.md).

### Harness

An SDK's test support, which runs a streamlet's logic on records in a test with no broker. See
[Test a streamlet](../build/testing.md).

### Inlet

A port a streamlet reads, bound to one topic. See [Pipelines and streamlets](../concepts/pipelines.md).

### Kafka cluster

A Secret `kafka-cluster-<name>` beside the operator: brokers, client properties, defaults, and
optionally the machine it is reached as. See [Topics and Kafka clusters](../concepts/topics.md).

### Lag

The records on an inlet's topic its consumer group has not committed, on the pipeline's status. See
[Observe a pipeline](../deploy/observe.md).

### Machine

A system outside an ankka installation, registered on an organization, that reads and writes the
topics a project grants it. A pipeline reaches ankka topics as one. See
[Read and write ankka topics](../build/ankka-topics.md).

### Managed topic

A topic the pipeline owns: the operator creates it and, when asked, deletes it with the pipeline. See
[Topics and Kafka clusters](../concepts/topics.md).

### Operator

The ankka-flow operator, which runs every `AnkkaFlow` in the cluster. See [Operator](operator.md).

### Outlet

A port a streamlet writes, bound to at most one topic. See
[Pipelines and streamlets](../concepts/pipelines.md).

### Parallelism

How many parallel tasks a Flink streamlet's job runs. See
[Configure at deploy time](../deploy/configuration.md).

### Parameter

A typed setting a streamlet declares, with an optional default, given a value at deploy time. See
[Descriptor](descriptor.md).

### Pekko Streams streamlet

A streamlet written with Pekko Streams' operators, for stages that need neither windows nor state. See
[Write a Pekko Streams streamlet](../build/pekko-streams-streamlet.md).

### Pipeline

A graph of streamlets connected by Kafka topics, described by a blueprint and deployed as one
`AnkkaFlow`. See [Pipelines and streamlets](../concepts/pipelines.md).

### Port

An inlet or an outlet, named and carrying a contract. A blueprint addresses it as `<streamlet>.<port>`.
See [Blueprint](blueprint.md).

### Reset

Moving a pipeline's consumer groups back to the earliest offset, and starting a Flink streamlet from an
empty state, so its streamlets reread their inputs from the start. See
[Rebuild from the start](../deploy/reset.md).

### State

What a Flink streamlet keeps between records, such as a window's count. It survives a failed pod and a
new image, and a reset discards it. See [Write a Flink streamlet](../build/flink-streamlet.md).

### State epoch

The number of a Flink streamlet's state directory in the state store, which a reset moves on. See
[Rebuild from the start](../deploy/reset.md).

### State store

The object store a Flink streamlet's checkpoints go to, named by the Secret `flink-state`. See
[Install the platform](../deploy/install.md).

### Streamlet

One stage of a pipeline, with typed inlets and outlets: logic written against one engine's own API, in
its own image. See [Pipelines and streamlets](../concepts/pipelines.md).

### Streamlet settings

The file `streamlet.properties` a streamlet is given: its names, parameters, and each port's topic,
group and Kafka properties. See [Streamlet settings](streamlet-settings.md).

### Unmanaged topic

A topic that belongs to something else. The pipeline only reads it and never creates, alters or deletes
it. See [Topics and Kafka clusters](../concepts/topics.md).

### Window

A span of time a Flink streamlet groups records by, such as a minute, by the time the records carry.
See [Write a Flink streamlet](../build/flink-streamlet.md).

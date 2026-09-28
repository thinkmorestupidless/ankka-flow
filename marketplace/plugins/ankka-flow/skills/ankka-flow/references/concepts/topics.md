# Topics and Kafka clusters

> Managed and unmanaged topics, how a topic's Kafka name is chosen, how its brokers and settings resolve from the blueprint and a Kafka cluster Secret, and how consumer groups and client ids are named.

Source: https://flow.ankka.cloud/concepts/topics/
A topic in a blueprint has an id, the ports that produce to and consume from it, and the Kafka
settings it needs. Whether the pipeline owns the Kafka topic behind it decides what the platform may
do with it.

## Managed topics

A topic is **managed** unless its blueprint entry says `managed = false`. The pipeline owns it: the
operator creates it with the declared partitions, replication and topic configuration.

The operator creates a managed topic once and never changes it afterwards. When a managed topic
already exists with a different partition count or replication, the operator leaves it as it is and
records a `TopicDiffers` warning; a changed topic configuration value on an existing topic is recorded
as `TopicSettingsIgnored`. Changing a live topic is an operation for Kafka's own tools.

A managed topic needs a partition count and a replication factor from somewhere: the blueprint, a
deploy-time override, or the defaults of its Kafka cluster. A managed topic with neither is refused
and nothing in the pipeline is applied.

When the `AnkkaFlow` resource is deleted, its managed topics are kept. Setting
`spec.onDelete.managedTopics: Delete` on the resource deletes them with it.

## Unmanaged topics

A topic with `managed = false` belongs to something else, such as an ankka service's event topic. The
platform only reads it: it never creates, alters or deletes it, and a blueprint may not produce to it.
An unmanaged topic must say where it lives, by `bootstrap.servers` or by `cluster`.

The sidecar reads with automatic topic creation turned off, so subscribing to an unmanaged topic that
does not exist never creates it. The operator records `TopicMissing`, and the streamlets consuming it
stay not ready until it exists.

## Kafka names

A topic's id is the name the blueprint and the resource use for it. Its Kafka name is:

1. `topic.name`, when the blueprint sets it;
2. otherwise, for a managed topic, `<pipeline>.<topic id>`, so two pipelines with a `valid-carts`
   topic do not collide;
3. otherwise, for an unmanaged topic, the topic id itself.

An unmanaged topic almost always sets `topic.name`, because its name was chosen by whoever owns it.

## Kafka clusters

A Kafka cluster is a Secret named `kafka-cluster-<name>` in the operator's namespace (`ankka-flow`
unless the operator is configured otherwise). It holds `bootstrap.servers`, which is required,
optional `connection-config`, `producer-config` and `consumer-config` as Java properties text, and
optional default `partitions` and `replicas` for managed topics that do not set their own. A topic
names its cluster with `cluster = <name>`.

Credentials live only in these Secrets and in the sidecar containers they are mounted into. A
blueprint, a resource and a streamlet's own container never hold them.

## How a topic's settings resolve

The CLI merges deploy-time overrides over the blueprint when it writes the resource, so the resource
holds the topic's own settings. The operator then resolves each topic against its cluster:

1. The cluster is the one the topic names. A topic that names no cluster and no `bootstrap.servers`
   uses the cluster named `default`. A topic that sets `bootstrap.servers` and no cluster uses no
   cluster at all.
2. The brokers are the topic's `bootstrap.servers`, else the cluster's.
3. Partitions and replicas are the topic's, else the cluster's defaults. They apply to managed topics
   only.
4. `connection-config`, `producer-config` and `consumer-config` are the cluster's with the topic's own
   values laid over them, key by key.

A topic that names a cluster with no Secret is refused, naming the Secret it expected.

## Consumer groups and client ids

Every inlet consumes in its own consumer group and every port has its own client id, named from the
pipeline, the streamlet and the port:

| Identity | Name |
|---|---|
| consumer group of an inlet | `<pipeline>.<streamlet>.<inlet>` |
| client id of an inlet or outlet | `<pipeline>.<streamlet>.<port>` |

The replicas of one streamlet share their inlets' groups, so Kafka divides each inlet's partitions
between them. Two streamlets reading one topic have separate groups and each read every record. Lag
in the sidecar's metrics is labelled with the client id, and `flow reset` moves the groups back to
the earliest offset.

A new consumer group starts at the earliest offset, so a new pipeline reads its inputs from the
beginning. A topic's `consumer-config { auto.offset.reset = latest }` changes that for its consumers.

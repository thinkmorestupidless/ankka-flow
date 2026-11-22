---
title: Topics and Kafka clusters
description: Managed and unmanaged topics, how a topic's Kafka name is chosen, how its brokers and settings resolve from the blueprint and a Kafka cluster Secret, and how consumer groups and client ids are named.
kind: concept
related: [concepts/pipelines.md, reference/blueprint.md, deploy/configuration.md, reference/resource.md, build/ankka-topics.md]
---

# Topics and Kafka clusters

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

When the `AnkkaFlow` resource is deleted, its managed topics are kept. A resource generated with
`flow generate --delete-managed-topics` carries `spec.onDelete.managedTopics: Delete` and deletes them
with it.

## Unmanaged topics

A topic with `managed = false` belongs to something else. The platform only reads it: it never
creates, alters or deletes it, and a blueprint may not produce to it. An unmanaged topic must say
where it lives, by `bootstrap.servers` or by `cluster`.

Every streamlet reads with automatic topic creation turned off, so subscribing to an unmanaged topic
that does not exist never creates it. The operator records `TopicMissing`, and the streamlets consuming it
stay not ready until it exists.

## ankka topics

A topic with `ankka = "<project>/<name>"` is one an ankka project declares. Like an unmanaged topic,
the platform never creates, alters or deletes it; unlike one, a pipeline may write it as well as
read it, as the machine the project grants it to. Its Kafka name is `<project>.<name>`, and it is on
the cluster named `ankka` unless it names another or deploy-time configuration sets
`flow.ankka.cluster`. [Read and write ankka topics](../build/ankka-topics.md) covers it end to end.

## Kafka names

A topic's id is the name the blueprint and the resource use for it. Its Kafka name is:

1. `topic.name`, when the blueprint sets it;
2. otherwise, for a managed topic, `<pipeline>.<topic id>`, so two pipelines with a `valid-carts`
   topic do not collide;
3. otherwise, for an ankka topic, `<project>.<name>`;
4. otherwise, for an unmanaged topic, the topic id itself.

An unmanaged topic almost always sets `topic.name`, because its name was chosen by whoever owns it.

## Kafka clusters

A Kafka cluster is a Secret named `kafka-cluster-<name>` in the operator's namespace (`ankka-flow`
unless the operator is configured otherwise). It holds `bootstrap.servers`, which is required,
optional `connection-config`, `producer-config` and `consumer-config` as Java properties text, and
optional default `partitions` and `replicas` for managed topics that do not set their own, and,
for a cluster reached as an ankka machine, `machine = <organization>/<name>`. A topic names its
cluster with `cluster = <name>`.

Credentials live in these Secrets, and the operator copies into each streamlet's settings the
credentials of that streamlet's own topics only. A blueprint and a resource never hold them.

## How a topic's settings resolve

The CLI merges deploy-time overrides over the blueprint when it writes the resource, so the resource
holds the topic's own settings. The operator then resolves each topic against its cluster:

1. The cluster is the one the topic names. An ankka topic that names none uses `flow.ankka.cluster`,
   else `ankka`; any other topic that names no cluster and no `bootstrap.servers` uses the cluster
   named `default`. A topic that sets `bootstrap.servers` and no cluster uses no
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

On a machine's cluster the group carries the machine's prefix too, the only groups ankka lets a
machine use: `ankka.machine.<organization>.<name>.<pipeline>.<streamlet>.<inlet>`.

The replicas of a Pekko Streams streamlet share their inlets' groups, so Kafka divides each inlet's
partitions between them; a Flink streamlet divides them between its parallel tasks and commits the
group's offsets on each checkpoint. Two streamlets reading one topic have separate groups and each
read every record. Each inlet's lag is on the pipeline's status, and `flow reset` moves the groups
back to the earliest offset.

A new consumer group starts at the earliest offset, so a new pipeline reads its inputs from the
beginning. A topic's `consumer-config { auto.offset.reset = latest }` changes that for its consumers.

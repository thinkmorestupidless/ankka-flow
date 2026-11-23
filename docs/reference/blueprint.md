---
title: Blueprint
description: Every key of the blueprint file, the HOCON that names a pipeline's streamlets and the topics connecting their ports, and every rule flow verify checks it against.
kind: reference
related: [build/blueprints.md, concepts/topics.md, reference/cli.md, deploy/configuration.md]
---

# Blueprint

A blueprint is a HOCON file, conventionally `blueprint.conf`, that names the streamlets a pipeline
uses and the topics that connect their ports. `flow verify` and `flow generate` read it together with a
directory of streamlet descriptors.

<!-- include: samples/cart-pipeline/blueprint.conf -->
```hocon
# The sample pipeline: the shop's cart events, cleaned, then counted per cart in windows of time.
blueprint {
  name = carts
  streamlets {
    cleaner = cart-cleaner
    counter = cart-counter
  }
  topics {
    cart-events {
      managed    = false
      topic.name = "shop.cart-events.v1"
      cluster    = shop
      consumers  = [cleaner.in]
      consumer-config { auto.offset.reset = earliest }
    }
    clean-carts {
      producers  = [cleaner.out]
      consumers  = [counter.in]
      partitions = 3
    }
    cart-counts {
      producers  = [counter.out]
      partitions = 3
    }
  }
}
```

## `blueprint.name`

Optional. The pipeline id when `flow generate` is given no `--pipeline`; without either, the pipeline
id is the blueprint file's name up to its first dot. A pipeline id is 1 to 40 characters of `a-z`,
`0-9` and `-`, not starting or ending with `-`. It prefixes managed topics' Kafka names, consumer
groups, client ids and the names of everything the operator creates.

## `blueprint.streamlets`

Required, and must name at least one streamlet. Each key is a streamlet's name in this pipeline; each
value is the name of the descriptor it runs, the `streamlet.name` in one of the descriptors given with
`--descriptors`.

```hocon
streamlets {
  cleaner    = cart-cleaner
  cleaner-eu = cart-cleaner     # the same descriptor, a second streamlet
}
```

A streamlet name is a DNS label: at most 63 characters of `a-z`, `0-9` and `-`, not starting or ending
with `-`. Two streamlets may not share a name.

The descriptor says the streamlet's engine; the blueprint never does, so streamlets of different
engines share one blueprint and one topic.

## `blueprint.topics`

Each key is a topic id. Each value is an object with these keys, all optional:

| Key | Type | Meaning |
|---|---|---|
| `producers` | list of port paths | outlets that write to the topic |
| `consumers` | list of port paths | inlets that read from the topic |
| `managed` | boolean, default `true` | `false`: the topic belongs to something else and is only read |
| `ankka` | `"<project>/<name>"` | the topic is that ankka project's; see [ankka topics](#ankka-topics) |
| `topic.name` | string | the Kafka topic name; defaults to `<pipeline>.<id>` for a managed topic and `<id>` for an unmanaged one |
| `cluster` | string | the Kafka cluster, a `kafka-cluster-<name>` Secret beside the operator |
| `bootstrap.servers` | string | brokers, instead of or over the cluster's |
| `partitions` | integer | partitions of a managed topic when the operator creates it |
| `replicas` | integer | replication factor of a managed topic when the operator creates it |
| `topic` | object | Kafka topic configuration for a managed topic, such as `retention.ms` |
| `connection-config` | object | Kafka client properties for every client of the topic, such as `security.protocol` |
| `producer-config` | object | Kafka producer properties for the topic's producers |
| `consumer-config` | object | Kafka consumer properties for the topic's consumers |

A port path is `<streamlet>.<port>`: `cleaner.out` is the `out` port of the streamlet named `cleaner`
in `blueprint.streamlets`.

Nested objects are flattened into Kafka property names, so `topic { retention.ms = 86400000 }` and
`topic.retention.ms = 86400000` are the same setting. How settings left out here are filled from a
Kafka cluster is on [Topics and Kafka clusters](../concepts/topics.md).

A topic with no producers and no consumers is ignored: it does not reach the resource and no Kafka
topic is created for it.

### ankka topics

A topic with `ankka = "<project>/<name>"` is a topic the ankka project declares. Its Kafka name is
`<project>.<name>`; it is unmanaged, so the operator never creates, alters or deletes it; it may have
producers as well as consumers; and it is on the Kafka cluster `ankka` unless it names another with
`cluster`, or deploy-time configuration names one with `flow.ankka.cluster`. It takes neither
`managed = true`, `topic.name` nor `bootstrap.servers`. `--ankka-topics` checks its ports' contracts
against the project's declarations; [Read and write ankka topics](../build/ankka-topics.md) covers
it end to end.

## Deploy-time overrides

Everything under `blueprint.topics.<id>` except `producers`, `consumers`, `managed` and `topic.name`
can be overridden at deploy time under `flow.topics.<id>` in a `--conf` file, and a streamlet's
replicas, parameters and engine settings under `flow.streamlets.<name>`.
[Configure at deploy time](../deploy/configuration.md) describes them.

## Verification rules

`flow verify` and `flow generate` report every problem in one pass and refuse when there is any. The
blueprint is refused when:

- the file is not valid HOCON, has no `blueprint.streamlets` section, or names no streamlets;
- it has a `blueprint.connections` section, an older format: connect ports through
  `blueprint.topics`;
- the descriptor directory holds no descriptors, a descriptor is invalid, names no engine or one
  `flow` does not know, or two descriptors declare the same streamlet;
- a streamlet name is not a DNS label, or two streamlets share a name;
- a streamlet names a descriptor that no descriptor declares;
- a port path is not of the form `<streamlet>.<port>`;
- a port path names a streamlet or port that does not exist (the message suggests the ports the
  streamlet has);
- a producer is an inlet, or a consumer is an outlet;
- a port is bound to more than one topic;
- an inlet is connected to no topic;
- two ports on one topic have different contracts, whatever their engines (the message names both
  ports and both contracts);
- a topic's Kafka name has characters other than `a-z`, `A-Z`, `0-9`, `.`, `_` and `-`, or is longer
  than 249 characters;
- a cluster name is not a DNS label;
- an unmanaged topic that is not an ankka topic has producers, or names neither `bootstrap.servers`
  nor `cluster`;
- an ankka topic is not `<project>/<name>`, or also sets `managed = true`, `topic.name` or
  `bootstrap.servers`;
- with `--ankka-topics`, an ankka topic its project does not declare, declares with no contract, or
  declares with a contract a port on it does not carry;
- a topic sets `consumer-config.flow.batch`, which is no longer a setting;
- a parameter has no default in its descriptor and no value in `--conf`, or a value that is not its
  type;
- a `--conf` file names a topic or streamlet the blueprint does not, or a parameter the streamlet does
  not declare, gives a replica count that is negative or not a number, gives a Flink streamlet
  `replicas` other than 0 or 1, or gives a Pekko Streams streamlet `parallelism` or `flink`.

`flow generate` also refuses when a streamlet has no image or the pipeline id is not valid.

An outlet connected to no topic is allowed: `flow verify` prints it as a note and carries on. So
is an ankka topic whose project's declarations were not given:

```text
note: Outlet cleaner.audit is not connected.
note: ankka topics of project 'checkout' were not checked: pass --ankka-topics checkout=<file>
```

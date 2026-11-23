---
title: Configure at deploy time
description: Override a blueprint's topics, replicas, parallelism, Flink settings and parameters per environment with --conf files, and point topics at Kafka clusters through kafka-cluster Secrets.
kind: guide
related: [deploy/deploy-a-pipeline.md, reference/blueprint.md, concepts/topics.md, reference/resource.md, reference/operator.md]
---

# Configure at deploy time

A blueprint says what a pipeline is. What differs between environments — partition counts, replica
counts, a Flink job's parallelism and resources, parameter values, which Kafka a topic lives on — is supplied when the resource is generated, in
two places:

- **`--conf` files**, merged into the resource by `flow generate`, so the generated resource says
  exactly what will run;
- **Kafka cluster Secrets** in the operator's namespace, which the operator reads for broker addresses,
  credentials and topic defaults. They never appear in the resource.

## `--conf` files

A `--conf` file is HOCON with two sections, one keyed by topic id and one by streamlet name:

```hocon
flow.topics.clean-carts { partitions = 12, topic { retention.ms = 604800000 } }
flow.topics.cart-events { bootstrap.servers = "shop-kafka:9092" }
flow.streamlets.cleaner { replicas = 3, config { max-total = 250 } }
flow.streamlets.counter {
  parallelism = 2
  flink { checkpoint-interval = 30s, task-manager { cpu = 1, memory = 2048m } }
}
```

```bash
flow generate blueprint.conf --descriptors flow --images images.conf --conf production.conf -n shop
```

`--conf` is repeatable, and a later file wins over an earlier one. `flow verify` accepts the same
files and checks them the same way.

### Topics

`flow.topics.<id>` takes the keys a topic takes in the blueprint, and each key it sets wins over the
blueprint's: `partitions`, `replicas`, `cluster`, `bootstrap.servers`, `connection-config`,
`producer-config`, `consumer-config` and `topic` (Kafka topic settings such as `retention.ms`).
`managed`, `ankka` and `topic.name` are read from the blueprint only: whether a pipeline owns a topic,
and what it is called in Kafka, is part of what the pipeline is. [Blueprint](../reference/blueprint.md)
describes each key.

### Streamlets

`flow.streamlets.<name>` takes:

| Key | Meaning |
|---|---|
| `replicas` | for a Pekko Streams streamlet, the number of pods, default 1; for a Flink streamlet `1` runs its job and `0` suspends it, keeping its state. `0` stops either without removing it |
| `parallelism` | a Flink streamlet only: the job's parallelism, default 1 |
| `flink.checkpoint-interval` | a Flink streamlet only: how often its job checkpoints, default `10s`. Downstream readers see its records this long after it writes them |
| `flink.job-manager`, `flink.task-manager` | a Flink streamlet only: `cpu` and `memory` of each, default `cpu = 1, memory = 1024m` |
| `config` | parameter values, by the parameter's key as the streamlet declares it |

A Flink key on a Pekko Streams streamlet is refused.

A parameter with no value here takes the default from the streamlet's descriptor. A parameter with
neither, a value that does not parse as the parameter's type, and a key the streamlet does not declare
are all refused. So is a topic id or streamlet name the blueprint does not have: a misspelt override
fails the build rather than being ignored.

### ankka topics

`flow.ankka.cluster` names the Kafka cluster every ankka topic is on unless the topic names its own,
usually the cluster Secret of the machine the pipeline acts as:

```hocon
flow.ankka.cluster = "pipelines"
```

Without it, an ankka topic is on the cluster named `ankka`.

## Kafka cluster Secrets

A Kafka cluster is a Secret named `kafka-cluster-<name>` in the namespace the operator reads clusters
from, `ankka-flow` unless the operator is configured otherwise. A topic uses:

1. the cluster its `cluster` key names;
2. otherwise, for an ankka topic, `flow.ankka.cluster`, else the `ankka` cluster;
3. otherwise, when it sets `bootstrap.servers` itself, no cluster;
4. otherwise the `default` cluster.

A topic that names a cluster with no Secret refuses the whole pipeline.

| Key | Meaning |
|---|---|
| `bootstrap.servers` | required |
| `connection-config` | Java properties text for every Kafka client of the topic: security protocol, SASL, TLS |
| `producer-config`, `consumer-config` | Java properties text for producers or consumers only |
| `partitions`, `replicas` | defaults for a managed topic that sets neither |
| `machine` | `<organization>/<name>`: the ankka machine the cluster is reached as. Its consumer groups carry the prefix `ankka.machine.<organization>.<name>.`, and the operator checks its grants on every ankka topic |

A named cluster, as a Secret:

<!-- include: samples/cart-pipeline/k8s/kafka-cluster-shop.yaml -->
```yaml
# A named Kafka cluster the operator resolves topics against: a topic with `cluster = shop` uses
# these brokers and settings. It lives in the operator's namespace. Never commit real credentials.
apiVersion: v1
kind: Secret
metadata:
  name: kafka-cluster-shop
  namespace: ankka-flow
  labels:
    flow.ankka.thinkmorestupidless.com/kafka-cluster: shop
stringData:
  bootstrap.servers: kafka.kafka.svc:9092
  connection-config: |
    security.protocol=PLAINTEXT
```

A machine's cluster, with the properties ankka documents for one:

<!-- include: samples/cart-pipeline/k8s/kafka-cluster-ankka.yaml#kafka-cluster-ankka -->
```yaml
apiVersion: v1
kind: Secret
metadata:
  name: kafka-cluster-ankka
  namespace: ankka-flow
  labels:
    flow.ankka.thinkmorestupidless.com/kafka-cluster: ankka
stringData:
  bootstrap.servers: broker.<base domain>:9094
  machine: affiliates/network
  connection-config: |
    security.protocol=SASL_SSL
    sasl.mechanism=OAUTHBEARER
    sasl.login.callback.handler.class=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler
    sasl.oauthbearer.token.endpoint.url=https://api.<base domain>/oauth/token
    sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required clientId="machine:affiliates/network" clientSecret="<the machine's secret>";
```

The operator reads the Secret, not the pipeline's namespace, so one set of credentials serves every
pipeline and no pipeline author handles them. The resolved connection settings of a streamlet's own
topics, credentials included, reach that streamlet in its [settings](../reference/streamlet-settings.md),
a Secret mounted into its pods; a streamlet never holds the settings of another's topics.

## Which setting wins

For each topic setting, in order:

1. the `--conf` files, later over earlier;
2. the blueprint;
3. the topic's Kafka cluster Secret.

Steps 1 and 2 are merged by `flow generate`, so the resource already holds their result. Step 3 is the
operator's, when it runs the resource. The properties blocks merge key by key: a topic's own
`consumer-config` entries win over its cluster's, and the cluster's other entries still apply.

A managed topic that has no `partitions` or `replicas` after all three steps is refused, with a message
naming the Secret where a default could go.

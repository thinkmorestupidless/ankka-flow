# Configure at deploy time

> Override a blueprint's topics, replica counts and parameters per environment with --conf files, and point topics at Kafka clusters through kafka-cluster Secrets.

Source: https://flow.ankka.cloud/deploy/configuration/
A blueprint says what a pipeline is. What differs between environments — partition counts, replica
counts, parameter values, which Kafka a topic lives on — is supplied when the resource is generated, in
two places:

- **`--conf` files**, merged into the resource by `flow generate`, so the generated resource says
  exactly what will run;
- **Kafka cluster Secrets** in the operator's namespace, which the operator reads for broker addresses,
  credentials and topic defaults. They never appear in the resource.

## `--conf` files

A `--conf` file is HOCON with two sections, one keyed by topic id and one by streamlet name:

```hocon
flow.topics.valid-carts { partitions = 12, topic { retention.ms = 604800000 } }
flow.topics.cart-events { bootstrap.servers = "shop-kafka:9092" }
flow.streamlets.router  { replicas = 3, config { review-threshold = 250 } }
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
`managed` and `topic.name` are read from the blueprint only: whether a pipeline owns a topic, and what
it is called in Kafka, is part of what the pipeline is. [Blueprint](../reference/blueprint.md)
describes each key.

An inlet's batch size is set here too, under `consumer-config.flow.batch`:

```hocon
flow.topics.cart-events { consumer-config.flow.batch { max-records = 500, max-bytes = 4 MiB } }
```

A batch is whatever arrived while the previous batch of that partition was in flight, capped at
`max-records` records (default 100) and `max-bytes` (default 1 MiB).

### Streamlets

`flow.streamlets.<name>` takes two keys:

| Key | Meaning |
|---|---|
| `replicas` | the number of pods; default 1; `0` stops the streamlet without removing it |
| `config` | parameter values, by the parameter's key as the streamlet declares it |

A parameter with no value here takes the default from the streamlet's descriptor. A parameter with
neither, a value that does not parse as the parameter's type, and a key the streamlet does not declare
are all refused. So is a topic id or streamlet name the blueprint does not have: a misspelt override
fails the build rather than being ignored.

## Kafka cluster Secrets

A Kafka cluster is a Secret named `kafka-cluster-<name>` in the namespace the operator reads clusters
from, `ankka-flow` unless the operator is configured otherwise. A topic uses:

1. the cluster its `cluster` key names;
2. otherwise, when it sets `bootstrap.servers` itself, no cluster;
3. otherwise the `default` cluster.

A topic that names a cluster with no Secret refuses the whole pipeline.

| Key | Meaning |
|---|---|
| `bootstrap.servers` | required |
| `connection-config` | Java properties text for every Kafka client of the topic: security protocol, SASL, TLS |
| `producer-config`, `consumer-config` | Java properties text for producers or consumers only |
| `partitions`, `replicas` | defaults for a managed topic that sets neither |

A named cluster, as a Secret:

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

The operator reads the Secret, not the pipeline's namespace, so one set of credentials serves every
pipeline and no pipeline author handles them. The resolved connection settings reach each streamlet's
sidecar in a Secret mounted into the sidecar container only. The streamlet's own container never sees
a broker address or a credential.

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

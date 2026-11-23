# Deploy a pipeline

> Verify a blueprint, generate the AnkkaFlow resource with each streamlet's image, apply it, and update, roll out or delete the pipeline afterwards.

Source: https://flow.ankka.cloud/deploy/deploy-a-pipeline/
A pipeline is deployed as one `AnkkaFlow` resource. The `flow` CLI writes it from three inputs — the
blueprint, the streamlets' descriptors and their images — and optionally deploy-time configuration.
`kubectl apply` hands it to the operator, which creates the managed topics and runs each streamlet
by its engine: a Pekko Streams streamlet as a Deployment, a Flink streamlet as a `FlinkDeployment`
the Flink Kubernetes Operator runs.

## Verify

```bash
flow verify blueprint.conf --descriptors flow
```

`flow verify` reads every `*.json` file in the `--descriptors` directory as a streamlet descriptor and
checks the blueprint against them: every streamlet names a descriptor that exists, every port path
names a declared port, every inlet is connected, the contracts on each topic match, unmanaged topics
have no producers, every parameter has a value of its type, and every deploy-time setting suits its
streamlet's engine. It lists every problem in one pass and
exits 1, or prints `verified: <n> streamlets, <m> topics`. It needs no image, no network and no
language runtime, so it belongs in a streamlet's own build.

An outlet connected to nothing is allowed; `verify` prints a note for it.

## Generate

```bash
flow generate blueprint.conf --descriptors flow \
  --image cleaner=registry.example.com/cart-cleaner:1.2.0 \
  --image counter=registry.example.com/cart-counter:1.2.0 \
  --conf production.conf -n shop -o carts.yaml
```

`flow generate` does everything `verify` does, then writes the resource as YAML to the file named by
`-o`, or to standard output. Nothing is written when any check fails.

| Option | Meaning |
|---|---|
| `--image <streamlet>=<reference>` | a streamlet's image; repeatable |
| `--images <file>` | a HOCON file mapping streamlet names to images; `--image` wins over it for the same streamlet |
| `--conf <file>` | deploy-time configuration; repeatable, later files win ([Configure at deploy time](configuration.md)) |
| `--pipeline <id>` | the pipeline id; default the blueprint's `blueprint.name`, else the blueprint file's name |
| `--version <v>` | the pipeline's version; default `git describe --tags --always --dirty` in the blueprint's directory, else `unversioned` |
| `-n`, `--namespace <ns>` | the resource's namespace |
| `-o`, `--output <file>` | where to write the YAML |

Every streamlet needs an image; one without is refused as `streamlet <name>: no image`. An images file
is one line per streamlet:

```hocon
# The image of each streamlet in the blueprint, by its name there: what `flow generate --images` reads.
cleaner = "registry.example.com/cart-cleaner:0.6.0"
counter = "registry.example.com/cart-counter:0.6.0"
```

The pipeline id names the resource and prefixes everything the operator creates for it. It is 1 to 40
characters of lower-case letters, digits and `-`, neither starting nor ending with `-`.

The generated resource says exactly what will run: deploy-time configuration is already merged in, and
each streamlet carries its engine and descriptor. The only things the operator adds are the Kafka
connection settings from the cluster Secrets and, for a Flink streamlet, the state store from
`flink-state`.

## Apply

```bash
kubectl apply -f carts.yaml
kubectl -n shop get aflow carts -w
```

The operator refuses the whole resource, applying nothing, when it cannot run it: a topic names a
Kafka cluster with no Secret, a managed topic has no partition count or replication from anywhere, a
descriptor is invalid or names no engine, or the pipeline has a Flink streamlet and the cluster lacks
the Flink Kubernetes Operator or `flink-state`. Otherwise it creates each missing managed topic first,
then per streamlet a Secret of its settings and a Deployment or a `FlinkDeployment`, each named
`flow-<pipeline>-<streamlet>`. The phase goes from `Pending` to `Ready` when every streamlet runs and
every topic check passes.
[Observe a pipeline](observe.md) covers the phases and events.

Generating and applying can be one pipe, as long as nothing else needs the file:

```bash
flow generate blueprint.conf --descriptors flow --images images.conf -n shop | kubectl apply -f -
```

## Update

Change what needs changing — a new image, a changed descriptor, a parameter, a replica count — then
generate and apply again. Each streamlet rolls on its own: a hash of its rendered configuration is an
annotation on its pod template, so a streamlet rolls when its image, descriptor or configuration
changes and not otherwise. A Pekko Streams streamlet's rollout replaces one pod at a time and never
goes below the desired count. A Flink streamlet's job is stopped and started again from its last
checkpoint, with its state, on the new image; see
[Write a Flink streamlet](../build/flink-streamlet.md#state-and-a-new-image) for what a new image may
change. The operator records `StreamletRolled` for each streamlet it rolls.

Topics behave differently. An existing managed topic is never altered: a different partition count or
replication is a `TopicDiffers` warning, and a changed topic setting is a `TopicSettingsIgnored`
warning. Change an existing topic with Kafka's own tools.

A streamlet removed from the blueprint loses its Deployment or `FlinkDeployment` and its Secret, and
the operator records `StreamletRemoved`.

## Delete

```bash
kubectl -n shop delete aflow carts
```

Deleting the resource deletes every streamlet's Deployment or `FlinkDeployment` and its Secret. A Flink
streamlet's files in the state store stay. What happens to the managed
topics is the resource's `spec.onDelete.managedTopics`:

| Value | On delete |
|---|---|
| `Keep` (the default) | the managed topics and their records stay in Kafka |
| `Delete` | the operator deletes the managed topics before the resource goes |

`flow generate` writes `Keep` unless it is given `--delete-managed-topics`, which writes `Delete`: use it
when the topics, and the records in them, should go with the pipeline. Unmanaged topics are never
deleted.

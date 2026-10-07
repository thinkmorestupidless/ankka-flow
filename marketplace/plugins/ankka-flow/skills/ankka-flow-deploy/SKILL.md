---
name: ankka-flow-deploy
description: Install the flow CLI with Homebrew or from a release archive, install ankka-flow on Kubernetes, and deploy, configure, rebuild, observe and troubleshoot pipelines — the flow CLI (verify, generate, reset, version), the AnkkaFlow resource and its status, the operator and its settings, Kafka cluster Secrets, deploy-time overrides with --conf and images, managed topic creation, rollouts per streamlet, the sidecar's environment, probes and metrics, consumer lag, PartitionStalled and the operator's events, and resetting consumer groups to the earliest offset. Use when the task names flow verify/generate/reset, an AnkkaFlow resource, the operator, kind, kubectl, a Kafka cluster Secret, lag, a stalled partition, or a pipeline that is not Ready. Also the built-in Neo4j merge sink, with its connection Secret, refusals, metrics and readiness, compacted delta topics, TopicNotCompacted, and rebuilding a graph from its delta topic.
---

# Deploying and operating ankka-flow pipelines

A pipeline is deployed as one `AnkkaFlow` resource. `flow generate` writes it from a blueprint, the
streamlets' descriptors, their images and any deploy-time configuration; `kubectl apply` hands it to
the operator, which creates the managed topics and runs each streamlet as a Deployment whose pods hold
the streamlet's container and the sidecar.

## Rules

1. **Verify before generating.** `flow verify <blueprint> --descriptors <dir>` checks every contract,
   binding, topic and parameter in one pass with no image, network or runtime. `flow generate` does
   the same checks before writing anything. Exit codes: `0` ok, `1` refused (every problem on stderr),
   `2` usage.
2. **Deploy-time configuration is merged by the CLI.** `--conf` files override topics
   (`flow.topics.<id>`) and streamlets (`flow.streamlets.<name>`: `replicas`, `config`); `--image` or
   `--images` (`generate` only; `--image` wins for the same streamlet) name each streamlet's image.
   Managed topics are kept when the resource is deleted unless `generate` is given
   `--delete-managed-topics`. The generated resource says exactly what will run.
3. **The operator adds only what only it knows.** The sidecar image comes from the operator's own
   setting (`FLOW_SIDECAR_IMAGE`), never from a pipeline; upgrading the platform upgrades every
   sidecar on its next rollout. Kafka connection settings come from the `kafka-cluster-<name>` Secret
   in `FLOW_KAFKA_CLUSTERS_NAMESPACE` (default `ankka-flow`); a topic with no cluster and no bootstrap servers uses `default`.
4. **Kafka credentials reach only the sidecar.** They are mounted into the sidecar container. The
   streamlet's container has no ports, probes, mounts or secrets; do not add any.
5. **Managed topics are created once and never altered.** A different partition count or replication
   on an existing topic is a `TopicDiffers` warning, not a change. An unmanaged topic that does not
   exist is `TopicMissing`, and its consumers stay not ready.
6. **Each streamlet rolls on its own.** Changing one streamlet's image, descriptor or configuration
   rolls that streamlet only, one pod at a time, never below the desired count.
7. **Reset only what is stopped.** `flow reset <pipeline>` refuses unless every target streamlet has
   `replicas: 0` and no pods left. The operator moves each inlet's consumer group to the earliest
   offset, records `ResetOffsets` per group, and never repeats a reset it has carried out.
8. **Read status, then events, then the sidecar.** `kubectl get aflow` shows the phase (`Pending`, `Ready`,
   `Degraded` or `Failed`) and `-o wide` its detail; `status.streamlets` holds ready/desired counts; events on the `AnkkaFlow` say why; the sidecar's
   log and its metrics on port 2050 say what one pod is doing.
9. **A built-in streamlet takes no image and a Secret.** `graph = builtin/neo4j-merge-sink` needs no
   descriptor file or `--image` (an image is refused); its pod has only the sidecar. Its `secret`
   parameter names a Secret in the pipeline's namespace with `uri`, `username`, `password` and
   optionally `database`, which the operator mounts read-only at `/etc/flow/neo4j` with mode `0440`.
   A missing or incomplete Secret is `Refused`; the sink needs Neo4j 5.26 or later.
10. **A managed delta topic is compacted by default.** For a managed topic with a port of
   `ankka.graph-delta.v1` and no `cleanup.policy` set, `flow generate` writes `cleanup.policy: compact`
   into the resource and prints a note; a policy set in the blueprint or `--conf` is kept and the note
   says what it costs. An existing topic that is not compacted is left alone and reported as
   `TopicNotCompacted`. An unmanaged delta topic is its owner's.
11. **A graph is rebuilt from its delta topic by resetting the sink alone.** Set the sink's `replicas`
   to 0, empty the database, `flow reset <pipeline> --streamlet <sink>`, set `replicas` back. No
   mapper is stopped and no upstream topic is read; the rebuild is done when the sink's lag is zero.
   The rebuilt graph has every live element; a tombstoned element whose record a writer removed with
   a delete marker is not in it.
12. **The sink refuses a delta under the wrong key.** Every delta's record key must be `node:<id>` or
   `edge:<id>`; otherwise the batch fails and the partition stalls with
   `key '<found>' is not this delta's element key '<expected>'` or `no key; …`. Records with no value
   are delete markers: passed over and counted in `ankka_flow_stage_delete_markers_total`.
13. **`flow` is installed, not built.** `brew install thinkmorestupidless/tap/ankka-flow`, the tap
   ankka's CLI ships through (the two coexist), or a release's
   `ankka-flow-cli-<version>-<platform>.tar.gz` for `macos-arm64`, `macos-x64`, `linux-arm64` or
   `linux-x64`, verified with `shasum -a 256 -c <archive>.sha256`; one native binary, no JVM.
   `flow version` prints `flow <version>, protocol <major>.<minor>`. Another platform, or a change to
   ankka-flow, builds from source with sbt.

14. **Through `flow mcp`, deploy and observe with tools.** In a project from `flow init`,
   `apply_pipeline` verifies, generates and applies, `get_pipeline` shows phase, status and events,
   `pipeline_logs` a streamlet's process or sidecar log, `pipeline_lag` consumer lag per inlet
   partition, and `reset_pipeline` requests a reset with `flow reset`'s guards — all on the one
   cluster `flow.toml` names, never kubectl's current context. `apply_pipeline` and `reset_pipeline`
   change it and are confirmed before they run.

## Troubleshooting order

`kubectl get aflow <name>` → `kubectl get events --field-selector involvedObject.kind=AnkkaFlow` →
the sidecar container's log → its `/metrics` (lag under `client_id="<pipeline>.<streamlet>.<inlet>"`).
Common shapes: `SidecarImageMissing` means the operator has no sidecar image configured; a pod that
restarts with a descriptor difference in both containers' logs means the image and the deployed
descriptor disagree (the sidecar refuses at discovery and exits 1); `TopicMissing` means an unmanaged input does not exist yet; growing lag with a
`PartitionStalled` event means one batch fails every time and the streamlet's code must change; on a
merge sink, a note ending `is not this delta's element key '…'` means the writer keys its deltas wrongly
and both the writer and the topic's old records have to be replaced.

## Mistakes to check for

- Running `kubectl` against the shell's current context instead of the cluster `flow.toml` names.
- A guide or script that builds `flow` from source instead of installing it, or a `cli/target/…`
  path where `flow` on the `PATH` belongs.
- A sidecar image, Kafka address or credential written into the resource or the streamlet's image.
- `flow reset` against running streamlets, or scaling the Deployment directly instead of `replicas`.
- Expecting the operator to change an existing managed topic's partitions.
- A literal `:latest` image that the cluster cannot pull; on kind, load the image first.
- Reading lag without the client id; Kafka reports topic names with dots replaced by underscores.
- An `--image` for a built-in streamlet, or the Neo4j Secret in the operator's namespace instead of
  the pipeline's.
- Resetting the whole pipeline, mappers included, to refill a lost graph database when the delta topic
  is compacted; reset the sink alone.
- Expecting the operator to make an existing delta topic compacted; it reports `TopicNotCompacted`
  and leaves the topic as it is.
- A merge sink that never becomes ready: read its log for credentials, a server below 5.26, or an
  unreachable `uri`; `ConstraintNotCreated` is a warning, not a failure.

## Reference files

Open the one a task needs; each is one topic and stands alone.

### Get started

- `references/get-started/install.md` — Install the flow CLI with Homebrew or from a release archive, pull or build the sidecar, operator and sample images, and set up the Python SDK.
- `references/get-started/deploy-locally.md` — Install the operator and a development Kafka on a kind cluster, deploy the sample cart router as a pipeline with flow generate and kubectl, and watch it become Ready.

### Concepts

- `references/concepts/delivery.md` — How the sidecar delivers records — commit only after the write, at least once, never skipping — and what happens when a batch fails, a partition stalls or a rebalance moves a partition.
- `references/concepts/topics.md` — Managed and unmanaged topics, how a topic's Kafka name is chosen, how its brokers and settings resolve from the blueprint and a Kafka cluster Secret, and how consumer groups and client ids are named.

### Build

- `references/build/ankka-topics.md` — Build a pipeline on the messages an ankka service publishes — give the service a broker in its descriptor, declare its topic unmanaged in the blueprint, and decode ankka's CloudEvents in a streamlet.
- `references/build/graph-sink.md` — Turn a service's events into a Neo4j graph — choose ids and versions, map events to keyed graph deltas in a streamlet, and wire the built-in Neo4j merge sink behind it.
- `references/build/graph-from-ankka.md` — Keep a Neo4j graph in step with an ankka service that publishes its own graph deltas, with a pipeline that is the built-in merge sink and nothing else.
- `references/build/images.md` — Package a streamlet as a container image that holds only its process — no Kafka client, no exposed ports — and make it available to a cluster.

### Run and operate

- `references/deploy/install.md` — Install the AnkkaFlow custom resource definition, the operator and at least one Kafka cluster Secret on a Kubernetes cluster, locally on kind or on any other cluster.
- `references/deploy/deploy-a-pipeline.md` — Verify a blueprint, generate the AnkkaFlow resource with each streamlet's image, apply it, and update, roll out or delete the pipeline afterwards.
- `references/deploy/configuration.md` — Override a blueprint's topics, replica counts and parameters per environment with --conf files, and point topics at Kafka clusters through kafka-cluster Secrets.
- `references/deploy/reset.md` — Reprocess a pipeline's inputs from the earliest offsets by stopping its streamlets, requesting a reset with flow reset, and starting them again.
- `references/deploy/rebuild-a-graph.md` — Fill an empty Neo4j database from a compacted topic of graph deltas by resetting only the merge sink, with no mapper running and no upstream topic read.
- `references/deploy/observe.md` — Read a running pipeline's phase, events, sidecar logs, consumer lag and stall warnings with kubectl and Prometheus.
- `references/deploy/troubleshooting.md` — Symptom, cause and fix for every way an ankka-flow pipeline is refused, fails to start, stops being ready or stalls, from flow verify to the sidecar.

### Reference

- `references/reference/cli.md` — Every command, option, message and exit code of the flow CLI, which starts streamlet projects, verifies blueprints, generates the AnkkaFlow resource and requests resets.
- `references/reference/blueprint.md` — Every key of the blueprint file, the HOCON that names a pipeline's streamlets and the topics connecting their ports, and every rule flow verify checks it against.
- `references/reference/resource.md` — Every field of the AnkkaFlow custom resource and its status, the Kafka cluster Secret, what the operator renders per streamlet, the events it records and the reset annotations.
- `references/reference/operator.md` — The ankka-flow operator's settings, the namespace and permissions it runs with, and what one reconcile of an AnkkaFlow does, in order.
- `references/reference/sidecar.md` — The ankka-flow sidecar's environment variables, the two files it reads, its start-up checks and exit codes, its probes, metrics, stall warnings and logs.
- `references/reference/graph-deltas.md` — The ankka.graph-delta.v1 contract a mapping streamlet writes and the Neo4j merge sink reads — node merges, edge merges and tombstones, each a versioned statement of state under its element key.
- `references/reference/neo4j-merge-sink.md` — The built-in streamlet that merges graph deltas into Neo4j in one transaction per batch — its descriptor, parameters, connection Secret, what it writes, and how it is watched.

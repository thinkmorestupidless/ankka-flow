---
name: ankka-flow-deploy
description: Install ankka-flow on Kubernetes and deploy, configure, rebuild, observe and troubleshoot pipelines — the flow CLI (verify, generate, reset, version), the AnkkaFlow resource and its status, the operator and its settings, Kafka cluster Secrets, deploy-time overrides with --conf and images, managed topic creation, rollouts per streamlet, the sidecar's environment, probes and metrics, consumer lag, PartitionStalled and the operator's events, and resetting consumer groups to the earliest offset. Use when the task names flow verify/generate/reset, an AnkkaFlow resource, the operator, kind, kubectl, a Kafka cluster Secret, lag, a stalled partition, or a pipeline that is not Ready. Also the built-in Neo4j merge sink, with its connection Secret, refusals, metrics and readiness, compacted delta topics, TopicNotCompacted, and rebuilding a graph from its delta topic.
pages:
  - get-started/install.md
  - get-started/deploy-locally.md
  - build/images.md
  - deploy/install.md
  - deploy/deploy-a-pipeline.md
  - deploy/configuration.md
  - build/ankka-topics.md
  - build/graph-sink.md
  - reference/neo4j-merge-sink.md
  - deploy/reset.md
  - deploy/rebuild-a-graph.md
  - reference/graph-deltas.md
  - deploy/observe.md
  - deploy/troubleshooting.md
  - reference/cli.md
  - reference/blueprint.md
  - reference/resource.md
  - reference/operator.md
  - reference/sidecar.md
  - concepts/topics.md
  - concepts/delivery.md
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

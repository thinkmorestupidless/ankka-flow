---
name: ankka-flow-deploy
description: Install the flow CLI with Homebrew or from a release archive, install ankka-flow on Kubernetes (the Flink Kubernetes Operator, the AnkkaFlow CRD, the operator, kafka-cluster Secrets including a machine's, the flink-state state store), and deploy, configure, rebuild, observe and troubleshoot pipelines — flow verify, generate, local, reset, version and mcp, the AnkkaFlow resource and its status, phases, conditions (FlinkOperatorMissing, StateStoreMissing, TopicNotGranted, MachineCredentialRefused, StateUnreadable), events and lag per inlet, --conf overrides for topics, replicas, parallelism, checkpoint interval and Flink resources, images, Deployments and FlinkDeployments, rollouts per streamlet, managed topic creation, resets with state epochs, and ankka topics' machines and grants. Use when the task names flow verify/generate/local/reset/mcp, an AnkkaFlow resource, the operator, a FlinkDeployment, kind, kubectl, a Kafka cluster Secret, flink-state, lag, a condition, a grant, or a pipeline that is not Ready.
---

# Deploying and operating ankka-flow pipelines

A pipeline is deployed as one `AnkkaFlow` resource. `flow generate` writes it from a blueprint, the
streamlets' descriptors, their images and any deploy-time configuration; `kubectl apply` hands it to
the operator, which creates the managed topics and runs each streamlet by its engine: a Pekko Streams
streamlet as a Deployment, a Flink streamlet as a `FlinkDeployment` the Flink Kubernetes Operator
runs, each with a Secret `flow-<pipeline>-<streamlet>` holding its `streamlet.properties`.

## Rules

1. **`flow` is installed, not built.** `brew install thinkmorestupidless/tap/ankka-flow` (the tap
   ankka's CLI ships through; the two coexist), or a release's
   `ankka-flow-cli-<version>-<platform>.tar.gz` for `macos-arm64`, `macos-x64`, `linux-arm64` or
   `linux-x64`, verified with `shasum -a 256 -c <archive>.sha256`; one native binary, no JVM.
   `flow version` prints `flow <version>, descriptor <major>.<minor>`.
2. **The platform is five pieces.** The Flink Kubernetes Operator (Helm chart 1.16 or later,
   `watchNamespaces` listing every namespace with a Flink pipeline; `--set webhook.create=false`
   without cert-manager), the CRD (`kubectl apply --server-side -f
   kustomization/components/crd/ankkaflow.yaml`, then wait for `Established`), the operator
   (`ankka-flow-operator` in namespace `ankka-flow`, image
   `ghcr.io/thinkmorestupidless/ankka-flow-operator:<version>`), `kafka-cluster-<name>` Secrets, and
   the Secret `flink-state`. ankka-flow's operator never installs or upgrades Flink's. On kind,
   `just up` installs all of it with MinIO as the state store and refuses any context but
   `kind-<cluster>`.
3. **The state store is one Secret.** `flink-state` in the operator's namespace: `dir` (required, a
   URI Flink can write; state under `<dir>/<namespace>/<pipeline>/<streamlet>/<epoch>/`),
   `flink-config` (`key=value` lines copied into every `FlinkDeployment`) and `env` (`NAME=value`
   lines given only to Flink's pods). Store credentials go in `env`, never `flink-config`.
4. **Verify before generating.** `flow verify <blueprint> --descriptors <dir> [--conf <file>]...
   [--ankka-topics <project>=<file>]...` checks every contract, binding, topic, parameter and
   deploy-time setting in one pass with no image, network or engine. `flow generate` repeats the checks
   and writes nothing if any fails. Exit codes: `0` ok, `1` refused (every problem on stderr), `2`
   usage.
5. **Deploy-time configuration is merged by the CLI.** `--conf` files (repeatable, later wins) set
   `flow.topics.<id>` (any topic key but `producers`, `consumers`, `managed`, `ankka` and
   `topic.name`), `flow.streamlets.<name>` (`replicas`, `config`, and for Flink only `parallelism`,
   `flink.checkpoint-interval`, `flink.job-manager`/`flink.task-manager` `cpu` and `memory`), and
   `flow.ankka.cluster`. A Flink key on a Pekko Streams streamlet, a misspelt topic or streamlet, or an
   undeclared parameter is refused. Images come from `--image <name>=<ref>` or `--images <file>`
   (`--image` wins). Managed topics are kept on delete unless `--delete-managed-topics`.
6. **`replicas` means different things per engine.** Pekko Streams: the number of pods, `0` stops it.
   Flink: `1` runs the job, `0` suspends it with its state kept; anything else is refused. Scale a
   Flink job with `parallelism`. Never scale a rendered Deployment directly: the operator restores it.
7. **Kafka clusters are Secrets beside the operator.** `kafka-cluster-<name>` in
   `FLOW_KAFKA_CLUSTERS_NAMESPACE` (default `ankka-flow`): `bootstrap.servers` (required),
   `connection-config`, `producer-config`, `consumer-config` (Java properties text), default
   `partitions` and `replicas`, and `machine = <organization>/<name>` for a cluster reached as an ankka
   machine. A topic uses the cluster it names; an ankka topic otherwise `flow.ankka.cluster`, else
   `ankka`; a topic with its own `bootstrap.servers` no cluster; anything else `default`.
8. **ankka topics need a machine and grants.** An organization owner runs
   `ankka organizations machines register <org> <name>`; the cluster Secret carries the machine's
   SASL_SSL/OAUTHBEARER `connection-config` and `machine`; a project owner runs
   `ankka projects grants make machine:<org>/<name> topic <topic> consume|produce -p <project>`. The
   operator checks the grants every 30 seconds: `TopicNotGranted` and `MachineCredentialRefused` make
   the pipeline `Degraded`, the streamlets run regardless, and the condition clears on the first pass
   that succeeds.
9. **A streamlet's container holds only its own topics' settings.** The operator renders each
   streamlet's settings, credentials included for its own ports, into its Secret mounted at
   `/etc/flow/config` (`FLOW_CONFIG_DIR`). Never put a broker address or credential in an image or a
   resource.
10. **Managed topics are created once and never altered.** Other partitions or replication on an
    existing topic is `TopicDiffers`, a changed setting `TopicSettingsIgnored`; both are warnings and
    the pipeline runs on the topic as it is. A missing unmanaged or ankka topic is `TopicMissing`.
11. **Each streamlet rolls on its own.** A changed image, descriptor or resolved configuration
    (including a cluster Secret) changes the pod template's `config-hash` and rolls that streamlet
    only (`StreamletRolled`): a Pekko Streams streamlet one pod at a time, never below the desired
    count; a Flink streamlet's job stopped and started again from its last checkpoint
    (`upgradeMode: last-state`). A new image under the same tag, or a changed `version`, rolls nothing.
12. **Reset only what is stopped.** Set the targets' `replicas` to 0 with `--conf`, apply, wait for
    Pekko pods to go and Flink jobs to be `suspended`, then `flow reset <pipeline> [--streamlet
    <name>]... [-n <ns>]`. The operator re-checks (`ResetRefused` while waiting), deletes each Flink
    target's `FlinkDeployment`, advances its state epoch (`StateDiscarded`, annotation
    `state-epochs`), moves each inlet's group to the earliest offset (`ResetOffsets` or
    `ResetOffsetsFailed`), and writes `reset-offsets-done` so a request runs once. Apply without the
    stop file to start again; a reset Flink streamlet has no state, and the earlier epoch's files stay
    in the store.
13. **Read status, then conditions, then events, then logs.** `kubectl get aflow <name> -o wide` gives
    the phase (`Pending`, `Ready`, `Degraded`, `Failed`) and `DETAIL`; `.status.conditions` the
    conditions; `.status.streamlets[].inlets[]` each inlet's `topic`, `group` and `lag`; events with
    `--field-selector involvedObject.kind=AnkkaFlow` (and `kind=FlinkDeployment` for Flink's own);
    logs by label `flow.ankka.thinkmorestupidless.com/streamlet=<name>` (container `streamlet`, or
    Flink's `flink-main-container`). Flink's web interface is
    `kubectl port-forward svc/flow-<pipeline>-<streamlet>-rest 8081`.
14. **A laptop runs streamlets from `flow local`.** `flow local <blueprint> --descriptors <dir>
    --bootstrap-servers <host:port> -o <dir> [--ankka-bootstrap-servers <host:port>]` writes
    `<dir>/<streamlet>/streamlet.properties` with `local=true`, every outlet at least once; start each
    streamlet's main with `FLOW_CONFIG_DIR` set to its directory. It touches no cluster or broker.
15. **Through `flow mcp`, deploy and observe with tools.** `flow mcp install` (`--client code|desktop`,
    `--scope user|project`) connects a client. `apply_pipeline` verifies, generates and applies,
    `get_pipeline` shows status, conditions and events, `pipeline_logs` a streamlet's pods (job and task
    managers for Flink), `pipeline_lag` the lag rows, and `reset_pipeline` requests a reset with
    `flow reset`'s guards — all on the one cluster `flow.toml`'s `context` and `namespace` name.

## Troubleshooting order

`kubectl get aflow <name> -o wide` → `.status.conditions` → events on the `AnkkaFlow` → the
streamlet's log. Common shapes: `FlinkOperatorMissing` or `StateStoreMissing` (phase `Failed`, nothing
applied, Pekko streamlets included) mean the cluster lacks Flink's operator or `flink-state` with a
`dir`; `Refused` events name each problem in a resource that cannot run; a Pekko pod that exits 1 with
`<streamlet>: <key> is missing` means the image's declaration differs from the deployed descriptor;
lag that grows and never falls on one partition means a record that fails every time, fixed in the
streamlet's code; a `FlinkDeployment` that never becomes `STABLE` shows `ClassNotFoundException` (jar
not at `/opt/flink/usrlib/streamlet.jar`) or a store `Access Denied` (`flink-state`'s `env` or
`flink-config`) in the Flink operator's events; `StateUnreadable` means a new image cannot restore the
state: deploy the previous image again, or reset the streamlet.

## Mistakes to check for

- Running `kubectl` against the shell's current context instead of the cluster `flow.toml` names.
- A pipeline with a Flink streamlet in a namespace the Flink operator's `watchNamespaces` omits.
- Store or Kafka credentials in `flink-config`, a resource, a blueprint or an image.
- A `kafka-cluster-*` or `flink-state` Secret in the pipeline's namespace instead of the operator's.
- `flow reset` against running streamlets, or `kubectl scale` instead of `replicas` in `--conf`.
- Flink `replicas` set above 1 to scale it; `parallelism` scales a Flink job.
- Expecting the operator to change an existing managed topic, or to create an unmanaged or ankka one.
- Reading a Flink streamlet's lag as falling behind; it trails by up to one checkpoint interval.
- Expecting a deploy to take effect from a new image pushed under the same tag; deploy a new tag.
- A literal `:latest` image the cluster cannot pull; on kind, load the image first.
- Expecting the operator to delete an earlier state epoch's files; the store's lifecycle rules do.

## Reference files

Open the one a task needs; each is one topic and stands alone.

### Get started

- `references/get-started/install.md` — Install the flow CLI with Homebrew or from a release archive, pull the operator and sample images, and add the SDK of each engine as one dependency.
- `references/get-started/deploy-locally.md` — Install the Flink Kubernetes Operator, ankka-flow's operator, a development Kafka and MinIO as the state store on a kind cluster, deploy the sample cart pipeline with flow generate and kubectl, and watch it become Ready.
- `references/get-started/coding-agents.md` — Give a coding agent this documentation as skills from the ankka marketplace and flow's abilities as tools through flow mcp — connecting Claude, naming the one cluster the tools may touch, and the loop from a change to a running pipeline.

### Concepts

- `references/concepts/delivery.md` — What each engine promises about delivery — a Pekko Streams streamlet at least once, committing only after the write; a Flink streamlet once between managed topics — and what happens when a streamlet fails.
- `references/concepts/topics.md` — Managed and unmanaged topics, how a topic's Kafka name is chosen, how its brokers and settings resolve from the blueprint and a Kafka cluster Secret, and how consumer groups and client ids are named.

### Build

- `references/build/ankka-topics.md` — Build a pipeline on an ankka project's topics — name each by project and name, check its contracts against the project's declarations, reach the broker as a registered machine, and see what a missing grant looks like.
- `references/build/images.md` — Package a streamlet as a container image for its engine — a Java runtime for a Pekko Streams streamlet, Flink's own image for a Flink streamlet — and make it available to a cluster.

### Run and operate

- `references/deploy/install.md` — Install the Flink Kubernetes Operator, the AnkkaFlow custom resource definition, the operator, the Kafka cluster Secrets and the flink-state Secret, locally on kind or on any other cluster.
- `references/deploy/deploy-a-pipeline.md` — Verify a blueprint, generate the AnkkaFlow resource with each streamlet's image, apply it, and update, roll out or delete the pipeline afterwards.
- `references/deploy/configuration.md` — Override a blueprint's topics, replicas, parallelism, Flink settings and parameters per environment with --conf files, and point topics at Kafka clusters through kafka-cluster Secrets.
- `references/deploy/reset.md` — Reprocess a pipeline's inputs from the earliest offsets by stopping its streamlets, requesting a reset with flow reset, and starting them again; a Flink streamlet starts again with no state.
- `references/deploy/observe.md` — Read a running pipeline's phase, conditions, events, each inlet's consumer lag and every streamlet's logs, whatever its engine, with kubectl and flow mcp.
- `references/deploy/troubleshooting.md` — Symptom, cause and fix for every way an ankka-flow pipeline is refused, fails to start, stops being ready, cannot read its state or falls behind, from flow verify to the operator's conditions.

### Reference

- `references/reference/cli.md` — Every command, option, message and exit code of the flow CLI, which verifies blueprints, writes a laptop's streamlet settings, generates the AnkkaFlow resource and requests resets.
- `references/reference/blueprint.md` — Every key of the blueprint file, the HOCON that names a pipeline's streamlets and the topics connecting their ports, and every rule flow verify checks it against.
- `references/reference/streamlet-settings.md` — The file streamlet.properties a streamlet is given at run time — its names, parameters, and the topic, group, client id, delivery and Kafka properties of each of its ports — and the rules both SDKs read it by.
- `references/reference/resource.md` — Every field of the AnkkaFlow custom resource and its status, the Kafka cluster Secret, what the operator renders per streamlet, the events it records and the reset annotations.
- `references/reference/operator.md` — The ankka-flow operator's settings, the namespace and permissions it runs with, and what one reconcile of an AnkkaFlow does, in order.

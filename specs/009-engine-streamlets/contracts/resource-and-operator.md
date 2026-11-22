# Contract: the resource and the operator

The Kubernetes shapes are in `data-model.md`; this is what an installer and a pipeline author can
rely on.

## Prerequisites the operator checks

| Needed for | Check | When absent |
|---|---|---|
| a Flink streamlet | the CRD `flinkdeployments.flink.apache.org` | condition `FlinkOperatorMissing`, phase `Failed`, event, nothing started |
| a Flink streamlet | Secret `flink-state` with key `dir`, in the namespace of the Kafka cluster Secrets | condition `StateStoreMissing`, same |
| any topic | its Kafka cluster Secret | refused, as today |

Message of each condition says what to do: `install the Flink Kubernetes Operator (see Install
the platform)`; `create the Secret flink-state in namespace ankka-flow with a dir key`.

## Secrets the operator reads (its own namespace)

`kafka-cluster-<name>`: as today, plus an optional `machine` key, `<organization>/<name>`, which
makes the cluster an ankka machine's: groups are prefixed `ankka.machine.<organization>.<name>.`
and grants are checked.

`flink-state`: `dir` (required), `flink-config` (properties text), `env` (properties text).

## What it creates in the pipeline's namespace

| Object | For |
|---|---|
| Secret `flow-<pipeline>-<streamlet>` (`streamlet.properties`) | every streamlet |
| Deployment `flow-<pipeline>-<streamlet>`, one container `streamlet` | a Pekko Streams streamlet |
| FlinkDeployment `flow-<pipeline>-<streamlet>` | a Flink streamlet |
| Secret `flow-<pipeline>-flink-state` (the `env` entries) | a pipeline with a Flink streamlet, when `env` is set |
| ServiceAccount, Role, RoleBinding `flow-<pipeline>` | as today |

Labels, owner references, the config hash and the rolling of only what changed are as today. A
Pekko Streams pod: no service account token, the Secret mounted read-only, readiness `test -f
/tmp/flow/ready`, liveness on `/tmp/flow/alive`'s age.

The operator's own RBAC gains `flinkdeployments.flink.apache.org` (get, list, watch, create,
patch, delete) and `customresourcedefinitions` (get).

## Status

```yaml
status:
  phase: Degraded
  conditions:
    - type: TopicNotGranted
      status: "True"
      reason: Read
      message: "machine affiliates/network holds no grant to read topic checkout.cart-checkouts: …"
      lastTransitionTime: "2026-11-22T10:00:00Z"
  streamlets:
    - {name: cleaner, engine: pekko-streams, desired: 3, ready: 3, detail: "3 of 3 ready",
       inlets: [{name: in, topic: checkout.cart-checkouts, group: ankka.machine.affiliates.network.carts.cleaner.in, lag: 0}]}
    - {name: counter, engine: flink, desired: 1, ready: 1, detail: "running",
       inlets: [{name: in, topic: carts.clean-carts, group: carts.counter.in, lag: 12}]}
```

| Condition | Phase | Cleared |
|---|---|---|
| `FlinkOperatorMissing`, `StateStoreMissing` | `Failed` | next reconcile that finds it |
| `TopicNotGranted` (reason `Read` or `Write`) | `Degraded` | the first observe pass in which the broker allows it |
| `MachineCredentialRefused` | `Degraded` | the first pass that authenticates |
| `StateUnreadable` | `Degraded` | the streamlet runs again (previous image, or after a reset) |
| `Ready` | `Ready` | status `False` in every other phase |

A condition that no longer holds is removed, except `Ready`, which is always present.

Events (added): `FlinkOperatorMissing`, `StateStoreMissing`, `TopicNotGranted`,
`MachineCredentialRefused`, `StateUnreadable`, `StateDiscarded`. Removed: `SidecarImageMissing`.

Observe pass: every `FLOW_OPERATOR_OBSERVE_SECONDS` (30) for every pipeline: lag, grants, Flink
status. So each of the conditions above appears within one minute of the cause.

## Reset

`flow reset <pipeline> [--streamlet <name>]…`, unchanged in form.

| Streamlet | Refused while | Then |
|---|---|---|
| Pekko Streams | it has replicas or pods | each inlet's group to earliest |
| Flink | its job is not suspended (`replicas = 0`) | the FlinkDeployment deleted, the state epoch advanced, each inlet's group to earliest, event `StateDiscarded` |

The earlier epoch's files remain in the state store; nothing reads them again.

## Settings (environment of the operator)

Added: `FLOW_FLINK_SERVICE_ACCOUNT` (`flink`), `FLOW_OPERATOR_OBSERVE_SECONDS` (`30`). Removed:
`FLOW_SIDECAR_IMAGE`.

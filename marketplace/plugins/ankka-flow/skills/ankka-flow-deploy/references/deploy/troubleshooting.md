# Troubleshooting

> Symptom, cause and fix for every way an ankka-flow pipeline is refused, fails to start, stops being ready or stalls, from flow verify to the sidecar.

Source: https://flow.ankka.cloud/deploy/troubleshooting/
Look in this order: `kubectl get aflow <name> -o wide` for the phase and its detail, the events on the
`AnkkaFlow`, the sidecar container's log, then its metrics. Each is described in
[Observe a pipeline](observe.md). Then find the symptom here.

```bash
kubectl -n shop get aflow cart -o wide
kubectl -n shop get events --field-selector involvedObject.kind=AnkkaFlow
kubectl -n shop logs deploy/flow-cart-router -c sidecar
```

## `flow verify` or `flow generate` refuses

The CLI lists every problem at once, one per line, and exits `1`. Nothing has been deployed. The common
ones:

| message | cause | fix |
|---|---|---|
| `'<outlet>' (…) is not compatible with '<inlet>' (…).` | the two ports on one topic carry different contracts | give both the same schema name; a new version of a contract is a new name, and every reader must move to it |
| `'<path>' does not point to a known streamlet inlet or outlet, please try …` | a typo in a port path, or a stale descriptor | use a suggested path; regenerate the descriptor if the port was renamed |
| `Streamlet '<name>' names descriptor '<d>', which no descriptor declares.` | the descriptor file is missing from `--descriptors`, or names the streamlet differently | write the descriptor into that directory, or fix the name in `blueprint.streamlets` |
| `Inlet <streamlet>.<port> is not connected.` | an inlet is on no topic | add it to a topic's `consumers` |
| `Topic '<id>' is not managed but has producers …` | the blueprint writes to a topic it does not own | make the topic managed, or write to a managed topic instead |
| `Topic '<id>' is not managed and names no bootstrap.servers or cluster.` | the platform cannot know where an unmanaged topic lives | set `cluster` or `bootstrap.servers` on it, in the blueprint or with `--conf` |
| `streamlet '<name>': parameter '<key>' has no default and no value` | a required parameter is unset | set it with `flow.streamlets.<name>.config` in a `--conf` file |
| `overrides name unknown topic '<id>'` | a `--conf` file names a topic the blueprint does not | correct the id |
| `Streamlet '<name>' has no image.` | `generate` was given no image for a streamlet | add `--image <name>=<ref>` or an entry in `--images` |
| `descriptor <file>: …` | a descriptor fails validation | regenerate it with the SDK rather than editing it by hand |

Every message is listed in [the CLI reference](../reference/cli.md).

## The pipeline is `Failed` with `SidecarImageMissing`

The operator has no `FLOW_SIDECAR_IMAGE`, so it refuses every pipeline and applies nothing. Its log says
so at start-up: `sidecar image (not configured: every pipeline will be refused)`. Set the variable on the
`ankka-flow-operator` Deployment; the pipelines are reconciled again when the operator restarts. See
[the operator reference](../reference/operator.md#settings).

## The pipeline is `Failed` with `Refused` events

The resource cannot run, so nothing was applied. Each `Refused` event names one problem, and the status
`DETAIL` lists them all:

| detail | fix |
|---|---|
| `topic '<id>' uses Kafka cluster '<name>', but there is no Secret 'kafka-cluster-<name>'` | create the Secret in the operator's clusters namespace, `ankka-flow` by default |
| `Kafka cluster '<name>' has no bootstrap.servers` | add `bootstrap.servers` to that Secret |
| `managed topic '<id>' has no partitions: …` (or `replicas`) | set it on the topic, or as a default in the cluster's Secret |
| `streamlet '<name>': inlet '<p>' is not bound to a topic` | a resource edited by hand; regenerate it with `flow generate` |
| `streamlet '<name>': its descriptor does not parse: …` | regenerate the resource from a valid descriptor |
| `streamlet '<name>' names Secret '<s>', which does not exist in namespace '<ns>'` | create the built-in streamlet's connection Secret in the pipeline's namespace |
| `streamlet '<name>': Secret '<s>' has no key '<k>'` | add `uri`, `username` and `password` to the Secret |
| `streamlet '<name>' is built in and names no Secret in its 'secret' parameter` | set `flow.streamlets.<name>.config.secret` with `--conf` and regenerate |
| `streamlet '<name>' is built in and takes no image` | a built-in streamlet runs in the sidecar; regenerate without an image for it |
| `streamlet '<name>' names built-in '<x>', which this operator does not know` | the CLI is newer than the operator; upgrade the operator |

Fixing a Secret needs no new apply: the operator reads the Secrets again at its next reconcile, within
`FLOW_OPERATOR_RESYNC_SECONDS` (five minutes by default), or at once when the `AnkkaFlow` changes.

## The pipeline is `Degraded` with `TopicMissing`

An unmanaged topic, one the pipeline reads but does not own, does not exist. The platform never creates
it, and the consumers of that topic stay not ready; the sidecar logs
`inlet '<name>': topic '<topic>' does not exist; not ready until it does`. Create the topic with whatever
owns it, or correct `topic.name`. The streamlets become ready on their own once it exists.

A `Kafka unreachable: …` detail means the operator could not reach the topic's brokers: check the
cluster's `bootstrap.servers` and `connection-config`.

## `TopicDiffers` or `TopicSettingsIgnored`

A managed topic already exists with other partitions or replication than the resource declares, or with a
different value for a `topicConfig` entry. The operator never alters an existing topic, so it left it as
it is and warned. The pipeline still runs, on the topic as it exists. To change the topic, change it with
Kafka's own tools, or delete it so the operator creates it afresh.

## A pod never becomes ready

The pod's readiness is the sidecar's: a conversation with the process is running and every inlet is
subscribed to a topic that exists. Read the sidecar's log:

| log | cause | fix |
|---|---|---|
| `discovery attempt <n> at 127.0.0.1:9010: …; retrying in …`, repeating | the process is not listening on `127.0.0.1:$FLOW_PROCESS_PORT` | check the process container's log; it may have crashed, or bind another port or address |
| `refusing to start: the process does not match the deployed streamlet`, then the sidecar exits `1` and the pod restarts | the image's declaration differs from the descriptor in the resource: a port, contract or parameter was changed without regenerating, or the wrong image tag is deployed | regenerate the descriptor from the code in the image, run `flow generate` with it, and apply; or deploy the image the descriptor was written from |
| `the SDK speaks protocol '<v>' and this sidecar speaks '<v>'; the major versions must match` | the SDK and the platform speak incompatible protocol versions | use an SDK of the platform's major version |
| `the SDK speaks protocol '<v>', later than this sidecar's '<v>'; …` | the SDK is newer than the platform | upgrade the platform, or pin the SDK |
| `refusing to start:` before any discovery, then exit `2` | `streamlet.conf` or `descriptor.json` does not parse, or does not match the descriptor's ports and parameters | on a laptop, correct the file; in a cluster, regenerate and apply the resource |
| `inlet '<name>': topic '<topic>' does not exist; not ready until it does` | an input topic is missing | create it; see the `TopicMissing` entry |
| `stream failed: …` then `reconnecting in …`, repeating | the process fails a batch or breaks the protocol every time | see the stalled partition entry |

When the sidecar refuses a process at discovery, it sends every problem to the process first, so they also
appear in the process container's log.

## The Neo4j merge sink never becomes ready

A built-in stage is ready once it has opened and every inlet is subscribed. Read the sidecar's log:

| log | cause | fix |
|---|---|---|
| `cannot open Neo4j at <uri>: …authentication…; retrying in …` | the Secret's username or password is wrong | correct the Secret; the sink re-reads the mounted files on every attempt, so no restart is needed |
| `cannot open Neo4j at <uri>: …; retrying in …` with a connection error | the `uri` is wrong or the database is unreachable from the pod | check the `uri` and the database's Service |
| `… is older than Neo4j 5.26, which the merge needs (dynamic labels)` | the server is too old | upgrade Neo4j to 5.26 or later |
| `refusing to start: credentials directory /etc/flow/neo4j has no 'uri'`, exit `2` | the Secret lacks a key, or on a laptop the directory is not mounted | add the key, or mount the directory |
| `refusing to start: cannot read 'password' in credentials directory …`, exit `2` | the files are not readable by the sidecar's user | in a cluster the operator mounts them with mode `0440`; on a laptop, make them readable |
| `refusing to start: the deployed descriptor is not this sidecar's built-in`, exit `1` | the resource was generated by a CLI whose built-in descriptor differs from this sidecar image's | regenerate the resource with a CLI of the platform's version |
| `stage '<x>' is not built into this sidecar`, exit `2` | the `stage` block names a stage this image does not have | use the stage name the CLI wrote, and a sidecar of the same version |

A `ConstraintNotCreated` Warning on the pod is not a failure: the sink runs, more slowly, until the
constraint exists; see [Observe a pipeline](observe.md#a-built-in-stage).

## Lag grows and a `PartitionStalled` event appears

A batch fails every time: the process raises on it, or breaks the protocol. The sidecar never skips a
record or sends it to a dead-letter topic; it redelivers the batch from the last commit, indefinitely, so
that partition stops advancing while the others carry on. The stall shows as growing lag,
`ankka_flow_sidecar_stalled_seconds` rising, and one `PartitionStalled` Warning on the pod after
`FLOW_STALL_WARNING_AFTER`, naming the inlet, the partition and the last error.

For the Neo4j merge sink, the note names the reason: `neo4j merge failed for inlet 'in' partition <p>:`
followed by a record that breaks the delta contract (`offset <n>: …`, fixed in the streamlet that wrote
it), the database's refusal, or `the transaction did not complete within …` when the database stopped
answering. A database outage drains on its own once the database is back.

The fix is in the streamlet: skip the record by acknowledging the batch without emitting for it, or
correct the code that fails. Deploying the fixed image resumes from the last committed offset. See
[Delivery and failure](../concepts/delivery.md).

## The same record arrives twice

Delivery is at least once. A record whose emits were written but whose offsets were not yet committed
when a pod stopped, a rebalance moved its partition, or the stream failed, is delivered again. This is
expected; streamlet logic must tolerate repeats.

## `flow reset` refuses

| message | fix |
|---|---|
| `cannot reset offsets while streamlets are running: [<name>] is not scaled to 0 …` | set `flow.streamlets.<name>.replicas = 0` with `--conf`, regenerate and apply |
| `… [<name>] still has <n> pod(s) …` | wait for the pods to terminate, then run it again |
| `no pipeline '<p>' in namespace '<ns>'` | pass `-n` with the pipeline's namespace |
| `streamlet [<name>] has no inlets, so no consumer groups to reset` | leave that streamlet out; only streamlets that read have groups |

A `ResetRefused` event with `reset <id> waits: …` means the request was recorded but a target still runs;
the operator carries it out once the pods are gone. A `ResetOffsetsFailed` event carries Kafka's reason;
`consumer group [<g>] has <n> active member(s)` means something still consumes with that group. See
[Rebuild from the start](reset.md).

## A streamlet does not roll after a change

A streamlet rolls when its image reference, descriptor or resolved configuration changes, and only then.
The resolved configuration includes the Kafka connection taken from a cluster Secret, so changing the
Secret rolls the streamlets that use it at the operator's next reconcile. Changing the pipeline's
`version` alone rolls nothing, and neither does a new image pushed under the same tag; deploy a new tag.

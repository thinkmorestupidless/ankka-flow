# Neo4j merge sink

> The built-in streamlet that merges graph deltas into Neo4j in one transaction per batch — its descriptor, parameters, connection Secret, what it writes, and how it is watched.

Source: https://flow.ankka.cloud/reference/neo4j-merge-sink/
`builtin/neo4j-merge-sink` is a streamlet whose descriptor ships with the platform and whose logic runs
inside the sidecar. Its pod has one container, the sidecar, and no process. It reads
[graph deltas](graph-deltas.md) from its inlet and merges each batch into Neo4j in one write
transaction; the inlet's offsets are committed only after that transaction commits.

Every delta is a versioned statement of state, so a batch applied twice — after a restart, a
rebalance or a reset to the start of the topic — leaves the graph as applying it once did. Delivery is
at least once; the graph does not care.

## Descriptor

| | |
|---|---|
| name | `neo4j-merge-sink`, named in a blueprint as `builtin/neo4j-merge-sink` |
| inlet `in` | `json`, `ankka.graph-delta.v1` |
| outlets | none |
| `secret` | STRING, required: the name of the connection Secret in the pipeline's namespace |
| `transaction-timeout` | DURATION, default `30s`: how long one batch's transaction may take |

The canonical descriptor is
[`protocol/fixtures/builtin/neo4j-merge-sink.json`](https://github.com/thinkmorestupidless/ankka-flow/blob/main/protocol/fixtures/builtin/neo4j-merge-sink.json).
No descriptor file is needed to verify a blueprint that uses it: `flow verify` knows the built-ins.

## In a blueprint

```hocon
blueprint {
  name = checkouts-graph
  streamlets {
    mapper = checkout-graph
    graph  = builtin/neo4j-merge-sink
  }
  topics {
    graph-deltas { producers = [mapper.deltas], consumers = [graph.in], partitions = 3, replicas = 1 }
  }
}
```

The sink's inlet is verified against the topic's producers like any port, so an outlet of another
contract is refused before anything runs. Its parameters are set at deploy time:

```hocon
flow.streamlets.graph { config { secret = neo4j-shop } }
flow.topics.graph-deltas { consumer-config.flow.batch { max-records = 500 } }
```

`flow generate` needs no `--image` for `graph`, and refuses one:
`Streamlet 'graph' is built in and takes no image.` The resource records the streamlet with
`builtin: true` and an empty `image`.

## The connection Secret

A Secret in the pipeline's namespace, named by the `secret` parameter:

| Key | Required | Meaning |
|---|---|---|
| `uri` | yes | `bolt://` or `neo4j://` address, such as `bolt://neo4j.neo4j.svc:7687` |
| `username` | yes | |
| `password` | yes | |
| `database` | no | default `neo4j` |

```yaml
apiVersion: v1
kind: Secret
metadata: { name: neo4j-shop, namespace: shop }
stringData:
  uri: bolt://neo4j.neo4j.svc:7687
  username: neo4j
  password: change-me
```

The operator mounts it read-only into the sidecar at `/etc/flow/neo4j`, one file per key, with mode
`0440`. The sidecar's user is in group 0, which the files belong to, and nobody else can read them.
The password never appears in the resource or in the rendered configuration. The Secret's
`resourceVersion` is part of the streamlet's configuration hash, so changing the Secret rolls the pod;
the sink also re-reads the files on every connection attempt, so a corrected password is picked up
without a restart.

The operator refuses the resource — a `Refused` event, phase `Failed`, nothing else applied — when:

| Problem | Message |
|---|---|
| no `secret` parameter | `streamlet 'graph' is built in and names no Secret in its 'secret' parameter` |
| the Secret does not exist | `streamlet 'graph' names Secret 'neo4j-shop', which does not exist in namespace 'shop'` |
| a required key is missing | `streamlet 'graph': Secret 'neo4j-shop' has no key 'password'` |
| the Secret cannot be read | `streamlet 'graph': Secret 'neo4j-shop': could not be read: <reason>` |
| an image is given | `streamlet 'graph' is built in and takes no image` |
| the operator does not know the built-in | `streamlet 'graph' names built-in 'x', which this operator does not know` |

## Opening

When the sidecar starts, and again after every failure, the sink opens:

1. It reads the credentials directory. A required file that is missing refuses start-up with exit
   code 2 (`credentials directory /etc/flow/neo4j has no 'uri'`); a file that is there but cannot be
   read says so (`cannot read 'password' in credentials directory …`).
2. It compares the deployed `descriptor.json` with its own built-in descriptor, and refuses start-up
   with exit code 1, naming every difference, if they differ.
3. It connects and asks the server's version. A server older than Neo4j 5.26 does not open — the
   merge uses dynamic labels in `MERGE`, which 5.26 introduced — and the sink retries.
4. It creates the uniqueness constraint `element_id` if it does not exist. Without the privilege to
   create it, the sink records a `ConstraintNotCreated` warning on its pod and opens anyway; merges
   then scan until the constraint exists.

Steps 3 and 4 are retried with backoff (500 ms doubling to 10 s) until they succeed. The pod is ready
once the sink is open and every inlet is subscribed.

## What it writes

| | Node | Edge |
|---|---|---|
| identity | `id`, unique among nodes; every node carries the label `Element` | `id` within the edges of one type between one `from` and one `to` |
| labels or type | `Element` plus the delta's labels, replaced by every applied merge | the delta's type |
| properties | the delta's, replaced whole by every applied merge | the same |
| `_version` | the version of the last applied delta; `-1` on a placeholder | the same |
| `_deleted` | `true` after a tombstone; cleared by a later applied merge | the same |

A **placeholder** is a node an edge names before the node's own delta has arrived: `Element`, its
`id`, `_version = -1` and nothing else. The node's first delta replaces it.

For one element, by the incoming version `v` against the stored `_version` `s`:

| Stored | Incoming | Result |
|---|---|---|
| absent | merge | created at `v` |
| absent | tombstone | created, marked deleted, at `v` |
| `s` | merge with `v > s` | state replaced, `_version = v`, `_deleted` cleared |
| `s` | tombstone with `v > s` | `_deleted = true`, `_version = v`, properties kept |
| `s` | anything with `v ≤ s` | unchanged; counted as stale |

## Processing a batch

1. Every record is parsed as a delta. A record that breaks the contract fails the batch; see
   [Graph deltas](graph-deltas.md#validation).
2. The batch is folded to one delta per element: the highest version, the first on a tie. The rest
   count as stale.
3. One write transaction runs four statements — node merges, edge merges, node tombstones, edge
   tombstones — each an `UNWIND` over its list, with the `transaction-timeout`. The driver retries a
   transient failure, such as a deadlock between two partitions' transactions, by running the whole
   transaction again, which is safe because every statement is idempotent.
4. When the transaction commits, the batch completes and the sidecar commits its offsets.

The node statement, as the sink runs it:

```cypher
UNWIND $nodes AS d
MERGE (n:Element {id: d.id})
  ON CREATE SET n._version = -1
WITH n, d WHERE n._version < d.version
REMOVE n:$([l IN labels(n) WHERE l <> 'Element'])
SET n = d.properties, n.id = d.id, n._version = d.version
SET n:$(d.labels)
RETURN count(n) AS written
```

The edge statement merges both endpoints and then the relationship:

```cypher
UNWIND $edges AS d
MERGE (a:Element {id: d.from}) ON CREATE SET a._version = -1
MERGE (b:Element {id: d.to})   ON CREATE SET b._version = -1
MERGE (a)-[r:$(d.type) {id: d.id}]->(b)
  ON CREATE SET r._version = -1
WITH r, d WHERE r._version < d.version
SET r = d.properties, r.id = d.id, r._version = d.version
RETURN count(r) AS written
```

The tombstone statements merge the element the same way and set `_version` and `_deleted = true`
under the same guard.

## When a batch fails

A failed transaction, an unreadable record, or a database that does not answer fails the batch:

- Nothing from the batch is committed. The sidecar tears the stream down, reports the pod not ready,
  backs off and opens the sink again, then reads the batch again from the last commit.
- The batch has a deadline of `transaction-timeout` plus 5 seconds. The transaction timeout is
  enforced by the server, so the deadline is what fails a batch when the server stops answering
  altogether; the message is `the transaction did not complete within …`.
- The driver of a failed batch is discarded, never reused.
- The failure message names the inlet and partition:
  `neo4j merge failed for inlet 'in' partition 2: offset 4711: unknown kind 'nod'`.
- A partition that stays uncommitted past `FLOW_STALL_WARNING_AFTER` raises one `PartitionStalled`
  warning on the pod, carrying the last error.

No log line, event or message contains the password.

## Metrics

Beside every inlet metric the sidecar exports, per inlet partition:

| Metric | Meaning |
|---|---|
| `ankka_flow_stage_deltas_written_total` | deltas applied |
| `ankka_flow_stage_deltas_stale_total` | deltas found stale, folded away in a batch or filtered by version |
| `ankka_flow_stage_batches_failed_total` | batches that failed |

Each carries the labels `inlet` and `partition`.

## Exit codes and readiness

| | |
|---|---|
| exit 2 | `streamlet.conf` or `descriptor.json` unreadable, a stage the image does not have, or a required credentials file missing |
| exit 1 | the deployed descriptor is not this sidecar's built-in |
| exit 0 | stopped |
| not ready | until the sink is open and every inlet is subscribed; again whenever a batch fails, until it is reopened |

## On a laptop

The sidecar runs the sink from a `streamlet.conf` with a `stage` block and a credentials directory
mounted at the path the block names. There is no `FLOW_PROCESS_ADDRESS`:

```hocon
flow {
  pipeline  = checkouts-graph
  streamlet = graph
  config    = { secret = "neo4j-local", transaction-timeout = "30s" }
  stage {
    name = "neo4j-merge-sink"
    neo4j { credentials-dir = "/etc/flow/neo4j" }
  }
  inlets {
    in { topic = "checkouts-graph.graph-deltas", bootstrap.servers = "kafka:9092" }
  }
}
```

The credentials directory holds the same four files a Secret would. The checkout graph sample's
compose file runs one; [Build a graph from a pipeline](../build/graph-sink.md) walks through it.

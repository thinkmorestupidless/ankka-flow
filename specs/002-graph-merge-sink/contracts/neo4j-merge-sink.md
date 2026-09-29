# Contract: the `neo4j-merge-sink` stage

What the sink is to a blueprint, what it needs from the operator, what it writes, and how it can be
watched.

## Descriptor

`builtin/neo4j-merge-sink`, defined as `Builtins.neo4jMergeSink` in `protocol` and committed as
`protocol/fixtures/builtin/neo4j-merge-sink.json`:

| | |
|---|---|
| name | `neo4j-merge-sink` |
| inlet `in` | `json`, `ankka.graph-delta.v1` |
| outlets | none |
| `secret` | STRING, required: the name of the connection Secret in the pipeline's namespace |
| `transaction-timeout` | DURATION, default `30s`: the per-batch transaction timeout |

The sidecar image at version *X* carries the descriptor of version *X*; at open it refuses, with
exit 1 and every difference named, a deployed `descriptor.json` that is not that descriptor.

## Blueprint and deploy-time configuration

```hocon
blueprint {
  name = checkouts
  streamlets {
    mapper = checkout-graph
    graph  = builtin/neo4j-merge-sink
  }
  topics {
    graph-deltas { producers = [mapper.deltas], consumers = [graph.in], partitions = 3, replicas = 1 }
  }
}
```

```hocon
# --conf
flow.streamlets.graph { config { secret = neo4j-shop }, replicas = 1 }
flow.topics.graph-deltas { consumer-config.flow.batch { max-records = 500 } }
```

`flow verify` checks `graph.in` against `mapper.deltas` as any pair of ports. `flow generate`
needs no `--image` for `graph` and refuses one.

## The connection Secret

In the pipeline's namespace; keys `uri`, `username`, `password`, optional `database` (default
`neo4j`). Mounted read-only at `/etc/flow/neo4j` in the sidecar container, mode `0440` (the sidecar
runs as uid 1001 in group 0 and the kubelet writes the files as root:root); `streamlet.conf` carries
`flow.stage.neo4j.credentials-dir = "/etc/flow/neo4j"`. The operator refuses the resource
(`Refused`, phase `Failed`, no other action) when the Secret does not exist or lacks a required key:

- `streamlet 'graph' names Secret 'neo4j-shop', which does not exist in namespace 'shop'`
- `streamlet 'graph': Secret 'neo4j-shop' has no key 'password'`
- `streamlet 'graph' is built in and names no Secret in its 'secret' parameter`
- `streamlet 'graph': Secret 'neo4j-shop': could not be read: <reason>`

A change to the Secret rolls the streamlet (its `resourceVersion` is in the config hash).

## Opening

In order, retried with the reconnect backoff (500 ms doubling to `FLOW_RECONNECT_MAX_BACKOFF`)
while any step but the descriptor check fails:

1. Read the four files. A missing required file: `credentials directory /etc/flow/neo4j has no
   'uri'`; one that is there but unreadable: `cannot read 'uri' in credentials directory …` — a
   refusal at startup, exit 2, because no retry can fix a file the operator did not mount. The
   files are read again on every later connection attempt, so a corrected or rotated password is
   used without a restart (and a file missing then is retried, not refused).
2. The deployed descriptor equals the built-in: otherwise log every difference and exit 1.
3. `verifyConnectivity`; the server agent must be `Neo4j/5.26` or later, else the stage does not
   open: `Neo4j 5.24.1 at bolt://… is older than 5.26, which the merge needs (dynamic labels)`.
4. `CREATE CONSTRAINT element_id IF NOT EXISTS FOR (n:Element) REQUIRE n.id IS UNIQUE`. A
   permission failure records `ConstraintNotCreated` on the pod and continues; any other failure
   fails the open.
5. Open. The pod is ready once every inlet is subscribed as well.

A log line never contains the password; a driver exception message is filtered for the `uri`'s
userinfo before logging.

## Processing a batch

1. Parse every record as a delta ([graph-delta.md](./graph-delta.md)). Any problem fails the batch:
   `StreamFailed("neo4j merge failed for inlet 'in' partition 2: offset 4711: unknown kind 'nod'")`.
2. Fold to one delta per element id per kind space (highest version; first on a tie); count the
   rest stale.
3. One managed write transaction (`session.executeWrite`, timeout `transaction-timeout`, database
   from the Secret, a client-side deadline of `transaction-timeout` + 5 s because a server that has
   stopped answering enforces no timeout of its own) running the four statements below with the folded lists as parameters; the
   driver retries transient failures (deadlocks between partitions' transactions) by rerunning the
   whole function, which is safe because every statement is idempotent.
4. On commit: add the statements' counts to `deltas_written`, the folded and filtered counts to
   `deltas_stale`, and complete the batch with `Acked(Vector.empty)`; the sidecar then commits the
   inlet's offsets.
5. On any failure: `batches_failed + 1`, the driver is discarded (a hung connection may still hold
   the transaction), and the Future fails with `StreamFailed` whose message
   names the inlet, the partition and the reason. The stream tears down, the pod is not ready, the
   supervisor backs off and reopens, and the batch is redelivered from the last commit.

### The statements

Nodes (`$nodes`: `[{id, version, labels, properties}]`):

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

Edges (`$edges`: `[{id, version, type, from, to, properties}]`):

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

Node tombstones (`$nodeTombstones`: `[{id, version}]`):

```cypher
UNWIND $nodeTombstones AS d
MERGE (n:Element {id: d.id})
  ON CREATE SET n._version = -1
WITH n, d WHERE n._version < d.version
SET n._version = d.version, n._deleted = true
RETURN count(n) AS written
```

Edge tombstones (`$edgeTombstones`: `[{id, version, type, from, to}]`):

```cypher
UNWIND $edgeTombstones AS d
MERGE (a:Element {id: d.from}) ON CREATE SET a._version = -1
MERGE (b:Element {id: d.to})   ON CREATE SET b._version = -1
MERGE (a)-[r:$(d.type) {id: d.id}]->(b)
  ON CREATE SET r._version = -1
WITH r, d WHERE r._version < d.version
SET r._version = d.version, r._deleted = true
RETURN count(r) AS written
```

`SET n = d.properties` replaces every property, so a property the delta no longer carries is gone
and `_deleted` is cleared by an applied merge; `id` and `_version` are set again after it. An empty
list for a kind skips its statement. Labels are replaced except `Element`.

## Metrics and events

| metric (Prometheus) | labels |
|---|---|
| `ankka_flow_stage_deltas_written_total` | `inlet`, `partition` |
| `ankka_flow_stage_deltas_stale_total` | `inlet`, `partition` |
| `ankka_flow_stage_batches_failed_total` | `inlet`, `partition` |

Plus every inlet metric the sidecar already has (`records_lag` under `client_id =
<pipeline>.<streamlet>.in`, `ankka_flow_sidecar_in_flight`, `ankka_flow_sidecar_stalled_seconds`).

| event | on | when |
|---|---|---|
| `PartitionStalled` (Warning) | the pod | a partition uncommitted past `FLOW_STALL_WARNING_AFTER`; the note carries the last Neo4j error |
| `ConstraintNotCreated` (Warning) | the pod | the sink lacks the privilege to create `element_id` |
| `Refused` (Warning) | the resource | the Secret is missing or incomplete, or the streamlet was given an image |

## Exit codes and readiness

| | |
|---|---|
| exit 2 | `streamlet.conf` or `descriptor.json` unreadable; `stage.name` unknown to this image; a required credentials file missing |
| exit 1 | the deployed descriptor is not this image's built-in |
| exit 0 | stopped |
| not ready | until every inlet is subscribed and the stage is open; again whenever the stream fails, until it is reopened |

## On a laptop

`docker compose` with Kafka, `neo4j:5.26-community`, the mapper's sidecar and the sink's sidecar,
the latter with `./flow-graph:/etc/flow/config:ro` (its `descriptor.json` and `streamlet.conf`) and
`./neo4j-secret:/etc/flow/neo4j:ro` (the four files). No `FLOW_PROCESS_ADDRESS`. The sample's
README has the loop.

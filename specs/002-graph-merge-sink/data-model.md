# Data model: a graph merge sink built into the sidecar

**Feature**: [spec.md](./spec.md) | **Plan**: [plan.md](./plan.md) | **Contracts**: [contracts/](./contracts/)

Four things are new: the **delta** (what a mapper writes and the sink reads), the **element** (what
the sink writes into the graph), the **built-in descriptor** (how the sink is known to the blueprint,
the resource and the sidecar), and the **connection** (how the sink reaches its database). Each is
defined once and derived from by one program.

## The delta (`ankka.graph-delta.v1`)

One JSON object per Kafka record; the contract's fingerprint is `Base64(SHA-256("ankka.graph-delta.v1"))`
like every JSON contract. The record's key SHOULD be the element's `id` (FR-006).

| field | type | node | edge | tombstone | rule |
|---|---|---|---|---|---|
| `kind` | `"node"` \| `"edge"` \| `"tombstone"` | ✓ | ✓ | ✓ | anything else fails the batch |
| `id` | string, non-empty | ✓ | ✓ | ✓ | global; nodes and edges are separate id spaces |
| `version` | integer ≥ 0 | ✓ | ✓ | ✓ | the source entity's sequence number; a JSON number with a fraction or exponent fails |
| `labels` | array of strings, may be empty | ✓ | | | each `[A-Za-z_][A-Za-z0-9_]*` |
| `type` | string | | ✓ | edge tombstone | `[A-Za-z_][A-Za-z0-9_]*`; one type per edge |
| `from`, `to` | string, non-empty | | ✓ | edge tombstone | node ids; created as placeholders when absent |
| `properties` | object, may be empty or absent | ✓ | ✓ | | values: string, number, boolean, or a non-empty array of one of those; `null`, nested objects and mixed arrays fail; keys `id`, `_version`, `_deleted` are refused |
| `element` | `"node"` \| `"edge"` | | | ✓ | which id space the tombstone marks |

A **node** delta is the node's whole state: after it is applied, the node's labels are exactly
`labels` (plus `Element`) and its properties exactly `properties` (plus the three the sink keeps).
An **edge** delta is the edge's whole state likewise; its direction is `from → to`. A **tombstone**
marks the element deleted at `version`; an edge tombstone carries `type`, `from` and `to` so the
edge can be found from its endpoints without a scan.

Numbers: an integral JSON number becomes a Neo4j integer (64-bit; larger fails the batch), any
other a float.

## The element (what the sink writes)

| | node | edge |
|---|---|---|
| identity | `id`, unique among nodes; label `Element` on every node the sink writes or places | `id` within the edges of one `type` between one `from` and one `to` |
| labels / type | `Element` + the delta's `labels`, replaced on every applied merge | the delta's `type`, fixed for the edge's life |
| properties | the delta's, replaced whole on every applied merge | the same |
| `_version` | the version of the last applied delta; `-1` on a placeholder | the same |
| `_deleted` | `true` after a tombstone; absent otherwise (a later applied merge clears it) | the same |
| constraint | `element_id`: `FOR (n:Element) REQUIRE n.id IS UNIQUE`, created by the sink when it can | none; the endpoints bound the lookup |

**Placeholder**: a node an edge delta names before its own delta arrives: `Element`, `id`,
`_version = -1`, no other property, no other label. Any node delta replaces it.

**State transitions** for one element, by the incoming delta's version `v` against the stored
`_version` `s`:

| stored | incoming | result |
|---|---|---|
| absent | node/edge, any `v` | created at `v` |
| absent | tombstone, any `v` | created marked, at `v` (so a later merge with `v' ≤ v` is stale) |
| `s` | merge with `v > s` | state replaced, `_version = v`, `_deleted` cleared |
| `s` | tombstone with `v > s` | `_deleted = true`, `_version = v`, properties kept |
| `s` | any with `v ≤ s` | unchanged; counted stale |

**Within one batch** the sink folds deltas to one per element id — the highest `v`; on a tie the
first — before writing, and counts the folded-away ones as stale. Nodes are written before edges,
edges before tombstones, so an edge's endpoints exist by the time it is merged.

## The built-in descriptor (`neo4j-merge-sink`)

The `Spec` `Builtins.neo4jMergeSink` in `protocol`; its canonical JSON is
`protocol/fixtures/builtin/neo4j-merge-sink.json`:

| field | value |
|---|---|
| `protocol_version` | the current protocol version |
| `sdk` | `{"name": "ankka-flow-sidecar", "version": "0.0.0"}` |
| `streamlet.name` | `neo4j-merge-sink` |
| `streamlet.description` | `Merges graph deltas into Neo4j in one transaction per batch.` |
| `streamlet.inlets` | `in`: `json`, `ankka.graph-delta.v1` |
| `streamlet.outlets` | none |
| `streamlet.config_parameters` | `secret` (STRING, no default); `transaction-timeout` (DURATION, `30s`) |

**Blueprint**: `streamlets { graph = builtin/neo4j-merge-sink }`. `StreamletDescriptor(proto,
builtin = true)` matches `"builtin/" + proto.name` only.

**Resource**: `spec.streamlets[]` gains `builtin: boolean` (default false); a built-in streamlet
has `image: ""`, its descriptor embedded as any streamlet's, and `config.secret` from `--conf`.

**Sidecar**: `descriptor.json` is the same file as for a process; at open the stage requires it to
equal its own built-in (`DescriptorValidation.compare`), else exit 1.

## The connection (the Secret and the stage block)

**The Secret**, in the pipeline's namespace, named by the `secret` parameter:

| key | required | meaning |
|---|---|---|
| `uri` | yes | `bolt://` or `neo4j://` address, e.g. `bolt://neo4j.neo4j.svc:7687` |
| `username` | yes | |
| `password` | yes | |
| `database` | no | default `neo4j` |

**In the pod**: mounted read-only at `/etc/flow/neo4j` (`defaultMode: 0440`) into the sidecar
container, one file per key. **On a laptop**: a directory of the same four files.

**`streamlet.conf`** for a built-in streamlet, rendered by the operator or written by hand:

```hocon
flow {
  pipeline  = checkouts
  streamlet = graph
  config    = { secret = "neo4j-local", transaction-timeout = "30s" }
  stage {
    name = neo4j-merge-sink
    neo4j { credentials-dir = "/etc/flow/neo4j" }
  }
  inlets { in { topic = "checkouts.graph-deltas", group = "checkouts.graph.in", client-id = "checkouts.graph.in", bootstrap.servers = "kafka.kafka.svc:9092", … } }
}
```

`stage` present ⇒ no process: `FLOW_PROCESS_ADDRESS` is not read, no channel is opened, no
discovery runs. `stage.name` must be a built-in the sidecar image knows, else exit 2 at startup.

## Observability

| metric | labels | meaning |
|---|---|---|
| `ankka_flow_stage_deltas_written_total` | `inlet`, `partition` | deltas applied |
| `ankka_flow_stage_deltas_stale_total` | `inlet`, `partition` | deltas found stale, folded or by version |
| `ankka_flow_stage_batches_failed_total` | `inlet`, `partition` | batches whose transaction failed or whose records were unreadable |

Events on the pod: `PartitionStalled` (as today, its note carrying the Neo4j error);
`ConstraintNotCreated` (once per open, when the privilege is missing). On the resource:
`Refused` for a missing or incomplete Secret, or an image given to a built-in streamlet.

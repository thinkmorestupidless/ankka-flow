# Data model: compacted delta topics

**Feature**: [spec.md](./spec.md) | **Plan**: [plan.md](./plan.md) | **Contracts**: [contracts/](./contracts/)

Nothing stored in the graph changes. What is new is on the wire and in the topic: a delta's key, a
record that is not a delta, and a decision about a topic.

## The element key

| delta | key (UTF-8 text) |
|---|---|
| `{"kind": "node", "id": X, …}` | `node:X` |
| `{"kind": "tombstone", "element": "node", "id": X, …}` | `node:X` |
| `{"kind": "edge", "id": X, …}` | `edge:X` |
| `{"kind": "tombstone", "element": "edge", "id": X, …}` | `edge:X` |

`X` is the delta's `id`, verbatim, after the first colon. Two records have the same key exactly
when they describe the same element. `protocol/fixtures/graph-deltas/keys.json` is a list of
`{"delta": {…}, "key": "…"}` that the sink's suite and the SDK's tests both check, including an id
with colons and one with non-ASCII characters.

## What the sink reads on a delta topic

| record | read as | effect |
|---|---|---|
| value empty or absent, any key | **delete marker** | nothing applied; counted; acknowledged with its batch |
| value a valid delta, key equal to its element key | **delta** | folded and merged as before |
| value a valid delta, key absent | refusal | `offset N: no key; this delta's element key is '<expected>'` |
| value a valid delta, key anything else | refusal | `offset N: key '<found>' is not this delta's element key '<expected>'` |
| value not a valid delta | refusal | the existing messages (`offset N: unknown kind 'x'`, …) |

A refusal fails the batch: nothing of it is applied or committed, the partition stalls, and the
message is in the sink's log and in the `PartitionStalled` note. The fold is over deltas only:
markers neither replace nor are replaced by a delta of the same key within a batch.

## The delta topic decision (`DeltaTopics.decide`)

Input: a verified topic (its ports with their contracts, whether it is managed) and its merged
topic settings (blueprint, then `--conf`).

| topic | `cleanup.policy` set? | decision | resource | note |
|---|---|---|---|---|
| no port of `ankka.graph-delta.v1` | — | none | unchanged | none |
| managed, a delta port | no | `Compacted` | `topicConfig["cleanup.policy"] = "compact"` | *carries graph deltas and is compacted* |
| managed, a delta port | yes, includes `compact` only | `Kept` | as set | none beyond *is compacted* |
| managed, a delta port | yes, `compact,delete` | `Kept` | as set | *records older than its retention are gone from a rebuild* |
| managed, a delta port | yes, without `compact` | `Kept` | as set | *will not hold the whole graph and cannot be relied on to rebuild it* |
| unmanaged, a delta port | — | `NotOurs` | unchanged | *is not managed; whether it is compacted is its owner's* |

## The operator

| observed | resource | action |
|---|---|---|
| topic missing | any | created with the resource's `topicConfig`, as before (`TopicCreated`) |
| topic exists, `cleanup.policy` without `compact` | `cleanup.policy` includes `compact` | `TopicNotCompacted` (Warning); the topic is left as it is; `cleanup.policy` is not also listed under `TopicSettingsIgnored` |
| topic exists, other settings differ | — | `TopicSettingsIgnored`, as before |

## Metrics

| metric | labels | meaning |
|---|---|---|
| `ankka_flow_stage_delete_markers_total` | `inlet`, `partition` | records with no value passed over |
| `ankka_flow_stage_deltas_stale_total` | `inlet`, `partition` | now `records − markers − written` |

The other stage counters are unchanged.

## The SDK (`ankka_flow.graph`)

| name | what |
|---|---|
| `GraphDeltaOutlet(name)` | a `JsonOutlet` of `ankka.graph-delta.v1`; `.node`, `.edge`, `.tombstone_node`, `.tombstone_edge` return an `Emit` whose record has the element key |
| `Delta` | frozen dataclass: `kind`, `element`, `id`, `version`, `labels`, `type`, `from_id`, `to_id`, `properties`, `key` |
| `read(record) -> Delta` | parses an emitted record, for tests; raises `ValueError` on a record that is not a delta or whose key is not its element's |
| `node_key(id)`, `edge_key(id)` | the key bytes |

## A rebuilt graph

| element in the original graph | its records in the topic | in a rebuild |
|---|---|---|
| live | its latest delta (earlier ones compacted away, or not yet) | identical: labels, properties, version |
| tombstoned | its tombstone | present, marked deleted |
| tombstoned, then a delete marker, compacted | none | absent |
| tombstoned, then a delete marker, not yet compacted | merge(s), tombstone, marker | present, marked deleted |
| a placeholder (an endpoint never described) | none of its own | a placeholder again, created by its edge |

In every row the **live** graph is the same before and after compaction. A delete marker written
for an element that was never tombstoned is outside this table: it is rebuilt live until the
broker compacts its key and absent afterwards.

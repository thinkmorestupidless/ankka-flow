# Research: compacted delta topics

**Feature**: [spec.md](./spec.md) | **Plan**: [plan.md](./plan.md)

Each finding says whether it was verified against this repository (read on 2026-10-01, at the
commit that merged feature 002) or is an assumption a named task settles at implementation. The
list at the end collects the latter.

## R1 — The element key is text: `node:<id>` or `edge:<id>`

**Verified in this repository.** A delta's value already says what it is (`kind`, `element` for a
tombstone) and carries its `id` (`sidecar/.../Deltas.scala`), and nodes and edges are separate id
spaces (`Delta.space`). A record's key reaches the stage as `InputRecord.record.key:
Option[ByteString]` (`InletGraph.scala:208-218`). The Python SDK's `emit(record, value=…, key=…)`
takes the key as bytes.

**Decision**: the key is the UTF-8 text `node:<id>` for a node merge or node tombstone and
`edge:<id>` for an edge merge or edge tombstone. The id follows the first colon verbatim, so an id
that itself contains colons (`cart:cart-1`) is unambiguous: `node:cart:cart-1`. The sink computes
the key from the parsed delta and compares bytes; it never parses a key.

**Alternatives considered**: the bare id — a node and an edge with one id would compact each other.
A structured key (JSON, or length-prefixed) — unreadable in a console consumer and no safer, since
the kind is a closed set. Hashing — hides the element from an operator reading the topic.

## R2 — The check and the marker live where deltas are read

**Verified in this repository.** `Neo4jMergeStage.process` parses each record with
`Deltas.parse(offset, value)` and fails the batch on the first `Left`
(`Neo4jMergeStage.scala`, *processing*). A record with no value reaches the stage as empty bytes:
`InletGraph` maps a null value to `ByteString.EMPTY` (`InletGraph.scala:216`), so a Kafka delete
marker and a zero-length value are indistinguishable, and today either fails as `not a JSON object`.

**Decision**: `Deltas.read(offset, key, value): Either[String, Read]`, where `Read` is `Delta(d)` or
`Marker`. An empty value is a `Marker` whatever its key. For a delta, the key must equal
`Deltas.key(d)`; otherwise the problem is `offset N: key '<found>' is not this delta's element key
'<expected>'`, or `offset N: no key; this delta's element key is '<expected>'`. Markers are dropped
before the fold and counted; a batch of markers alone runs no transaction and is acknowledged.
`Neo4jMergeStage.Record.applied` gains the marker count, and the stale count becomes
`records − markers − written`.

**Alternatives considered**: a marker that marks its element deleted in the graph — it would make
every rebuild agree with the graph built as records arrived even for a marker on a live element,
but the feature's decision is that a marker applies nothing and the platform never deletes on a
writer's behalf; the guarantee is stated for markers that follow tombstones instead (spec FR-014).
Distinguishing null from empty in `InletGraph` — a protocol change (`Record.value` is `bytes`) for
no gain: neither is a delta.

## R3 — A delta topic is recognised by its ports' contract, in `blueprint`

**Verified in this repository.** A `VerifiedTopic` has its `connections` (producers and consumers,
each a port with its schema name) and its Kafka settings (`VerifiedBlueprint.scala:51-64`).
`ResourceWriter.topic` turns the merged settings into `TopicSpec.topicConfig` through
`TopicSettings.fromConfig(verified.overrides.topicConfig(t))` (`ResourceWriter.scala:70-84`), and
notes are strings `Verify.run` collects and both `verify` and `generate` print on stderr
(`Verify.scala:55-56`, `Main.scala`). `Builtins.GraphDeltaSchema` is in `protocol`, which
`blueprint` depends on.

**Decision**: `blueprint/.../DeltaTopics.scala`, pure: `decide(topic: VerifiedTopic, topicConfig:
Map[String, String]): Option[Decision]` — `None` when no port of the topic carries
`ankka.graph-delta.v1`; otherwise `Compacted` (managed, no `cleanup.policy` set: the CLI adds
`cleanup.policy = compact`), `Kept(policy)` (the blueprint or `--conf` set one), or `NotOurs`
(unmanaged). Each has a note:

- `note: Topic 'graph-deltas' carries graph deltas and is compacted (cleanup.policy = compact).`
- `note: Topic 'graph-deltas' carries graph deltas and sets cleanup.policy = delete; it will not hold the whole graph and cannot be relied on to rebuild it.`
- `note: Topic 'graph-deltas' carries graph deltas and sets cleanup.policy = compact,delete; records older than its retention are gone from a rebuild.`
- `note: Topic 'graph-deltas' carries graph deltas and is not managed; whether it is compacted is its owner's.`

`Verify.run` adds the notes; `ResourceWriter.topic` applies the default. Any port counts, producer
or consumer, so a pipeline that only writes deltas gets a compacted topic too (the spec's FR-008
was widened from "consumer port" for this).

**Alternatives considered**: deciding in the operator — the resource would no longer say what runs.
A `compacted = true` key in the blueprint — the Kafka setting already exists and is what an
operator recognises.

## R4 — The operator needs one new event, and no schema change

**Verified in this repository.** `KafkaExecutor.describe` reads, for an existing topic, the actual
value of every key in the resource's `topicConfig`, defaults included
(`KafkaExecutor.scala:51-60`), and `Rendering` records `TopicSettingsIgnored` naming the keys whose
values differ (`Rendering.scala:103-112`). `TopicSpec.topicConfig` is a string map; nothing in the
CRD changes.

**Decision**: when the resource's `cleanup.policy` includes `compact` and the existing topic's does
not, `Rendering` records `TopicNotCompacted` (Warning): `topic '<name>' exists and is not compacted
(cleanup.policy = delete); the resource asks for compact. Left as it is: it will not hold the whole
graph. To compact it, alter or recreate the topic.` `cleanup.policy` is then left out of the
generic `TopicSettingsIgnored` list so the same fact is not reported twice.

## R5 — The SDK builds deltas through a port that owns the contract

**Verified in this repository.** Ports are discovered by `isinstance(value, JsonOutlet)`
(`streamlet.py:35-39`), so a subclass is declared and described like any outlet. `emit(record,
value=…, key=…)` keeps the input record's offset, which is how the harness knows a record was not
skipped (`ports.py`, `testkit`). The sample builds three dicts by hand and passes `key=…` itself
(`samples/checkout-graph/.../mapper.py`).

**Decision**: `ankka_flow/graph.py` with `GraphDeltaOutlet(name)` — a `JsonOutlet` whose schema name
is fixed to `ankka.graph-delta.v1` — and four methods that return an `Emit`:
`node(record, *, id, version, labels=(), properties=None)`,
`edge(record, *, id, version, type, from_id, to_id, properties=None)`,
`tombstone_node(record, *, id, version)`,
`tombstone_edge(record, *, id, version, type, from_id, to_id)`. `record` is the input the delta was
derived from (its headers and offset are carried; `None` builds a fresh record). None takes a key.
Each validates what the sink would refuse (identifiers, a non-negative integer version, scalar or
homogeneous-array properties, reserved property names) and raises `ValueError`, so a mapper's
mistakes fail in its own tests. For tests: `graph.read(record) -> Delta` (a frozen dataclass with
`kind`, `element`, `id`, `version`, `labels`, `type`, `from_id`, `to_id`, `properties`, `key`) and
`graph.node_key(id)`, `graph.edge_key(id)`. `GraphDeltaOutlet` is exported from `ankka_flow`.

So that the SDK and the sink can never disagree on a key, `protocol/fixtures/graph-deltas/keys.json`
lists deltas with their expected keys; `DeltasSuite` and the SDK's tests both read it (it travels
to the SDK with the rest of `protocol/fixtures`).

**Alternatives considered**: free functions returning `(key, value)` for `emit` — the author can
still pass another key. A separate package — one module, and the contract is the platform's.

## R6 — Rebuilding needs no new command

**Verified in this repository.** `flow reset <pipeline> --streamlet <name>` resets only the named
streamlets' consumer groups and refuses unless each *target* is scaled to zero with no pods
(`ResetGuards.scala:55-74`); other streamlets may be running. The sink reads its credentials files
on every connection attempt and the Secret's `resourceVersion` is in its config hash, so pointing
the Secret at another database rolls the sink.

**Decision**: the rebuild is a documented procedure over existing commands: scale the sink to zero
(`flow.streamlets.<sink>.replicas = 0`, generate, apply); empty the database or point the Secret at
an empty one; `flow reset <pipeline> --streamlet <sink>`; scale it back. A guide,
`deploy/rebuild-a-graph.md`, and a `CliResetSuite` case proving a reset of one streamlet is
accepted while another runs.

**Alternatives considered**: a `flow rebuild` command — it would wrap three steps, one of which
(emptying a database) the platform must not do.

## R7 — Proving compaction needs a broker tuned to compact within a test

**Assumption, settled by the first compaction task.** Kafka never compacts the active segment and
its cleaner wakes on `log.cleaner.backoff.ms` (15 s by default). A test that waits for compaction
therefore needs a broker with a short cleaner backoff and a topic with `segment.ms`,
`min.cleanable.dirty.ratio`, `min.compaction.lag.ms`, `max.compaction.lag.ms` and
`delete.retention.ms` set small, and must write a little after the bulk so the segment rolls.

**Decision**: `KafkaSuite` gains `protected def kafkaEnv: Map[String, String]` (empty by default,
applied to the container); `CompactionKafkaSuite` overrides it
(`KAFKA_LOG_CLEANER_BACKOFF_MS=500`) and creates its topic with the small settings. It writes ten
versions of each of a thousand elements with a plain producer, builds the graph with the sidecar,
snapshots it, waits until a consumer from the start sees fewer than two thousand records, clears
the database, runs the sidecar under a new group from the start, and compares. The same suite
writes a delete marker after a tombstone and rebuilds again.

## R8 — The sample, the migration and the release

**Decision**: `samples/checkout-graph` declares `GraphDeltaOutlet("deltas")` and builds its three
deltas with it; its tests read them back with `graph.read`; `bench.py` uses `graph.node_key` and
`graph.edge_key`. Its topic `graph-deltas` becomes compacted by default, with the note in `flow
verify`'s output. Bringing a pipeline written for 0.2.0 across is documented on the delta
contract's page and exercised on the kind cluster that still runs one: scale both streamlets to
zero, delete the delta topic (the platform never alters or deletes it; the operator then recreates
it compacted), deploy the new mapper image, reset the mapper and the sink, scale up. This ships as
0.3.0, and its release notes say that a writer of `ankka.graph-delta.v1` built for 0.2.0 is
refused until it keys its deltas `node:<id>` / `edge:<id>`.

## R9 — Documentation

**Decision**: `reference/graph-deltas.md` gains the key rule, the delete marker and the migration;
`reference/neo4j-merge-sink.md` the wrong-key refusal, markers and the fourth counter;
`reference/blueprint.md` and `reference/cli.md` the compaction default and its notes;
`reference/operator.md` and `reference/resource.md` `TopicNotCompacted`; `reference/python-sdk.md`
the `graph` module; `build/graph-sink.md` the helper; a new guide `deploy/rebuild-a-graph.md`;
`deploy/troubleshooting.md` rows for the wrong key and the uncompacted topic; `concepts/topics.md`
a paragraph on delta topics; `reference/limitations.md` and `reference/glossary.md` entries. Skills:
`ankka-flow` (rule 9 gains the key; `rebuild-a-graph`), `ankka-flow-python` (the helper replaces
hand-built deltas), `ankka-flow-deploy` (compaction, `TopicNotCompacted`, the rebuild guide),
`ankka-flow-protocol` unchanged but for the page it carries.

## Verify at implementation

1. **Compaction within a test**: the broker and topic settings of R7 make `apache/kafka:3.9.1`
   compact twenty thousand small records to under two thousand within a minute (the first
   `CompactionKafkaSuite` task; if not, the suite's sizes shrink, never its assertion).
2. **A null value from a plain producer** arrives at the stage as empty bytes and is counted as a
   marker; a zero-length value likewise.
3. **`describeConfigs` reports `cleanup.policy`** for an existing topic created without it (the
   default `delete`), so `TopicNotCompacted` fires for a topic made by 0.2.0.
4. **`cleanup.policy` from a blueprint** (`topic { cleanup.policy = compact }`) reaches
   `TopicSettings.topicConfig` under exactly that key, and `--conf` overrides it.
5. **A reset of one streamlet while another runs** is accepted by `ResetGuards` and carried out by
   the operator for that streamlet's group only.
6. **Non-ASCII ids**: the SDK's key bytes and the sink's computed key agree for an id with
   non-ASCII characters (a row of `keys.json`).

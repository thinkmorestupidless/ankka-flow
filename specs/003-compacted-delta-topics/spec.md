# Feature Specification: Compacted delta topics

**Feature Branch**: `003-compacted-delta-topics`

**Created**: 2026-10-01

**Status**: Draft

**Input**: User description: "Compacted delta topics: make a topic of graph deltas a durable, bounded
record of the graph, so a graph can be rebuilt from its delta topic alone rather than by replaying
every upstream service's history. Scope is ankka-flow only; publishing deltas directly from an ankka
service is a later feature in the ankka repository. (1) Keys identify elements: on every delta topic
the merge sink reads, a delta's record key must name its element — its kind (node or edge, which are
separate id spaces, so a node and an edge with the same id never share a key) and its id — because
compaction keeps the last record per key and a delta keyed by anything else silently destroys other
elements' state. A delta whose key does not identify its element fails its batch and stalls the
partition, naming the offset, as an unreadable delta does (decided: fail, never apply-and-warn, and
on every delta topic, not only compacted ones). The Python SDK gets a helper that builds a delta
record with the right key so a mapper cannot get it wrong. (2) Delete markers: compaction removes an
element's record with a record that has a key and no value. The sink must treat such a record as
"nothing to apply" and acknowledge it, not as an unreadable delta. (3) Compaction by default: a
managed topic whose consumers include a port of the graph delta contract is created compacted unless
the blueprint says otherwise; verification reports what it decided; an existing topic that is not
compacted is reported, never altered. (4) Tombstones expire: a tombstoned element's delta stays in
the topic long enough that any straggling older delta is still refused, and then a delete marker can
remove it from the topic; who writes the delete marker, and after how long, needs deciding. (5)
Rebuild from the topic: resetting only the sink (not the mappers) against a compacted delta topic and
an empty database rebuilds the same graph, and the documentation says how. The checkout-graph sample
moves to the keyed form. Out of scope: publishing deltas from ankka, removing tombstoned elements
from the graph database itself, other built-in stages."

## Context

The graph merge sink made a graph built by a pipeline trustworthy: every delta is a statement of an
element's whole state with a version, so the graph is the same however often and in whatever order
the deltas arrive. What it did not do is make the *topic of deltas* worth keeping. A delta topic
today is an ordinary log: it grows with every version of every element, it is trimmed by time like
any other topic, and once the old records are gone the only way to rebuild the graph is to replay
every upstream service's whole history through every mapper — the expensive path, and one that
depends on every upstream topic still holding its history.

A delta is already exactly what a compacted topic wants: the latest record for an element is the
element. If the topic keeps the last delta for every element and discards the versions before it, the
topic *is* the graph, in a form that never outgrows the graph itself, and a new or emptied database
can be filled from the topic alone. That is the feature: delta topics that are compacted, safely.

"Safely" is most of the work. A compacted topic keeps one record per **key**, so the key must be the
element and nothing else. Today a delta's key is only advice: a mapper that keeps its input's key —
the cart's id on a cart node, a checkout node and the edge between them — writes three elements under
one key, and compaction would keep one of them and silently destroy the other two. Nodes and edges
are separate id spaces, so a node and an edge that happen to share an id must not share a key either.
And compaction has its own way of removing a key, a record with a key and no value, which the sink
would today refuse as an unreadable delta and stall on. So the sink starts enforcing what the
contract only suggested, learns to pass over a delete marker, and the SDK gives mapper authors a way
to build a delta that cannot be keyed wrongly.

## Clarifications

### Session 2026-10-01

- Q: Should this feature also cover publishing deltas from an ankka service? → A: No. This feature
  is ankka-flow only: compaction end to end. Publishing deltas from ankka needs a change to ankka's
  core (several messages per event) and is a later feature in that repository.
- Q: What does the sink do with a delta whose record key does not identify its element? → A: It
  fails the batch and the partition stalls, naming the offset, as for a delta it cannot read. On
  every delta topic, compacted or not: one rule, and a wrongly keyed delta is never applied.
- Q: Who writes the delete marker that removes a tombstoned element's record from the topic, and
  after how long? → A: Nobody by default. A tombstone stays in the topic as its element's last
  record, so the topic is bounded by the elements ever created and a rebuild always has deletions
  marked. A writer that wants a record gone writes the delete marker itself; the platform writes
  none and only has to pass over them.
- Q: Does the contract's name change now that the key is required and includes the element's kind?
  → A: No. It stays `ankka.graph-delta.v1`. A writer built before this feature passes verification
  and is refused by the sink at run time, with a message naming the key it should have used; the
  documentation says how to bring a writer and its topic across.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - A delta that cannot be keyed wrongly (Priority: P1)

A mapper author builds deltas with the SDK. They say what the element is — a node or an edge, its id,
its version and its state — and the SDK produces the record, key included. They never choose a key.
If a writer in another language, or an older mapper, sends a delta whose key is not its element's,
the sink refuses it: the batch fails, the partition stalls, and the sink's log names the record and
says what the key should have been. A node and an edge with the same id are different keys.

**Why this priority**: Everything else in the feature is unsafe without it. A compacted topic with
one wrongly keyed writer loses data silently and permanently; the refusal is what turns that into a
visible, fixable stall before anything is lost.

**Independent Test**: With the sink running against a delta topic, produce deltas built by the SDK
helper for a node and an edge that share an id: both are applied, under different keys. Produce a
well-formed delta keyed by something else: the batch fails naming the offset, the expected key and
the key found, nothing from the batch is in the graph, and the partition's stall is reported as for
any failed batch. A mapper written with the helper has no way to pass a key.

**Acceptance Scenarios**:

1. **Given** a node delta built with the SDK helper, **When** it is emitted, **Then** its record key
   names the element's kind and id, and no argument of the helper lets the author set another.
2. **Given** a node and an edge with the same id, **When** both are written, **Then** their keys
   differ and both are in the graph.
3. **Given** a delta whose key does not name its own element, **When** the sink reads it, **Then**
   the batch fails, nothing of the batch is applied or committed, and the failure names the offset,
   the key found and the key expected.
4. **Given** a delta with no key at all, **When** the sink reads it, **Then** it is refused the same
   way.
5. **Given** a tombstone, **When** it is built with the helper, **Then** its key is the key of the
   element it marks, so it replaces that element's last delta under compaction.

---

### User Story 2 - A delta topic that stays the size of the graph (Priority: P2)

A pipeline author declares a topic that carries graph deltas and does nothing else. `flow verify`
tells them the topic will be compacted and why. The operator creates it compacted. As elements
change, old versions are discarded by the broker and the topic holds the latest delta of each
element. When something removes an element's record from the topic with a delete marker, the sink
passes over the marker without complaint. A blueprint that asks for something else gets it, and is
told what it gives up.

**Why this priority**: This is what the feature is for: a topic that never outgrows the graph and
never ages the graph away. It depends on the keys of story 1.

**Independent Test**: Verify a blueprint whose managed topic feeds the sink: the output says the
topic is compacted because it carries graph deltas. Deploy it; the topic exists with compaction on.
Write ten versions of each of a thousand elements; once the broker has compacted, the topic holds
about one record per element, and a reader from the start sees each element's latest version. Write a
delete marker for one element: the sink acknowledges it, counts it, and the partition does not stall.
Verify a blueprint that sets the topic to be trimmed by time instead: it verifies, with a note that
the topic cannot be relied on to rebuild the graph.

**Acceptance Scenarios**:

1. **Given** a managed topic with a consumer port of the graph delta contract and no cleanup setting
   of its own, **When** the blueprint is verified, **Then** the topic is reported as compacted, with
   the reason, and the generated resource says so.
2. **Given** that resource, **When** the operator creates the topic, **Then** the topic is compacted.
3. **Given** a blueprint that sets the topic's cleanup itself, **When** it is verified, **Then** the
   blueprint's setting is kept and a note says the topic will not hold the whole graph.
4. **Given** a delta topic that already exists and is not compacted, **When** the pipeline is
   deployed, **Then** the topic is left as it is and a warning on the resource says it is not
   compacted and what that means.
5. **Given** a record with a key and no value on a delta topic, **When** the sink reads it, **Then**
   it applies nothing, counts a delete marker, and the batch is acknowledged and committed.
6. **Given** many versions of an element written over time, **When** the broker has compacted the
   topic, **Then** a reader from the start finds the element's latest delta and none before it.

---

### User Story 3 - A graph rebuilt from its topic alone (Priority: P3)

An operator has lost a graph database, or wants a second one. They point the sink at an empty
database, reset the sink — only the sink, not the mappers in front of it — and scale it up. The sink
reads the delta topic from the start and the graph is back, without any upstream service's history
being replayed and without the mappers running at all.

**Why this priority**: It is the pay-off the first two stories make possible, and the procedure
operators will reach for; it needs both of them.

**Independent Test**: Build a graph through a pipeline, with elements updated many times, some
tombstoned. Let the topic compact. Record the graph. Stop the mappers; empty the database; reset the
sink alone and start it. When its lag is zero the graph's live elements — every node and edge not
marked deleted, with their labels, properties and versions — are identical to the recording, and the
sink read roughly one record per element rather than one per version ever written.

**Acceptance Scenarios**:

1. **Given** a compacted delta topic and an empty database, **When** the sink alone is reset and
   started, **Then** every live element of the original graph is in the new one with the same state
   and version.
2. **Given** the same rebuild, **When** it completes, **Then** no mapper ran and no upstream topic
   was read.
3. **Given** a rebuild in progress, **When** new deltas arrive on the topic, **Then** they are
   applied in their place and the rebuilt graph ends at the same state as the original.
4. **Given** the documentation, **When** an operator follows the rebuild procedure, **Then** it
   names the commands in order and says what the rebuilt graph will and will not contain.

---

### User Story 4 - Deleted elements leave the topic eventually (Priority: P4)

An element is tombstoned. Its tombstone delta is now the last record under its key, so the topic
keeps it, and a graph rebuilt from the topic has the element marked deleted. Over years, deleted
elements accumulate in the topic. An operator needs to know whether, when and how they leave it, and
what a rebuilt graph looks like afterwards.

**Why this priority**: A delta topic is bounded by the number of elements ever created, which is
enough for most graphs for a long time; this story is about the long run.

**Independent Test**: Tombstone an element; confirm a rebuild has it marked deleted. Remove its
record from the topic with a delete marker, let the broker compact, and rebuild: the element is
absent, every live element is unchanged, and the sink did not stall on the marker.

**Acceptance Scenarios**:

1. **Given** a tombstoned element whose record is still in the topic, **When** the graph is rebuilt,
   **Then** the element is present and marked deleted.
2. **Given** a tombstoned element whose record a delete marker has removed, **When** the graph is
   rebuilt, **Then** the element is absent and nothing else differs.
3. **Given** a delete marker for an element that was never tombstoned, **When** the graph is rebuilt,
   **Then** the element is absent from the rebuilt graph, and the documentation says a delete marker
   on a live element removes it from every future rebuild.
4. **Given** a tombstoned element and no writer that removes it, **When** any amount of time passes,
   **Then** its tombstone is still the last record under its key: the platform writes no delete
   marker of its own.
5. **Given** a writer that writes a delete marker for an element it tombstoned earlier, **When** the
   sink reads the marker, **Then** the graph is unchanged (the element stays marked deleted there)
   and only future rebuilds differ.

---

### Edge Cases

- A record with a key and an empty value (zero bytes rather than no value): treated as a delete
  marker; the sink cannot tell the two apart and neither is a delta.
- A delete marker whose key is not a well-formed element key: nothing to apply; acknowledged and
  counted like any marker.
- A delta whose key names the right id but the wrong kind (a node delta under an edge's key): refused
  as a wrong key.
- A tombstone whose key is its own element's: accepted; it is that element's latest record.
- A delta topic with several producers that key differently: each record is checked on its own; the
  first wrongly keyed one fails its batch.
- An existing pipeline whose mappers were written before this feature and key by the element's bare
  id: the contract's name is unchanged, so the blueprint still verifies; the sink refuses the first
  such delta at run time, the partition stalls, and the message gives the key expected. The fix is
  in the mapper, followed by a reset of the mapper and the sink so the topic is rewritten with the
  new keys; the documentation gives the steps.
- A delta topic that holds records written before this feature, keyed the old way, read from the
  start by the new sink: refused at the first one. The topic is emptied or replaced as part of
  bringing the pipeline across; the documentation says so.
- A blueprint that sets compaction on a topic that carries something other than deltas: unchanged
  from today; this feature decides a default for delta topics only.
- A delta topic the platform does not own (unmanaged): never created or altered; whether it is
  compacted is its owner's business, and the documentation says what a rebuild needs of it.
- A managed delta topic with both compaction and time-based trimming set by the blueprint: allowed;
  verification notes that records older than the trim are gone from a rebuild.
- The broker has not compacted yet (recent records are never compacted immediately): a rebuild reads
  more records than elements and ends at the same graph; correctness never depends on compaction
  having run.
- A rebuild into a database that is not empty: every delta is applied by the usual version rule;
  elements already at or above their topic version are untouched, and the documentation says a
  rebuild is into an empty database when the aim is an exact copy.
- Resetting the mappers as well as the sink: allowed and correct, only slower; the mappers re-emit
  their history in order and each element's last delta is again its latest.

## Requirements *(mandatory)*

### Functional Requirements

**Keys**

- **FR-001**: The graph delta contract MUST define the record key of a delta: it names the element's
  kind (node or edge) and its id, so that two records share a key exactly when they describe the same
  element.
- **FR-002**: A tombstone's key MUST be the key of the element it marks.
- **FR-003**: The sink MUST refuse a delta whose key is absent or is not the key of the element the
  delta describes: the batch fails, nothing of it is applied or committed, and the failure names the
  record's offset, the key found and the key expected. This applies on every delta topic.
- **FR-004**: The SDK MUST provide a way to build a node delta, an edge delta and a tombstone that
  yields the record with its key, and MUST NOT let the author supply a different key through it.
- **FR-005**: The SDK's test harness MUST let a mapper's tests assert on the deltas it emits without
  parsing keys by hand.

**Delete markers**

- **FR-006**: The sink MUST treat a record with a key and no value, or an empty value, on a delta
  topic as a delete marker: it applies nothing, and the record is acknowledged and committed with its
  batch.
- **FR-007**: The sink MUST count delete markers separately from deltas written, stale and failed.

**Compaction**

- **FR-008**: A managed topic with at least one consumer port of the graph delta contract MUST be
  compacted by default: when the blueprint and the deploy-time configuration set no cleanup policy
  for it, the generated resource sets compaction.
- **FR-009**: `flow verify` and `flow generate` MUST report, for each such topic, that it is
  compacted and why, and for a delta topic whose blueprint sets another cleanup policy, that the
  topic will not hold the whole graph.
- **FR-010**: The operator MUST create a topic with the cleanup policy the resource carries, and for
  an existing delta topic that is not compacted MUST record a warning on the resource and leave the
  topic unchanged.
- **FR-011**: A topic that carries no graph delta port MUST be created exactly as before.

**Rebuilding**

- **FR-012**: Resetting only the sink of a pipeline MUST be possible while its mappers are stopped or
  running, and MUST move only the sink's consumer group.
- **FR-013**: A sink started against an empty database and a delta topic read from the start MUST
  produce a graph whose live elements are identical — labels, type, endpoints, properties and
  version — to those of a graph built from the same topic as the records arrived.
- **FR-014**: A rebuild MUST NOT depend on compaction having run: an uncompacted or partly compacted
  topic rebuilds the same graph.

**Tombstones over time**

- **FR-015**: A tombstoned element whose record is in the topic MUST be rebuilt marked deleted; one
  whose record has been removed by a delete marker MUST be absent from a rebuilt graph, with no other
  element affected.
- **FR-016**: The platform MUST NOT write delete markers: a tombstone remains its element's last
  record until a writer of that topic removes it. The documentation MUST say that a delete marker is
  the writer's to send, what it changes (future rebuilds) and what it does not (a graph that already
  holds the element).
- **FR-016a**: The contract's schema name MUST remain `ankka.graph-delta.v1`; the key rule is stated
  as part of it, and the sink's refusal of a delta keyed the earlier way MUST say what key was
  expected, so that a writer built before this feature can be corrected from the message alone.

**The sample and the documentation**

- **FR-017**: The checkout graph sample MUST build its deltas with the SDK's helper, and its tests
  MUST show the keys of a node and an edge.
- **FR-018**: The documentation MUST state the key rule on the delta contract's page, describe
  compaction and its default on the sink's and the blueprint's pages, give the rebuild procedure as a
  guide, say what a delete marker does, and say how to bring an existing delta topic and its writers
  to the keyed, compacted form; the agent skills MUST carry it.
- **FR-019**: The feature MUST be tested against a real broker with compaction actually run: a
  compacted topic rebuilt into an empty database, a delete marker read by the sink, and a wrongly
  keyed delta refused.

### Key Entities

- **Element key**: the record key of a delta: the element's kind and id. The unit compaction keeps
  one record of.
- **Delta topic**: a topic with at least one consumer port of the graph delta contract. Compacted by
  default when the pipeline owns it.
- **Delete marker**: a record with a key and no value. Removes the key's earlier records from a
  compacted topic; not a delta.
- **Tombstone**: a delta that marks an element deleted at a version. It is the element's latest
  record until a delete marker removes it.
- **Rebuild**: filling an empty database by reading a delta topic from the start with the sink
  alone.
- **Live element**: a node or edge in the graph that is not marked deleted.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: A graph rebuilt from its compacted delta topic into an empty database, with no mapper
  running and no upstream topic read, has exactly the live elements of the original: every node and
  edge, with the same labels, properties and versions, and none besides tombstoned ones still in the
  topic.
- **SC-002**: After the broker has compacted, a delta topic for a thousand elements written ten times
  each holds fewer than twice a thousand records, and a rebuild from it reads fewer than a fifth of
  the records ever written.
- **SC-003**: Every delta whose key does not identify its element is refused: in a run mixing
  correctly and wrongly keyed deltas, none of the wrongly keyed ones is in the graph, and each
  refusal names the record and the key it should have had.
- **SC-004**: A mapper written with the SDK's helper cannot emit a wrongly keyed delta: the helper
  takes no key, and the sample's tests assert the keys it produces.
- **SC-005**: A delete marker never stalls a partition: a topic with markers in it is read to the
  end, with the markers counted.
- **SC-006**: A blueprint with a managed delta topic and no cleanup setting is deployed with that
  topic compacted, and verification said so beforehand; a blueprint that chose otherwise keeps its
  choice and is told the consequence.
- **SC-007**: An operator following the documented rebuild procedure restores a graph using the
  pipeline's existing resource and one reset command, without editing the mappers.

## Assumptions

- **One writer per element, in order.** As in the delta contract: an element's deltas come from one
  source in version order, on one partition because they share a key. Compaction keeping "the last
  record" is then the same as keeping "the highest version".
- **Compaction is the broker's.** The platform sets the policy when it creates a topic; when and how
  far the broker compacts is the broker's configuration. Nothing here depends on its timing.
- **A rebuilt graph is the live graph.** Elements tombstoned and since removed from the topic are
  not in a rebuild; placeholders for endpoints never described by a delta are rebuilt as
  placeholders. Removing tombstoned elements from a database that already holds them stays out of
  scope.
- **The key is text.** It is readable in a console consumer and stable across languages; its exact
  form is settled in the plan and stated on the contract's page.
- **The contract tightens under its existing name.** Requiring the key is a change an earlier
  writer does not survive; it is made without a new name because the contract is days old and its
  only known writer is this repository's sample, which moves with the feature. A later incompatible
  change takes a new name, as the contract's own rule says.
- **Existing topics are never altered**, as everywhere on the platform: a delta topic created before
  this feature keeps its policy until its owner changes it, and the documentation says how.
- **Partitions and replication** of a delta topic are as the blueprint or the cluster says; this
  feature sets only the cleanup policy by default.
- **The harness needs no broker.** A mapper's tests check keys through the SDK's harness; compaction
  itself is tested in the build against a real broker.

## Out of Scope

- Publishing deltas from an ankka service: a consumer that emits several deltas per event with
  versions from the entity's sequence number. A later feature in the ankka repository.
- Removing tombstoned elements from the graph database, and any compaction of the graph itself.
- Altering an existing topic's cleanup policy from the platform.
- Compaction defaults for topics that carry anything other than graph deltas.
- A second built-in stage, or a general mechanism for stages.
- Snapshots or exports of a graph other than its delta topic.

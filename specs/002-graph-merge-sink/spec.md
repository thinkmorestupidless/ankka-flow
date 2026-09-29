# Feature Specification: A graph merge sink built into the sidecar

**Feature Branch**: `002-graph-merge-sink`

**Created**: 2026-09-29

**Status**: Draft

**Input**: User description: "A batched graph merge sink built into the sidecar: the first stage that
runs inside the sidecar image with no process container. A blueprint declares it as a streamlet with
a built-in descriptor (`streamlets { graph = builtin/neo4j-merge-sink }`); the CLI knows the built-in
descriptors, verifies contracts exactly as for any streamlet, and the operator renders a pod with only
the sidecar. Records on its inlet are graph deltas in a versioned JSON contract — MergeNode, MergeEdge,
Tombstone — state-shaped (never increments), each carrying a global id and the source entity's
sequence number as its version, self-describing in the body. The sink writes each batch to Neo4j 5
over Bolt in one transaction: UNWIND per batch, version-guarded MERGE (a delta older than what the
graph holds is a no-op), tombstones mark rather than delete, and the inlet's offsets are committed
only after the transaction commits. Delivery stays at least once and per-key order is preserved
through the partition. Neo4j connection settings and credentials reach only the sidecar, from a
Kubernetes Secret named in the blueprint or deploy-time configuration, never in the resource. Upstream
streamlets in any language map domain events to deltas; the sink is generic and owns idempotence.
Observability: lag per inlet partition as for any streamlet, plus a metric for deltas written and
skipped as stale. Tested against Neo4j Community in testcontainers; a sample pipeline maps ankka's
shopping-cart checkout notices to deltas and merges them. Neo4j only in this version; Memgraph and
custom Cypher are out of scope."

## Context

Version one shipped the platform: streamlets in any language, a sidecar in every pod that owns Kafka,
blueprints verified before anything runs, and offsets committed only after the write. It shipped no
stage of its own, but its sidecar was written so that one could be: the seam between "read a batch
from Kafka" and "commit its offsets" takes any processor, and the remote process behind the protocol
is only the first. This feature is the second, and the reason the seam exists.

The stage is a **graph merge sink**. Services publish what happened to them as events on Kafka
topics; a central graph should be built from those topics by a pipeline rather than by a hand-written
service that subscribes to everything. The awkward half of that job is the same for every domain:
turning a stream of at-least-once, possibly redelivered, possibly rebuilt-from-the-start records into
a graph that is correct whatever order and however many times they arrive. The domain-specific half —
which events become which nodes and edges — is different every time and belongs in ordinary
streamlets, in whatever language their authors like.

So the sink is generic and owns idempotence. It reads **graph deltas**: small, self-describing JSON
records that say "this node now looks like this", "this edge now exists between these two", or "this
element is gone", each carrying a global id and a version that increases with the source entity's own
history. A delta is a statement of state, never an increment, so applying it twice is the same as
applying it once, and applying an old one after a newer one changes nothing. Upstream streamlets map
domain events to deltas; the sink merges them, in batches, in one transaction per batch, and commits
the inlet's offsets only when the transaction has. A pipeline that projects into a graph is only
trustworthy if it can be rebuilt from the start of its inputs and arrive at the same graph, and this
is what makes that true.

Because the stage ships inside the sidecar image, a pod running it has one container. Nothing else
about the pipeline changes: the blueprint names the sink as a streamlet, its inlet has a contract
that upstream outlets must match, verification catches a mismatch before anything runs, the resource
carries it like any streamlet, and lag, readiness and resets work as they do for every other inlet.
The one new thing an operator must provide is the graph database's address and credentials, which
reach only the sidecar, the way Kafka's do.

## Clarifications

### Session 2026-09-29

- Q: How does the sink know what to write — a fixed delta schema, or Cypher supplied per pipeline?
  → A: A fixed, versioned graph-delta schema. The sink is fully generic and owns idempotence;
  mapping from domain events to deltas happens in ordinary streamlets upstream. Custom Cypher is out
  of scope.
- Q: How is a stage with no image declared in a blueprint? → A: As a streamlet whose descriptor is
  built in (`streamlets { graph = builtin/neo4j-merge-sink }`). The CLI knows the built-in
  descriptors; verification, the resource and the operator treat it as a streamlet that happens to
  need no process container.
- Q: Which graph databases? → A: Neo4j 5 only in this version.
- Q: What does the sink do with a delta it cannot read — one that does not parse as the contract, or
  names a kind the contract does not define? → A: It fails the batch and the partition stalls, as
  for any record the platform cannot process. The platform never skips; the fix is upstream, in the
  mapping streamlet, and the stall is visible as lag and a warning event.
- Q: Who creates the uniqueness constraints the merge relies on? → A: The sink creates them at
  startup if its credentials allow, idempotently; if they do not, it records a warning event and
  continues, so the failure mode is a slow sink, not a stopped one.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - A graph that is right however the deltas arrive (Priority: P1)

A pipeline author wires a topic of graph deltas into the merge sink and points the sink at a graph
database. Deltas arrive: nodes, edges between them, and eventually a tombstone for one of the nodes.
The graph reflects them. The author then does everything the platform promises can happen — a delta
is delivered twice, an old version of a node arrives after a newer one, the sink is killed with a
batch in flight and restarted, the whole pipeline is reset and replays its inputs from the start —
and the graph is the same graph at the end of each.

**Why this priority**: This is the feature. A sink that merges correctly under redelivery, reordering
across sources, restart and rebuild is what makes a graph built by a pipeline trustworthy; without it
the stage is a convenience that a hand-written consumer could match.

**Independent Test**: Run Kafka, a graph database and the sidecar with the sink configured, and
produce a scripted sequence of deltas with a plain Kafka client: twenty nodes over five ids with
rising versions, edges between them, one tombstone, then every delta again, then the newest deltas
followed by older ones. Kill the sidecar after the first batch has been written but before it is
committed, and start it again. Query the graph: each node holds the properties of its highest
version, each edge exists once, the tombstoned node is marked and present, and nothing else. Reset
the inlet's offsets to the start and let it replay; query again and get an identical answer.

**Acceptance Scenarios**:

1. **Given** a node delta for an id the graph does not hold, **When** the sink applies it, **Then**
   a node with that id, its labels and its properties exists.
2. **Given** a node delta whose version is higher than the node the graph holds, **When** the sink
   applies it, **Then** the node's properties are replaced by the delta's, including the removal of
   properties the delta no longer carries.
3. **Given** a node delta whose version is equal to or lower than the node the graph holds,
   **When** the sink applies it, **Then** the node is unchanged and the delta is counted as stale.
4. **Given** an edge delta naming two node ids, **When** the sink applies it, **Then** exactly one
   edge of that type with that id exists between those nodes, whether or not either node had been
   written before; a node the edge names that has not yet been written exists with only its id until
   its own delta arrives.
5. **Given** a tombstone for an element, **When** the sink applies it, **Then** the element remains
   in the graph, marked as deleted with the tombstone's version, and a later delta for it with a
   lower version changes nothing.
6. **Given** a batch of deltas, **When** the sink writes it, **Then** either every delta in the
   batch is applied or none is, and the inlet's offsets for the batch are committed only after the
   write has been confirmed by the database.
7. **Given** the sink dies after a batch's write is confirmed but before its offsets are committed,
   **When** it restarts, **Then** the batch is read again, every delta in it is found stale or
   already applied, and the graph is unchanged.
8. **Given** a pipeline whose inputs are replayed from the start, **When** every delta has been
   applied again, **Then** the graph is identical to the graph before the replay.

---

### User Story 2 - Declared, verified and deployed like any streamlet (Priority: P2)

A pipeline author adds the sink to a blueprint as a streamlet with a built-in descriptor, connects
an upstream outlet to its inlet, and runs `flow verify`. The check refuses an upstream outlet whose
contract is not the graph-delta contract, exactly as it refuses any mismatched pair of ports, before
anything is deployed. `flow generate` needs no image for the sink and writes a resource the operator
runs as a pod with one container. The pipeline shows the sink's inlet lag, readiness and events the
way it shows every streamlet's, and `flow reset` can reset it.

**Why this priority**: The sink earns its place by being an ordinary part of a pipeline. If it needed
its own resource, its own verification or its own operating procedures, it would be a second product.

**Independent Test**: Take the cart pipeline's blueprint, add the sink fed by a new streamlet's
outlet, and run `flow verify` with the upstream outlet declaring the wrong contract: it refuses,
naming both ports. Correct the contract: it passes without a descriptor file for the sink. Run
`flow generate` with images for every other streamlet and none for the sink; apply the resource on a
cluster; the sink's pod has one container, becomes ready, and `kubectl get aflow` reports the pipeline
`Ready`. Scale the sink to zero and `flow reset` it; the operator records the reset event for its
consumer group.

**Acceptance Scenarios**:

1. **Given** a blueprint naming `builtin/neo4j-merge-sink` as a streamlet's descriptor, **When**
   `flow verify` runs with no descriptor file for it, **Then** the sink's inlet and its contract are
   known and verified against the topic's producers.
2. **Given** an upstream outlet whose contract differs from the graph-delta contract, **When**
   `flow verify` runs, **Then** it refuses, naming both ports and both contracts.
3. **Given** `flow generate` run with images for every streamlet except the sink, **When** it
   writes the resource, **Then** it does not refuse for the sink's missing image, and the resource
   carries the sink's descriptor and its inlet binding like any streamlet's.
4. **Given** a resource with a built-in streamlet, **When** the operator renders it, **Then** the
   streamlet's pod has only the sidecar container, with the graph database's connection settings
   mounted from a Secret and nothing about them in the resource.
5. **Given** a blueprint that gives the sink an outlet, or connects its inlet to nothing, **When**
   `flow verify` runs, **Then** it refuses with the same messages any streamlet would get.
6. **Given** a running sink, **When** an operator asks for its lag, readiness or events, **Then**
   they are reported as for any streamlet, under the same names.

---

### User Story 3 - Watched while it writes (Priority: P3)

An operator watching a pipeline that feeds the sink can see how many deltas it has written and how
many it found stale, per inlet partition, beside the lag the platform already reports. When the graph
database is unreachable or refuses a write, the batch is not committed, the sink retries and reports
not ready, lag grows, and the reason is in the sink's log and, once the stall passes the threshold,
on its pod as a warning event, exactly as for a streamlet whose process fails a batch.

**Why this priority**: A sink that silently found every delta stale, or silently stopped writing,
would look healthy from Kafka's side. The counts and the failure path make its work visible.

**Independent Test**: Run the sink against a graph database, write deltas, and scrape its metrics:
written and stale counts match the scripted sequence. Stop the database; produce more deltas; the
sink reports not ready, its lag grows, its log names the database error, and after the stall
threshold a warning event is recorded on the pod. Start the database; the sink becomes ready, the lag
drains, and the graph is complete.

**Acceptance Scenarios**:

1. **Given** deltas applied by the sink, **When** its metrics are read, **Then** they report the
   number of deltas written and the number found stale, labelled by inlet and partition.
2. **Given** a graph database that cannot be reached, **When** the sink has a batch to write,
   **Then** the batch is not committed, the sink retries with backoff, reports itself not ready,
   and the partition's lag grows.
3. **Given** a graph database that refuses a batch's write, **When** the sink retries and the
   refusal persists past the stall threshold, **Then** a warning event naming the partition is
   recorded on the pod, and the refusal's reason is in the sink's log.
4. **Given** the database becomes reachable again, **When** the sink's next attempt succeeds,
   **Then** it reports ready and every batch written since the outage is in the graph exactly once.

---

### User Story 4 - A sample beside ankka (Priority: P4)

A developer wants to see a graph built from a real ankka service. A sample pipeline reads the
checkout notices ankka's shopping cart publishes, a Python streamlet maps each one to deltas — a cart
node, a checkout node and an edge between them — and the sink merges them. The developer checks a
cart out through the shopping cart's API and finds the cart and its checkout in the graph.

**Why this priority**: The sample is the tutorial: it shows a mapping streamlet written in another
language, the delta contract as an upstream author sees it, and the sink as one more streamlet in a
pipeline. It depends on everything above.

**Independent Test**: On a cluster running ankka's shopping cart with a broker configured, deploy the
sample pipeline. Check out three carts. Query the graph: three cart nodes, three checkout nodes, an
edge from each cart to its checkout, each checkout carrying the time the notice recorded. Check the
same cart out again after a reset of the pipeline: the graph is unchanged.

**Acceptance Scenarios**:

1. **Given** the sample pipeline deployed beside the shopping cart, **When** a cart is checked out,
   **Then** the graph holds a node for the cart, a node for the checkout, and an edge between them.
2. **Given** the sample's mapping streamlet, **When** a reader looks at its code, **Then** the
   deltas it emits are built from the shopping cart's notice with no reference to the graph
   database or to how the sink writes.

---

### Edge Cases

- A delta that does not parse as the graph-delta contract, or names a kind the contract does not
  define: the batch fails and the partition stalls, as for any record the platform cannot process.
  The sink's log names the record's offset and what was wrong with it; nothing is skipped, and the
  fix is in the mapping streamlet upstream, followed by a reset or the record ageing out.
- A node delta and an edge delta for the same ids in one batch, in either order: the batch applies
  both, and the edge exists between the nodes whichever came first.
- An edge whose endpoints are tombstoned: the edge is written; the tombstone marks, it does not
  remove, and the graph's readers decide what a marked node means.
- Two deltas for the same element with the same version but different properties: the second is
  stale; the first writer wins, because equal versions should not differ and the sink cannot choose.
- A delta whose version is lower than the element's tombstone: stale.
- A property whose value is a nested object or an array of objects: refused by the contract; a
  property is a scalar or an array of scalars, so the graph's readers see plain values.
- A batch that is larger than the database will accept in one transaction: the sink splits nothing;
  the inlet's batch size bounds it, and the limit is documented with the sink's parameters.
- The graph database is reachable but the credentials are wrong: the sink never becomes ready, and
  its log says why, without printing the credentials.
- The Secret the blueprint names does not exist: the operator reports it as it reports a missing
  Kafka cluster Secret, and the pipeline is not run.
- The sink's inlet is on a partitioned topic and its consumer group is rebalanced while a batch is in
  flight: the batch's transaction may complete, but its offsets are not committed by the old owner;
  the new owner reads the batch again and finds it applied.
- A reset while the sink is running: refused by `flow reset`, as for any streamlet.
- The graph holds an element with a version but no record of who wrote it (written by something
  other than the sink): the sink treats it as its own, and a delta with a higher version replaces it.

## Requirements *(mandatory)*

### Functional Requirements

**The delta contract**

- **FR-001**: The platform MUST define a JSON contract for graph deltas with a versioned schema name,
  so that any streamlet, in any language, can declare an outlet of it and any blueprint can verify
  the connection to the sink's inlet.
- **FR-002**: A delta MUST be one of three kinds — a node merge, an edge merge, or a tombstone — and
  MUST say which it is in its body.
- **FR-003**: Every delta MUST carry the global id of the element it describes and a version that is
  a non-negative integer increasing with the source entity's own history; a node merge MUST carry
  its labels and properties; an edge merge MUST carry its type, its endpoints' ids and its
  properties; a tombstone MUST say whether it marks a node or an edge.
- **FR-004**: A delta MUST be a statement of state: a node merge carries every property the node
  now has, and applying a delta twice MUST leave the graph as applying it once did.
- **FR-005**: A property value MUST be a string, number, boolean, or array of one of those; the
  contract MUST refuse nested objects.
- **FR-006**: The record key of a delta SHOULD be the id of the element it describes, so that every
  delta for one element is on one partition and applied in order; the sink MUST NOT depend on it.

**Merging**

- **FR-007**: The sink MUST apply a node merge only when its version is higher than the version the
  graph holds for that id, and MUST then replace the node's properties with the delta's, removing
  any the delta does not carry.
- **FR-008**: The sink MUST apply an edge merge only when its version is higher than the version the
  graph holds for that edge id, and MUST create any endpoint node that does not yet exist with its
  id alone.
- **FR-009**: The sink MUST apply a tombstone by marking the element as deleted at that version,
  never by removing it, and MUST treat any later delta for the element with a lower version as
  stale.
- **FR-010**: The sink MUST count a delta whose version is not higher than the graph's as stale and
  apply nothing for it.
- **FR-011**: The sink MUST write every delta of a batch in one transaction, so that a batch is
  applied entirely or not at all.
- **FR-012**: The sink MUST commit a batch's offsets only after the database has confirmed the
  batch's transaction, and MUST NOT commit them if it has not.
- **FR-013**: When a write fails, the sink MUST NOT commit the batch, MUST retry it with backoff for
  as long as it takes, MUST report itself not ready while it cannot write, and MUST record the stall
  warning event as a streamlet whose process fails a batch does.
- **FR-014**: The sink MUST preserve per-partition order: batches of one partition are written and
  committed in the order they were read.
- **FR-015**: The sink MUST fail a batch holding a delta it cannot read — one that does not parse as
  the contract or names a kind the contract does not define — and MUST NOT skip it; the failure is
  retried, reported and stalled exactly as a refused write is, and the log names the record's offset
  and the problem.
- **FR-016**: At startup the sink MUST ensure a uniqueness constraint on the id it merges nodes by
  and on the id it merges edges by, creating them if its credentials allow and leaving them if they
  exist; if it cannot create them it MUST record a warning event naming the missing constraint and
  continue.

**Declaration and deployment**

- **FR-017**: A blueprint MUST be able to name a built-in descriptor for a streamlet in place of a
  descriptor file, and `flow verify` and `flow generate` MUST know the platform's built-in
  descriptors without any file on disk.
- **FR-018**: The sink's built-in descriptor MUST declare one inlet of the graph-delta contract, no
  outlets, and its parameters.
- **FR-019**: Verification MUST treat the sink as any streamlet: its inlet's contract is checked
  against the topic's producers, an unconnected inlet is refused, and a port the descriptor does not
  declare is refused.
- **FR-020**: `flow generate` MUST NOT require an image for a streamlet with a built-in descriptor,
  and the resource MUST record that the streamlet is built in.
- **FR-021**: The operator MUST render a streamlet with a built-in descriptor as a pod with the
  sidecar container only, and MUST refuse a resource that gives such a streamlet an image.
- **FR-022**: The sink's connection to the graph database — its address, database name and
  credentials — MUST come from a Kubernetes Secret named by the blueprint or by deploy-time
  configuration, MUST be mounted into the sidecar, and MUST NOT appear in the resource.
- **FR-023**: The operator MUST report a missing connection Secret as it reports a missing Kafka
  cluster Secret, and MUST NOT run the pipeline until it exists.
- **FR-024**: On a laptop, the sink MUST be runnable from the sidecar image with the same
  configuration file the sidecar already reads, with the connection settings given there.
- **FR-025**: The sink MUST become ready only once it can reach the database and every inlet is
  subscribed, and MUST report itself not ready while it cannot write.

**Observability**

- **FR-026**: The sink MUST expose the count of deltas written and the count found stale, labelled
  by inlet and partition, beside the lag metrics every inlet has.
- **FR-027**: The sink's log MUST name the database's reason for a refused write without printing
  credentials.
- **FR-028**: `flow reset` MUST apply to the sink as to any streamlet with an inlet, and a replay
  MUST leave the graph identical.

**The sample and the documentation**

- **FR-029**: A sample pipeline MUST map ankka's shopping-cart checkout notices to deltas in a
  streamlet written in another language and merge them with the sink, runnable beside ankka on a
  local cluster.
- **FR-030**: The documentation MUST include the delta contract as a reference page, a guide to
  building a graph from a pipeline, and the sink's parameters, and the sink MUST be carried by the
  agent skills.
- **FR-031**: The sink MUST be tested against a real graph database in the build, including
  redelivery, reordering across sources, restart with a batch in flight, and a full replay.

### Key Entities

- **Graph delta**: one record on the sink's inlet: a kind (node merge, edge merge, tombstone), the
  global id of the element, a version, and for a merge the element's state — labels and properties
  for a node, type, endpoints and properties for an edge. Self-describing; never an increment.
- **Element**: a node or an edge in the graph, identified by a global id, holding the version of the
  last delta applied and whether it is marked deleted.
- **Built-in descriptor**: a streamlet descriptor the platform ships rather than an SDK writes, named
  in a blueprint by a reserved prefix; the sink's is the first.
- **Connection Secret**: the graph database's address, database name and credentials, held in the
  cluster and mounted into the sidecar; named by the blueprint or deploy-time configuration.
- **Batch**: the records of one inlet partition delivered together, written in one transaction, and
  committed as one.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: After a scripted sequence of deltas that includes every delta delivered twice, older
  versions after newer ones, a restart with a batch in flight and a full replay from the start, the
  graph is identical to the graph after the sequence delivered once, in order, with no restart:
  every node, every edge, every property and every deletion mark.
- **SC-002**: A batch is never committed before its write is confirmed: in a run where the sink is
  killed at every point between reading a batch and committing it, no delta is missing from the
  graph and no element holds a version lower than one it was sent.
- **SC-003**: A blueprint that connects an outlet of the wrong contract to the sink is refused by
  `flow verify` before anything is deployed, naming both ports; a correct one is verified with no
  descriptor file for the sink.
- **SC-004**: A pipeline with the sink becomes `Ready` on a cluster with one container in the sink's
  pod and nothing about the graph database in the resource, and the sink's lag, readiness, events
  and reset behave as any streamlet's do.
- **SC-005**: With the graph database stopped, the sink reports not ready within the platform's
  readiness period, its lag grows, and a warning event is recorded once the stall threshold passes;
  with the database started again, every delta produced during the outage is in the graph exactly
  once.
- **SC-006**: The sample pipeline builds a graph of the shopping cart's checkouts beside a running
  ankka, and a reader of its mapping streamlet finds no reference to the graph database in it.
- **SC-007**: The sink sustains the platform's throughput floor: at least 1,000 deltas per second
  per inlet partition against a local graph database, with the batch's transaction as the only
  write per batch.

## Assumptions

- **One writer per element.** An element's version is the sequence number of the one source entity
  whose deltas describe it. A node that several source entities would contribute to is modelled
  upstream as several nodes and edges, not merged by property; the sink compares one version per
  element and does not merge properties from different sources.
- **Identity is the id, not the labels.** A node is found by its global id alone; labels are set
  from the delta and may change between versions. Ids are unique across all nodes, and edge ids
  across all edges, in one graph.
- **The sink keeps the version on the element.** How the version and the deletion mark are stored on
  the element is the sink's business and is documented so that readers of the graph can see them,
  but no reader is expected to write them.
- **Tombstones keep edges.** Marking a node deleted does not mark or remove its edges; a pipeline
  that wants an edge gone sends a tombstone for the edge.
- **One graph database per sink.** A sink writes to one database named in its connection Secret. A
  pipeline that writes to two runs two sinks.
- **The delta contract is versioned by name.** Its schema name ends in `.v1`; a change that is not
  compatible is a new name, and the sink of that version declares the new name.
- **Batch size is the inlet's.** The sink's transaction holds the inlet's batch, bounded by the
  batch settings every inlet has; the sink adds no batching of its own.
- **Uniqueness constraints on the id.** A merge is a lookup rather than a scan only with a
  uniqueness constraint on the id property. The sink creates one for nodes and one for edges at
  startup when its credentials allow; when they do not, it warns and continues, and the
  documentation says how an operator creates them with a privileged account instead.
- **Neo4j 5 Community is the tested target**; Enterprise is expected to work and is not tested.
- **The shopping cart publishes checkout notices** when configured with a broker, as it does since
  the previous feature; the sample depends on it and on a broker both ankka and the pipeline reach.

## Out of Scope

- Any graph database other than Neo4j 5; Memgraph and other Bolt-speaking databases may follow.
- Cypher supplied by a pipeline; the sink runs only its own merge.
- Deltas that increment, append or otherwise depend on the graph's current state.
- Merging properties from several sources into one element.
- Physical deletion of tombstoned elements, and any compaction or garbage collection of the graph.
- Reading from the graph: the sink writes only; queries belong to the graph's own clients.
- A second built-in stage, or a general mechanism for third parties to add stages to the sidecar
  image. This feature adds the first stage and the declaration path; the mechanism is judged on a
  second stage.
- Exactly-once delivery. The sink is idempotent under at-least-once delivery, which is what makes
  exactly-once unnecessary for it.
- Schema evolution of the delta contract beyond "a new version is a new name".

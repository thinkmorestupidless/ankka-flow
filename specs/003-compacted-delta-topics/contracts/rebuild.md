# Contract: rebuilding a graph from its delta topic

## The procedure

For a pipeline `<p>` whose sink streamlet is `<sink>`, with the mappers stopped or running:

1. **Stop the sink.** `flow.streamlets.<sink>.replicas = 0` in a `--conf` file; `flow generate … |
   kubectl apply -f -`; wait for its pod to go.
2. **Give it an empty database.** Empty the one its Secret names, or point the Secret at an empty
   one (the sink is rolled when the Secret changes).
3. **Reset the sink alone.** `flow reset <p> --streamlet <sink>`; the operator records
   `ResetOffsets` for the sink's group only.
4. **Start the sink.** Remove the override, generate, apply. It reads the delta topic from the
   start; when its lag is zero the graph is rebuilt.

No mapper is reset and no upstream topic is read. New deltas arriving meanwhile are applied in
their place.

## What the rebuilt graph contains

- Every **live** element, with its latest labels, properties and version: identical to the
  original.
- Every tombstoned element whose tombstone is still in the topic, marked deleted.
- Placeholders for endpoints that no delta ever described.
- **Not** tombstoned elements whose records a delete marker removed and the broker has compacted
  away.

It does not depend on the broker having compacted: an uncompacted topic rebuilds the same live
graph, reading more records to get there.

## What it needs of the topic

- Keys that identify elements (the sink refuses anything else, so a topic the sink has read to the
  end has them).
- A cleanup policy that keeps every element's last record: `compact`. With `compact,delete`,
  elements untouched for longer than the retention are missing. With `delete` alone, the topic
  holds only recent history and cannot rebuild the graph.
- Delete markers only after tombstones. A marker for a live element makes the rebuilt graph differ
  from the original once the broker compacts it.

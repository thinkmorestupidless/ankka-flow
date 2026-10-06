---
name: ankka-flow-python
description: Write, test and package an ankka-flow streamlet in Python with the ankka-flow SDK — declaring a Streamlet with JsonInlet, JsonOutlet and typed parameters, the process(batch) function and its emits, acknowledging, skipping and failing a batch, serve(), writing and checking flow/descriptor.json with uv run descriptor, the testkit Harness, the local laptop loop with the sidecar in docker compose, and the image. Use when the task is Python code for a streamlet (for Scala, ankka-flow-scala), its tests, its descriptor, its Dockerfile, or running it on a laptop. Also mapping events to graph deltas (ankka.graph-delta.v1) for the Neo4j merge sink with GraphDeltaOutlet, which keys each delta by its element.
pages:
  - build/python-streamlet.md
  - build/testing.md
  - build/images.md
  - reference/python-sdk.md
  - reference/descriptor.md
  - reference/sidecar.md
  - concepts/contracts.md
  - concepts/delivery.md
  - get-started/first-streamlet.md
  - reference/graph-deltas.md
---

# Writing a streamlet in Python

A Python streamlet is a subclass of `ankka_flow.Streamlet` that declares its ports and parameters as
class attributes and implements `process(batch)`, yielding emits. `serve()` runs it on
`127.0.0.1:$FLOW_PROCESS_PORT`, where the sidecar in the same pod finds it. The SDK writes the
streamlet's descriptor from the declaration; the descriptor is what a blueprint is checked against and
what the sidecar compares with the running process before it starts.

## Rules

1. **Declare everything; discover nothing.** Ports (`JsonInlet`, `JsonOutlet`) and parameters
   (`StringParameter`, `IntegerParameter`, `DoubleParameter`, `BooleanParameter`, `DurationParameter`,
   `MemorySizeParameter`) are class attributes. There is no classpath or module scanning.
2. **The descriptor is written, committed and checked.** `uv run descriptor` writes
   `flow/descriptor.json` from the streamlet named in `[tool.ankka-flow] streamlet` in
   `pyproject.toml`; `uv run descriptor --check` exits 1 when the committed file differs. Change a
   port, contract or parameter and the descriptor changes; the sidecar refuses to start a process
   whose declaration differs from the deployed descriptor.
3. **`process` is synchronous and per batch.** It is called on a worker thread, never twice at once
   for one partition, possibly concurrently for different partitions. Returning normally acknowledges
   the batch; raising fails it, which fails the stream: every batch in flight in the pod is voided and
   redelivered from the last commit. Emits are held by the sidecar until the ack and discarded on a
   failure.
4. **Skip by not emitting.** A record the streamlet does not want — including one it cannot decode —
   is skipped by yielding nothing for it. Raising for it stalls the partition until the code changes.
5. **Bytes in, bytes out.** `record.value` is the bytes Kafka held. `outlet.emit(record)` forwards the
   same key, headers and value; `outlet.emit(value=..., key=..., headers=...)` builds a new record.
   Keep the key to keep per-key ordering downstream.
6. **Tolerate repeats.** Delivery is at least once; a record may arrive again after a failure.
7. **The process gets `FLOW_PROCESS_PORT` and nothing else.** No Kafka address, no credentials, no
   ports to expose, no probes. The image holds only the streamlet's code.
8. **Test with the Harness first.** `ankka_flow.testkit.Harness` calls `process` with batches it
   builds and applies the protocol's rules, with no Kafka, sidecar or gRPC.
9. **Deltas for the graph sink go through `GraphDeltaOutlet`.** Declare
   `deltas = GraphDeltaOutlet("deltas")` and yield `self.deltas.node(record, id=…, version=…,
   labels=[…], properties={…})`, `.edge(record, id=…, version=…, type=…, from_id=…, to_id=…)`,
   `.tombstone_node(…)` or `.tombstone_edge(…)`. Each delta is the element's whole state, with a
   version that rises with the source entity. The outlet sets the record key to the element key
   (`node:<id>` or `edge:<id>`), which the sink requires, and raises `ValueError` for what the sink
   would refuse. Passing `record` keeps its headers and tells the Harness the record was not skipped.
   In a test, `ankka_flow.graph.read(emitted)` parses a record back and checks its key.

## Before writing

- What is each inlet's and outlet's schema name, and does it match the topic's other ports?
- Which records are skipped, which fail the batch, and is that intended?
- Does the key survive every emit that needs downstream ordering?
- Is the descriptor regenerated and committed after changing the declaration?

## Mistakes to check for

- `import kafka` or any Kafka client in streamlet code; the sidecar owns Kafka.
- Raising on a malformed record when the intent was to drop it.
- Per-partition or per-key state held in memory across batches.
- `serve()` bound to anything but loopback, or a Dockerfile that exposes a port.
- A stale `flow/descriptor.json`, or one edited by hand.
- Graph deltas built by hand with `JsonOutlet.emit(…, key=…)`: a key that is the bare id, or the
  input record's key, is refused by the sink. Use `GraphDeltaOutlet`.

# The ankka-flow streamlet protocol, `ankka.flow.v1`

This directory is the platform's promise to every SDK. Copy it into an SDK **verbatim**; CI diffs
each copy against this one. It holds:

| path | what |
|---|---|
| `src/main/protobuf/ankka/flow/v1/payload.proto` | `Record`, `Header`, `Error`, `Problems` |
| `src/main/protobuf/ankka/flow/v1/discovery.proto` | `Discovery`: the process describes itself; the descriptor is its `Spec` |
| `src/main/protobuf/ankka/flow/v1/streamlet.proto` | `Streamlet.Run`: one bidirectional conversation per streamlet instance |
| `DESCRIPTOR.md` | the descriptor file's canonical JSON |
| `fixtures/declarations/` | streamlet declarations in prose |
| `fixtures/descriptors/` | the exact bytes each declaration must produce |
| `fixtures/conversations/` | the scripted inputs the conformance suite sends |

## Directions

The developer's process serves `Discovery` and `Streamlet` on `127.0.0.1:$FLOW_PROCESS_PORT`
(default 9010), bound to loopback only. The sidecar dials it. The sidecar binds no gRPC port in
`1.x`; 9011 and `FLOW_SIDECAR_PORT` are reserved for a callback service a later minor may add.
The sidecar sends no HTTP/2 keepalive pings, so a process keeps its gRPC library's default ping
policy; one that ends connections for too many pings is never provoked.

## Discovery

1. The sidecar calls `Discover`, retrying with backoff (500 ms doubling to 10 s) until the process
   answers. It is not ready until then.
2. It checks the `Spec`'s protocol version, validates the descriptor, and compares it field by
   field with the descriptor it was deployed with.
3. On any problem it calls `ReportError` once with every problem, logs them, and exits.

## Run

1. The sidecar sends `Start`. The process sends nothing before it.
2. The sidecar sends `Batch`es: at most one in flight per (inlet, partition), each partition's in
   offset order. Batches for different partitions interleave freely.
3. The process answers each batch with zero or more `Emit` and then exactly one `Ack` or `Fail`.
   Messages for different batches may interleave.
4. On `Ack` the sidecar produces the batch's emits, waits for the broker to confirm every one, and
   only then commits the batch's offsets.
5. On shutdown the sidecar sends `Stop` and half-closes.

## Rules the messages do not state

- **One batch in flight per (inlet, partition).** A partition's batches arrive in offset order.
- **Emits precede the ack.** An emit after its batch's ack fails the stream.
- **Commit after the write.** Offsets are committed only after every emit for the batch is confirmed.
- **A `Fail` fails the stream, and nothing is skipped.** The sidecar voids every in-flight batch,
  reconnects with backoff, repeats discovery, sends a new `Start`, and redelivers from the last
  commit, indefinitely.
- **Skipping is acking without emitting.** The sidecar never knows a record was skipped.
- **A rebalance discards.** An emit or ack for a partition the sidecar no longer owns is dropped
  silently and nothing is committed for it; the new owner reads it again. This is not a violation.
- **The sidecar never decodes a value.** A contract is a format and a fingerprint.
- **A keyless emit is partitioned by Kafka's default partitioner.** Per-key order is promised for
  keyed records only.
- **A new `Start` voids everything.** State tied to an older `conversation_id` must be discarded.
- **Violations fail the stream:** an emit naming an outlet not in `Start.outlets`; an emit, ack or
  fail for a batch that was never sent or is already acknowledged; an emit after its ack.
- **Message size.** Batches are bounded by count, bytes and time so they stay under 4 MiB. A single
  input record over the limit fails the stream naming its topic, partition and offset. An emit must
  stay under `Start.max_message_bytes`.

## Versioning

`protocol_version` is `MAJOR.MINOR`, `1.0` here. The sidecar accepts a `Spec` of its own major
whose minor is not later than its own, and refuses anything else naming both. Adding an optional
field, a message, an rpc, a `ConfigType` value, a contract `format` or a fixture is a minor.
Renaming, removing or re-meaning anything is a major.

## Proving an SDK

Two things define a compatible SDK:

- **The descriptor fixtures.** Declare each streamlet in `fixtures/declarations/` in your language,
  write its descriptor with `sdk` pinned to `{"name": "fixture", "version": "0.0.0"}`, and assert
  the bytes equal `fixtures/descriptors/<name>.json`.
- **The conformance suite.** Implement the `conformance` reference streamlet, serve it on a port,
  and run `sbt 'sidecar/testOnly *ConformanceSuite' -Dflow.conformance.target=127.0.0.1:9010` from
  the ankka-flow repository. Every case is named; a failure names the conversation that broke.

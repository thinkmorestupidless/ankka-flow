# Contract: the streamlet protocol, `ankka.flow.v1`

**Feature**: [spec.md](../spec.md) | **Data model**: [data-model.md](../data-model.md) | **Research**: R1–R3, R5, R10

The protocol is the platform's promise to every SDK. It is versioned on its own (`1.0` here),
carried in discovery, and the sidecar refuses another major. Within a major, fields, messages,
rpcs and fixtures are only ever added, with defaults that mean "as before". The artefact is the
`protocol/` directory, copied verbatim into every SDK (FR-027):

```
protocol/
├── README.md                          # version rule; the rules the messages do not state (below)
├── DESCRIPTOR.md                      # the descriptor file's canonical JSON (contracts/descriptor.md)
├── fixtures/
│   ├── descriptors/<name>.json        # descriptors every SDK must reproduce byte for byte
│   ├── declarations/<name>.md         # the declaration each descriptor is written from, in prose
│   └── conversations/<name>.json      # scripted conversations the conformance suite replays
└── src/main/protobuf/ankka/flow/v1/
    ├── payload.proto
    ├── discovery.proto
    └── streamlet.proto
```

## Directions and addresses

| service | implemented by | dialled by | address |
|---|---|---|---|
| `Discovery`, `Streamlet` | the developer's process | the sidecar | `127.0.0.1:${FLOW_PROCESS_PORT}` (default 9010) |

The process binds loopback only (FR-007). The sidecar binds no gRPC port in version one; 9011
and `FLOW_SIDECAR_PORT` are reserved (R2).

## `payload.proto`

```proto
syntax = "proto3";
package ankka.flow.v1;

// A Kafka record, exactly as Kafka holds it. The sidecar never inspects `value`.
message Record {
  optional bytes key = 1;        // absent = Kafka's default partitioner decides
  repeated Header headers = 2;   // order preserved
  bytes value = 3;
}

message Header { string key = 1; bytes value = 2; }

message Error { string message = 1; }

message Problem { string message = 1; }
message Problems { repeated Problem problems = 1; }

message Empty {}
```

## `discovery.proto`

```proto
syntax = "proto3";
package ankka.flow.v1;
import "ankka/flow/v1/payload.proto";

service Discovery {
  rpc Discover (SidecarInfo) returns (Spec);
  rpc ReportError (Problems) returns (Empty);   // the sidecar's refusal, so it appears in the process's log
}

message SidecarInfo { string protocol_version = 1; string sidecar_version = 2; }

// `Spec` is the descriptor. The SDK writes this same message as canonical JSON to descriptor.json
// at build time (DESCRIPTOR.md); the sidecar compares the deployed file with this answer.
message Spec {
  string protocol_version = 1;             // "1.0"
  SdkInfo sdk = 2;
  StreamletDescriptor streamlet = 3;
}

message SdkInfo { string name = 1; string version = 2; }

message StreamletDescriptor {
  string name = 1;                         // [a-z0-9-]{1,63}; the name a blueprint refers to
  string description = 2;
  repeated Port inlets = 3;                // names unique across inlets and outlets together
  repeated Port outlets = 4;
  repeated ConfigParameter config_parameters = 5;
}

message Port { string name = 1; Contract contract = 2; }

message Contract {
  string format = 1;                       // "json" is the only value in 1.0
  string schema_name = 2;                  // e.g. "cart-events.v1"
  string fingerprint = 3;                  // Base64(SHA-256(UTF-8(schema_name)))
}

message ConfigParameter {
  string key = 1;                          // [a-z][a-z0-9-]*
  string description = 2;
  ConfigType type = 3;
  string default_value = 4;                // absent = required at deploy time
}

enum ConfigType { STRING = 0; INTEGER = 1; DOUBLE = 2; BOOLEAN = 3; DURATION = 4; MEMORY_SIZE = 5; }
```

## `streamlet.proto`

```proto
syntax = "proto3";
package ankka.flow.v1;
import "ankka/flow/v1/payload.proto";

service Streamlet {
  // One conversation per streamlet instance. The sidecar opens it and speaks first.
  rpc Run (stream ToProcess) returns (stream FromProcess);
}

message ToProcess {
  oneof message { Start start = 1; Batch batch = 2; Stop stop = 3; }
}

message Start {
  string conversation_id = 1;              // new on every (re)connect; state tied to an old one is void
  string pipeline = 2;
  string streamlet = 3;
  string config_json = 4;                  // {"key": value} for every declared parameter, resolved
  repeated PortBinding inlets = 5;
  repeated PortBinding outlets = 6;
  uint32 max_message_bytes = 7;            // an Emit must stay under this (4 MiB in 1.0)
}

message PortBinding { string port = 1; string topic = 2; }

message Batch {
  uint64 batch_id = 1;                     // unique within the conversation, increasing
  string inlet = 2;
  int32 partition = 3;
  repeated InputRecord records = 4;        // in offset order
}

message InputRecord { int64 offset = 1; int64 timestamp_ms = 2; Record record = 3; }

message Stop { string reason = 1; }

message FromProcess {
  oneof message { Emit emit = 1; Ack ack = 2; Fail fail = 3; }
}

message Emit { uint64 batch_id = 1; string outlet = 2; Record record = 3; }
message Ack  { uint64 batch_id = 1; }
message Fail { uint64 batch_id = 1; Error error = 2; }
```

## Conversations, one by one

### Discovery

1. The sidecar dials `Discover(SidecarInfo)` with backoff (500 ms doubling to 10 s) until it
   answers; it is not ready until then (S1.5).
2. It checks `Spec.protocol_version` against its own: another major, or a later minor, is refused
   naming both (FR-017, edge case).
3. It compares `Spec.streamlet` field by field with the deployed `descriptor.json` (R5) and
   validates it: a name that is not `[a-z0-9-]{1,63}`, a port name declared twice across inlets
   and outlets, a `format` other than `json`, a `fingerprint` that is not
   `Base64(SHA-256(schema_name))`, a duplicate parameter key. Every problem is collected.
4. On problems: `ReportError(Problems)` with all of them, one log line each, exit 1 (FR-008, S1.6).
5. On success: `Run`.

### Run

1. The sidecar sends `Start`. The process sends nothing before it.
2. For each (inlet, partition) the sidecar owns, at most one `Batch` is in flight. Batches for
   different partitions interleave freely; within a partition they arrive in offset order
   (FR-010).
3. The process answers a batch with zero or more `Emit` then exactly one `Ack` or `Fail`
   (FR-011). Emits and the ack for one batch may interleave with those of other batches.
4. On `Ack`: the sidecar produces every buffered emit for that batch to its outlet's topic with
   the key and headers given, awaits every broker confirmation, then commits the batch's offsets
   (FR-012, S1.2). A produce failure fails the stream.
5. On `Fail`: the stream fails (below).
6. On shutdown the sidecar sends `Stop` and half-closes; the process completes its side.

### Skipping a record

The process acknowledges the batch without emitting for that record (S1.7). The sidecar never
knows.

### Failing the stream

Any of: `Fail`; an `Emit` naming an outlet not in `Start.outlets`; an `Emit`, `Ack` or `Fail` for
a `batch_id` not in flight (never sent, already acknowledged, or revoked); an `Emit` after its
`Ack`; a message the sidecar cannot parse; the process closing the stream or becoming unreachable;
a produce failure. The sidecar then: completes every in-flight batch as failed with nothing
committed; removes the ready file; closes the conversation; backs off (500 ms doubling to 30 s);
repeats **Discovery** in full; sends a new `Start` with a new `conversation_id`; resumes from the
last committed offsets. The process must discard anything tied to the old conversation (edge
case). A batch that fails every time stalls its partition and, after `FLOW_STALL_WARNING_AFTER`,
is a warning event once per stall (FR-012a). The sidecar never skips.

### Rebalance

A partition revoked while its batch is in flight: the batch is marked revoked, its `Ack` (and
emits) are discarded, nothing is committed, and the new owner reads the batch again (edge case).
An `Emit` or `Ack` for a revoked batch is **not** a violation; it is dropped silently, since the
process could not have known.

### Message limits

A `Batch` is whatever arrived for its partition while the previous batch was with the process,
capped by `max-records` and `max-bytes` (defaults 100 and 1 MiB) so it stays under the 4 MiB message
limit. A quiet stream sends each record at once; nothing waits on a timer. One input record over the limit fails the stream naming
its topic, partition and offset, before any batch containing it is sent. An `Emit` over
`Start.max_message_bytes` is a gRPC-level failure that fails the stream like any other.

## Versioning

`protocol_version` is `MAJOR.MINOR`. The sidecar accepts a `Spec` whose major equals its own and
whose minor is not later than its own, and refuses otherwise naming both (FR-017). Adding an
optional field, a message, an rpc, a `ConfigType` value, a `format` value or a fixture is a minor.
Renaming, removing or re-meaning anything is a major.

## Rules the messages do not state (the `README.md`)

- One batch in flight per (inlet, partition); batches of one partition in offset order.
- Emits precede the ack. An emit after the ack fails the stream.
- Commit after the write. Offsets are committed only after every emit for the batch is confirmed.
- A `Fail` fails the stream and the sidecar redelivers from the last commit, indefinitely.
- A rebalance discards. An ack for a partition the sidecar no longer owns commits nothing.
- Skipping is acking without emitting.
- The sidecar never decodes a value. A contract is a format and a fingerprint.
- A keyless emit is partitioned by Kafka's default partitioner; per-key order is promised for keyed
  records only.
- A new `Start` voids everything: state tied to an old `conversation_id` must be discarded.

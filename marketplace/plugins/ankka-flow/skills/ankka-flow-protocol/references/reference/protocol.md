# Streamlet protocol

> The gRPC protocol between the sidecar and a streamlet's process — the Discovery and Streamlet services, the Run conversation, the rules the messages do not state, failure, rebalances, message limits and versioning.

Source: https://flow.ankka.cloud/reference/protocol/
The streamlet protocol, package `ankka.flow.v1`, is how the sidecar and a streamlet's process talk
inside one pod. The process is the gRPC server; the sidecar is the client. The protocol is versioned
on its own, `1.0` here, and carried in discovery.

Its authoritative copy is the
[`protocol/`](https://github.com/thinkmorestupidless/ankka-flow/blob/main/protocol) directory of the
repository: the `.proto` files, `DESCRIPTOR.md`, and the fixtures every SDK must reproduce. An SDK
copies the whole directory verbatim and generates its code from the copy.

## Addresses

| services | implemented by | dialled by | address |
|---|---|---|---|
| `Discovery`, `Streamlet` | the streamlet's process | the sidecar | `127.0.0.1:$FLOW_PROCESS_PORT`, 9010 by default |

The process binds the loopback interface only. The sidecar binds no gRPC port; port 9011 and
`FLOW_SIDECAR_PORT` are reserved for a callback service a later minor version may add. The sidecar
sends no HTTP/2 keepalive pings, so a process keeps its gRPC library's default ping policy: a server
that ends connections after too many pings is never provoked.

## Services

| Service | RPC | Request | Response | Defined in |
|---|---|---|---|---|
| `Discovery` | `Discover` | `SidecarInfo` | `Spec` | `discovery.proto` |
| `Discovery` | `ReportError` | `Problems` | `Empty` | `discovery.proto` |
| `Streamlet` | `Run` | `stream ToProcess` | `stream FromProcess` | `streamlet.proto` |
## Discovery

1. The sidecar calls `Discover(SidecarInfo)`, retrying with a backoff of 500 ms doubling to 10 s, until
   the process answers. It is not ready until then.
2. It checks `Spec.protocol_version` against its own. Another major version, or a later minor, is
   refused, naming both.
3. It validates the `Spec`'s descriptor and compares `Spec.streamlet` field by field with the
   descriptor it was deployed with. See [Descriptor](descriptor.md).
4. On any problem it calls `ReportError(Problems)` once with every problem, so they appear in the
   process's own log, logs each itself, and exits.
5. Otherwise it opens `Run`.

## The conversation

1. The sidecar opens `Run` and sends `Start`: a new `conversation_id`, the pipeline and streamlet
   names, `config_json` (every declared parameter, resolved, as `{"key": value}`), the inlet and outlet
   bindings to topics, and `max_message_bytes`. The process sends nothing before `Start`.
2. The sidecar sends `Batch`es. For each inlet partition it owns, at most one batch is in flight, and a
   partition's batches arrive in offset order. Batches of different partitions interleave freely.
3. The process answers each batch with zero or more `Emit`s, each naming a declared outlet and carrying
   a record, then exactly one `Ack` or `Fail` with the batch's `batch_id`. Messages for different
   batches may interleave.
4. The sidecar holds a batch's emits until its `Ack`. It then produces them to their outlets' topics
   with the keys and headers given, waits for the broker to confirm every one, and only then commits
   the batch's offsets. A produce failure fails the stream.
5. On shutdown the sidecar sends `Stop` and half-closes; the process finishes its batches and completes
   its side of the stream.

### Skipping a record

The process acknowledges the batch without emitting for that record. The sidecar never knows a record
was skipped; its offset is committed with the batch.

### Failing the stream

Any of these fails the stream:

- a `Fail`;
- an `Emit` naming an outlet not in `Start.outlets`;
- an `Emit`, `Ack` or `Fail` for a `batch_id` not in flight: never sent or already acknowledged;
- an `Emit` after its batch's `Ack`;
- a message the sidecar cannot parse, or an empty message;
- the process closing the stream or becoming unreachable;
- a produce failure.

The sidecar then fails every in-flight batch with nothing committed, removes its readiness file, closes
the conversation, backs off from 500 ms doubling to 30 s, repeats discovery in full, sends a new `Start`
with a new `conversation_id`, and resumes from the last committed offsets. It does this indefinitely
and never skips a record. A batch that fails every time stalls its partition; once stalled for
`FLOW_STALL_WARNING_AFTER`, the sidecar records a `PartitionStalled` warning event on its pod, once per
stall.

### Rebalances

When a partition is revoked while its batch is in flight, the batch is marked revoked: its emits and
`Ack` are discarded, nothing is committed for it, and the partition's new owner reads it again. An
`Emit` or `Ack` for a revoked batch is not a violation; the process could not have known, and the
message is dropped silently.

### Message limits

A batch is whatever arrived for its partition while the previous batch was with the process, capped by
the inlet's `batch.max-records` and `batch.max-bytes` (100 records and 1 MiB by default) so it stays
under the 4 MiB message limit. A quiet stream sends each record at once; nothing waits on a timer. One
input record larger than the limit fails the stream, naming its topic, partition and offset, before any
batch containing it is sent. An `Emit` must stay under `Start.max_message_bytes`; a larger one fails the
stream at the gRPC level.

## Rules

The messages cannot state these; every SDK and the sidecar keep them.

- **One batch in flight per inlet partition**, and a partition's batches in offset order.
- **Emits precede the ack.** An emit after its batch's ack fails the stream.
- **Commit after the write.** Offsets are committed only after every emit of the batch is confirmed by
  the broker.
- **A failure redelivers; nothing is skipped.** A `Fail` fails the stream, and the sidecar redelivers
  from the last commit, indefinitely.
- **Skipping is acknowledging without emitting.**
- **A rebalance discards.** An ack for a partition the sidecar no longer owns commits nothing.
- **The sidecar never decodes a value.** A contract is a format and a fingerprint.
- **A keyless emit is placed by Kafka's default partitioner.** Per-key order is promised for keyed
  records only; an emit without a key must leave `Record.key` unset, not empty.
- **A new `Start` voids everything.** State tied to an older `conversation_id` must be discarded.

## Versioning

`protocol_version` is `MAJOR.MINOR`. The sidecar accepts a `Spec` whose major equals its own and whose
minor is not later than its own, and refuses anything else, naming both versions. Adding an optional
field, a message, an rpc, a `ConfigType` value, a contract `format` or a fixture is a minor change.
Renaming, removing or changing the meaning of anything is a major change.

## `payload.proto`

```protobuf
syntax = "proto3";

package ankka.flow.v1;

// A Kafka record, exactly as Kafka holds it. The sidecar never inspects `value`.
message Record {
  optional bytes key = 1;        // absent: Kafka's default partitioner decides
  repeated Header headers = 2;   // order preserved
  bytes value = 3;
}

message Header {
  string key = 1;
  bytes value = 2;
}

// A process's failure of a batch, or the reason the sidecar gives for something.
message Error {
  string message = 1;
}

message Problem {
  string message = 1;
}

// Every problem the sidecar found in discovery, sent at once so they appear in the process's log.
message Problems {
  repeated Problem problems = 1;
}

message Empty {}
```

## `discovery.proto`

```protobuf
syntax = "proto3";

package ankka.flow.v1;

import "ankka/flow/v1/payload.proto";

// Implemented by the developer's process on 127.0.0.1:$FLOW_PROCESS_PORT; dialled by the sidecar.
service Discovery {
  // The process describes itself. The sidecar compares the answer with the deployed descriptor.
  rpc Discover (SidecarInfo) returns (Spec);
  // The sidecar's refusal, so the problems appear in the process's own log before it exits.
  rpc ReportError (Problems) returns (Empty);
}

message SidecarInfo {
  string protocol_version = 1;
  string sidecar_version = 2;
}

// The descriptor. An SDK writes this same message as canonical JSON to descriptor.json at build
// time (DESCRIPTOR.md).
message Spec {
  string protocol_version = 1;   // "MAJOR.MINOR", "1.0" here
  SdkInfo sdk = 2;
  StreamletDescriptor streamlet = 3;
}

message SdkInfo {
  string name = 1;
  string version = 2;
}

message StreamletDescriptor {
  string name = 1;                             // [a-z0-9-]{1,63}; what a blueprint refers to
  string description = 2;
  repeated Port inlets = 3;                    // port names unique across inlets and outlets
  repeated Port outlets = 4;
  repeated ConfigParameter config_parameters = 5;
}

message Port {
  string name = 1;
  Contract contract = 2;
}

message Contract {
  string format = 1;        // "json" is the only value in 1.0
  string schema_name = 2;   // e.g. "cart-events.v1"
  string fingerprint = 3;   // Base64(SHA-256(UTF-8(schema_name))), standard alphabet, padded
}

message ConfigParameter {
  string key = 1;             // [a-z][a-z0-9-]*
  string description = 2;
  ConfigType type = 3;
  string default_value = 4;   // absent: required at deploy time
}

enum ConfigType {
  STRING = 0;
  INTEGER = 1;
  DOUBLE = 2;
  BOOLEAN = 3;
  DURATION = 4;
  MEMORY_SIZE = 5;
}
```

## `streamlet.proto`

```protobuf
syntax = "proto3";

package ankka.flow.v1;

import "ankka/flow/v1/payload.proto";

// Implemented by the developer's process; dialled by the sidecar.
service Streamlet {
  // One conversation per streamlet instance. The sidecar opens it and speaks first.
  rpc Run (stream ToProcess) returns (stream FromProcess);
}

message ToProcess {
  oneof message {
    Start start = 1;
    Batch batch = 2;
    Stop stop = 3;
  }
}

message Start {
  string conversation_id = 1;          // new on every (re)connect: state tied to an old one is void
  string pipeline = 2;
  string streamlet = 3;
  string config_json = 4;              // {"key": value} for every declared parameter, resolved
  repeated PortBinding inlets = 5;
  repeated PortBinding outlets = 6;
  uint32 max_message_bytes = 7;        // an Emit must stay under this
}

message PortBinding {
  string port = 1;
  string topic = 2;
}

message Batch {
  uint64 batch_id = 1;                 // unique within the conversation, increasing
  string inlet = 2;
  int32 partition = 3;
  repeated InputRecord records = 4;    // in offset order
}

message InputRecord {
  int64 offset = 1;
  int64 timestamp_ms = 2;
  Record record = 3;
}

message Stop {
  string reason = 1;
}

message FromProcess {
  oneof message {
    Emit emit = 1;
    Ack ack = 2;
    Fail fail = 3;
  }
}

// Zero or more per batch, all before the batch's Ack.
message Emit {
  uint64 batch_id = 1;
  string outlet = 2;
  Record record = 3;
}

// Exactly one Ack or Fail per batch.
message Ack {
  uint64 batch_id = 1;
}

message Fail {
  uint64 batch_id = 1;
  Error error = 2;
}
```

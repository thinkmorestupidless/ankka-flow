---
name: ankka-flow-protocol
description: The streamlet protocol between the ankka-flow sidecar and a streamlet's process, and the descriptor file — the Discovery and Streamlet gRPC services in ankka.flow.v1, Discover and ReportError, the Run conversation (Start, Batch, Emit, Ack, Fail, Stop), ordering and concurrency rules, rebalances, message limits, protocol versioning, and the canonical JSON descriptor with its fingerprints. Use when implementing a streamlet in a language with no SDK, debugging what an SDK sends the sidecar, reading a conformance failure, or generating or validating a descriptor by hand.
---

# The streamlet protocol

The sidecar is the gRPC client; the streamlet's process is the server, on
`127.0.0.1:$FLOW_PROCESS_PORT` and nowhere else. The sidecar calls `Discovery.Discover` until the
process answers with its descriptor, compares it with the deployed one, then opens one bidirectional
`Streamlet.Run` stream that carries every batch for the life of the conversation.

## Rules

1. **Discovery first, and it must match.** `Discover` returns the same descriptor the build wrote.
   Any difference, an unsupported protocol major version, or an invalid descriptor refuses start-up;
   the sidecar sends every problem to `ReportError` first so the process can log it.
2. **`Start` before any batch.** The first message on `Run` is `Start` with `config_json`, the
   streamlet's resolved parameters. Apply it before handling a batch.
3. **Emits, then exactly one acknowledgement.** For each `Batch`, send any number of `Emit` messages,
   each naming a declared outlet, then one `Ack` for that batch — or one `Fail` instead. An emit after
   the ack, an emit to an undeclared outlet, or a second ack is a protocol violation that fails the
   stream: every in-flight batch is voided, the sidecar reconnects, repeats discovery and redelivers
   from the last commit.
4. **Never reorder within a partition; interleave across partitions.** At most one batch per inlet
   partition is in flight. Batches of different partitions may be processed concurrently and their
   messages interleaved; one batch's messages keep their order.
5. **A new `Run` voids the old one.** When the sidecar reconnects, everything tied to the previous
   conversation is discarded. On `Stop`, finish in-flight batches and complete the stream.
6. **Bytes only.** Records carry `key` (optional), ordered `headers`, and `value` as bytes. The
   protocol never interprets them.
7. **The descriptor is canonical JSON.** snake_case field names, sorted keys, ports sorted by name,
   defaults omitted, two-space indentation, one trailing newline. A JSON contract's fingerprint is
   Base64 of the SHA-256 of its schema name.
8. **Prove it with the fixtures and the conformance suite.** An SDK is compatible when its descriptors
   equal `protocol/fixtures/descriptors/*.json` byte for byte and it passes the conformance suite run
   against its port.

## Before implementing

- Is the whole `protocol/` directory copied verbatim, with code generated from the copy?
- Does the server bind only loopback, on `FLOW_PROCESS_PORT`?
- Is each batch's `Ack` sent only after all of its emits?
- What happens to in-flight work when a second `Run` arrives?

## Mistakes to check for

- Treating a skipped record as an error: skipping is an `Ack` with no emits for it.
- Sending `Ack` before all of the batch's emits: any later emit is a violation that fails the stream.
- Expecting emits to reach Kafka as they are sent: the sidecar holds a batch's emits until its `Ack`,
  and discards them on a `Fail`.
- A descriptor with unsorted keys, defaults written out, or a fingerprint computed from the schema's
  content instead of its name.
- Holding state per partition across conversations; the assignment changes on every rebalance.

## Reference files

Open the one a task needs; each is one topic and stands alone.

### Concepts

- `references/concepts/sidecar.md` — The container the platform runs beside every streamlet, which owns everything Kafka and drives the streamlet's process over a gRPC protocol on the pod's loopback interface.
- `references/concepts/delivery.md` — How the sidecar delivers records — commit only after the write, at least once, never skipping — and what happens when a batch fails, a partition stalls or a rebalance moves a partition.
- `references/concepts/contracts.md` — What a port's contract is — a format and a fingerprint — how two ports are matched by it before anything runs, and why the sidecar never decodes a record.

### Reference

- `references/reference/descriptor.md` — The descriptor file a streamlet's SDK writes from its declaration — its fields, the canonical JSON every SDK produces byte for byte, contract fingerprints, and the validation rules.
- `references/reference/protocol.md` — The gRPC protocol between the sidecar and a streamlet's process — the Discovery and Streamlet services, the Run conversation, the rules the messages do not state, failure, rebalances, message limits and versioning.
- `references/reference/python-sdk.md` — Every public name of the ankka-flow Python package — Streamlet, ports, parameters and their Python types, records, serve, the testkit Harness — and the descriptor and conformance commands.

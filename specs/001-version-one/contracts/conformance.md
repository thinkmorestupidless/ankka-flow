# Contract: the conformance suite

**Feature**: [spec.md](../spec.md) | **Protocol**: [protocol.md](./protocol.md) | **Research**: R16

Two things define a compatible SDK (FR-026, FR-027, S5): the **conformance suite**
(`sidecar/src/test`, a munit suite) that drives a process through every conversation the protocol
defines and asserts on what it sends, and the **descriptor fixtures** each SDK runs in its own test
runner (contracts/descriptor.md). The suite needs no Kafka: it stands in for the sidecar's
conversation side with the same `RemoteProcessor` the sidecar uses, fed by scripted batches.

## Targets

```bash
sbt 'sidecar/testOnly *ConformanceSuite'                                              # the Scala reference, in-process
sbt 'sidecar/testOnly *ConformanceSuite' -Dflow.conformance.target=127.0.0.1:9010     # a process listening there
cd sdks/python && uv run conformance                                                   # starts the Python reference, then the line above
```

`ConformanceTarget.InProcess` starts `ConformanceReference` (a Scala implementation of the
reference streamlet speaking the protocol over grpc-java on an ephemeral loopback port) and
`Sidecar(address)` dials a process already listening. The suite prints `conformance target: …`.

## The reference streamlet

Every SDK ships one, named `conformance`, with this declaration (also fixture `conformance`):

| port | contract | role |
|---|---|---|
| inlet `in` | `json` `conformance.v1` | the stream under test |
| inlet `side` | `json` `conformance-side.v1` | a second inlet, to prove interleaving |
| outlet `out` | `json` `conformance.v1` | normal output |
| outlet `other` | `json` `conformance-other.v1` | fan-out |

Parameters: `mode` (STRING, default `echo`), `factor` (INTEGER, default `1`). Behaviour is
driven by the input record's **key**, so the suite can script it without the reference decoding
values: key `echo` → emit the record to `out`; `fan` → emit to `out` and `other`; `skip` → emit
nothing; `fail` → raise; `late` → sleep 200 ms then emit; `rogue-outlet` → emit to an undeclared
outlet `nope`; `double-ack` → (reference SDKs cannot express this; the suite sends it only to the
Scala double); `multiply` → emit the record `factor` times with headers `n=<i>`; `unkeyed` → emit
with no key; `header-echo` → emit with the input's headers reversed. The value is passed through
unchanged in every case.

## Conversations, by name

| case | what the suite does | passes when |
|---|---|---|
| `discovery.answers-spec` | `Discover` | `Spec.streamlet` equals fixture `conformance`; `protocol_version` major is 1 |
| `discovery.reports-error` | `ReportError` with two problems | rpc succeeds; the process's log (or `GET` on its problems hook, if it has one) is not asserted |
| `run.start-then-silence` | `Start`, nothing else, 500 ms | the process sends nothing |
| `run.echo-preserves-record` | one batch, key `echo`, headers with a binary value | one `Emit` to `out` with identical key, headers in order, value bytes; then `Ack` |
| `run.fan-out` | key `fan` | two emits, `out` then `other`, then `Ack` |
| `run.skip-acks-without-emit` | key `skip` | `Ack` only |
| `run.fail-sends-fail` | key `fail` | `Fail` with a non-empty message, no emits |
| `run.emits-precede-ack` | key `multiply`, factor 5 | five emits then `Ack`; nothing after |
| `run.two-partitions-interleave` | batches on partitions 0 and 1, `late` on 0, `echo` on 1 | partition 1's emit and ack arrive before partition 0's |
| `run.two-inlets` | a batch on `in` and one on `side` | each acked with its own `batch_id` |
| `run.batch-order-within-partition` | three sequential batches on one partition | acked in order; the next is sent only after the previous ack (the suite enforces this, the process just must not reorder) |
| `run.config-applied` | `Start` with `factor = 3`, key `multiply` | three emits |
| `run.unkeyed-emit` | key `unkeyed` | `Emit.record.key` unset |
| `run.header-order` | key `header-echo` with four headers | reversed order, bytes intact |
| `run.large-record` | a 3 MiB value, key `echo` | echoed intact |
| `run.stop-completes` | `Stop` then half-close | the process completes its stream within 2 s |
| `run.reconnect-new-conversation` | close; open a new `Run` with a new id; key `echo` | works as fresh; nothing from the old conversation leaks |
| `run.rogue-outlet` | key `rogue-outlet` | the stream fails: either the SDK refuses the undeclared outlet and fails the batch, or it emits and the sidecar refuses |
| `violation.double-ack` | Scala double only | the suite's `RemoteProcessor` fails the stream |
| `violation.emit-after-ack` | Scala double only | ditto |
| `violation.unknown-batch` | Scala double only | ditto |
| `version.refuses-other-major` | `Discover` answered with `2.0` (double only) | refused naming both |
| `version.accepts-earlier-minor` | `Spec` says `1.0` to a `1.1` sidecar (double only) | accepted |

`violation.*` and `version.*` cases run against the scriptable `ProcessDouble` in the same JVM,
not against a target, because a correct SDK cannot produce them; they prove the *sidecar's* side
of the protocol. They print as `skipped (double-only)` for an external target.

Each case is one munit test named as above (`-Dflow.conformance.only=run.fan-out` runs one), and a
failing SDK gets the exact conversation by name (S5.1).

**Proving SC-005.** A deliberately broken rule must fail exactly the conversation that covers it.
The rule used is "a keyless emit carries no key": with `ANKKA_FLOW_BREAK=keyless-empty-key` the
Python SDK sends an empty key instead, and exactly `run.unkeyed-emit` fails. Acknowledging before
emitting (`ANKKA_FLOW_BREAK=ack-first`), the example the spec gives, cannot fail only one case:
every case that emits then sees its emits after the ack, which the sidecar refuses. It fails all
twelve emitting cases, `run.emits-precede-ack` among them, and passes the six that emit nothing.

## Fixtures the suite replays

`protocol/fixtures/conversations/*.json` hold the scripted inputs for each `run.*` case (batches
as base64 records) so that a third SDK's author can read exactly what will be sent. The suite
loads them; they are not hand-copied into test code.

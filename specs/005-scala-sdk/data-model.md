# Data Model: A Scala SDK

The entities a streamlet author and the build handle. Each is a glossary term; the Scala names are
in `contracts/scala-sdk-api.md`.

| Entity | What it is | Fields | Rules |
|---|---|---|---|
| streamlet | one stage of a pipeline, declared in Scala as a `Streamlet` subclass | name, description, inlets, outlets, parameters, `process` | name 1–63 of `[a-z0-9-]`, not starting or ending with `-`; constructed with no arguments; a port name or parameter key declared twice, or a name the descriptor rules refuse, fails construction with the rule's message |
| inlet / outlet | a port of a streamlet with a JSON contract | name, schema name, fingerprint (Base64 of SHA-256 of the schema name) | name `[a-z][a-z0-9-]{0,62}`; schema name non-empty; the graph-delta outlet is an outlet whose schema name is `ankka.graph-delta.v1` and whose emits are validated deltas |
| parameter | a typed setting of a streamlet | key, type (string, integer, double, boolean, duration, memory-size), default (rendered as the start carries it), description | key `[a-z][a-z0-9-]*`; a default of another type is refused at construction; a value is read as the declared type, falling back to the default |
| record | what a topic holds | value bytes, optional key bytes, ordered headers, offset, timestamp | nothing decoded; an emit built from an inlet record keeps key, headers and value unless replaced |
| batch | one call of `process` | inlet name, partition, records in offset order | at most one in flight per `(inlet, partition)`; different partitions concurrently |
| emit | one record to one outlet | outlet name, record | the outlet is declared, or the batch fails; derived from an inlet record or built fresh |
| descriptor | the discovery `Spec` as canonical JSON | protocol version, sdk `{name, version}`, streamlet `{name, description, inlets, outlets, config_parameters}` | written by `DescriptorJson.write`; ports sorted by name, parameters by key; the `streamlet` part is what the blueprint is verified against and what the sidecar compares with discovery; the `sdk` part names the SDK that wrote it |
| harness | the in-memory stand-in for Kafka and the sidecar | streamlet, config, inlets' pending records, outlets' records, failures, skipped, batches | records per inlet are placed on partitions by the test's function, batched in offset order by the test's size; a batch's emits are recorded only when it succeeds; a record with no emit derived from it is skipped |
| conversation | one `Run` stream between the sidecar and the process | config, in-flight batches, ended | starts on `Start`; a new `Run` ends the previous, whose batches finish silently; `Stop` waits for in-flight batches and completes |
| artifact | the SDK on Maven Central | `com.thinkmorestupidless:ankka-flow-sdk_3:<version>`, depending on `ankka-flow-protocol_3:<version>` | published once per release tag, for Scala 3.3 LTS on Java 21; never for a pre-release |
| sample | the Scala cart router | `samples/cart-router-scala`: the streamlet, two tests, `flow/descriptor.json`, a Dockerfile-less image from the build, a README | the `streamlet` part of its descriptor equals the Python sample's; it runs the Python sample's blueprint and compose file |

## State transitions

A streamlet process: **constructed** (declaration validated) → **serving** (discovery answered) →
**started** (a `Run` received `Start`; config applied) → **processing** (batches in flight) →
**stopped** (`Stop` received, in-flight batches finished, stream completed), or **superseded** (a
new `Run` arrived; the old conversation sends nothing more).

A batch in the harness or the server: **received** → **processed** (emits collected) → **acked**
(emits recorded or sent, then the ack) or **failed** (emits discarded, the failure recorded or
sent with the exception's message).

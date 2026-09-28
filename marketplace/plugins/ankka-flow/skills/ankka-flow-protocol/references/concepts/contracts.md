# Contracts

> What a port's contract is — a format and a fingerprint — how two ports are matched by it before anything runs, and why the sidecar never decodes a record.

Source: https://flow.ankka.cloud/concepts/contracts/
Every port declares a **contract**: a format and a fingerprint. Two ports on one topic connect when
their formats and fingerprints are equal, and never otherwise. The check happens when the blueprint is
verified, before anything runs, and needs no language runtime: it compares the contracts written in
the streamlets' descriptors.

## JSON, by schema name

The only format is `json`. A JSON contract names a schema, such as `cart-events.v1`. Its fingerprint
is the Base64 of the SHA-256 of that name, so two ports connect exactly when they name the same
schema.

```python
inlet = JsonInlet("in", schema_name="cart-events.v1")
valid = JsonOutlet("valid", schema_name="cart-events.v1")
```

The schema name is a promise between the streamlet that writes a topic and the streamlets that read
it, not a file the platform reads: nothing checks a record against a schema. A new version of a
contract is a new name, `cart-events.v2`, which no longer connects to readers of `cart-events.v1`
until they move to it. That is the only form of schema evolution.

`flow verify` refuses a topic whose ports disagree, naming both ports and both contracts:

```text
'router.valid' (json cart-events.v1) is not compatible with 'sink.in' (json cart-events.v2).
```

## The sidecar never decodes

Records cross the protocol as bytes, exactly as Kafka holds them. The sidecar never reads a value;
a contract is a format and a fingerprint to compare, never a type it understands. Decoding is the
streamlet's job, which is why one sidecar serves every language.

A record the streamlet cannot decode is the streamlet's decision: skip it by acknowledging the batch
without emitting for it, or fail the batch. Failing it redelivers the batch indefinitely and stalls the
partition until the code changes; [Delivery and failure](delivery.md) explains why.

## What contracts do not do

Avro and Protobuf contracts, schema registries, and compatibility rules between schema versions do
not exist. A descriptor carries a `format` field for every contract so that other formats can be added
without changing its shape; today any value but `json` is refused.

# Contracts

Every port declares a **contract**: a format and a fingerprint. Two ports connect when their
format and fingerprint are equal, and never otherwise. The check happens when the blueprint is
verified, before anything runs, and needs no language runtime.

## JSON, by schema name

The only format in this version is `json`. A JSON contract names a schema, such as
`cart-events.v1`. Its fingerprint is the Base64 of the SHA-256 of that name.

Two ports connect when they name the same schema. A new version of a contract is a new name,
`cart-events.v2`, which no longer connects to readers of `v1` until they move to it.

```python
inlet  = JsonInlet("in",    schema_name="cart-events.v1")
valid  = JsonOutlet("valid", schema_name="cart-events.v1")
```

## The sidecar never decodes

Records cross the protocol as bytes, exactly as Kafka holds them. The sidecar never reads a value.
Decoding is your code's job, which is why one sidecar serves every language. A record your code
cannot decode is your decision: skip it by acknowledging without emitting, or fail the batch.

## What is not here yet

Avro and Protobuf contracts, schema registries, and schema evolution beyond "a new contract is a new
name" are later features. The descriptor carries a `format` field so they can be added without
changing its shape.

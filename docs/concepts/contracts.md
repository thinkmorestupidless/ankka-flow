---
title: Contracts
description: What a port's contract is — a name and, optionally, a schema's fingerprint, as ankka names one — and how two ports are matched by it before anything runs.
kind: concept
languages: [scala, java]
related: [concepts/pipelines.md, reference/descriptor.md, reference/blueprint.md, build/ankka-topics.md]
---

# Contracts

Every port declares a **contract**: what the records on it are, by name, as an ankka project names
the contract of a topic. Two ports on one topic connect when their contracts agree, and never
otherwise. The check happens when the blueprint is verified, before anything runs, and needs no
engine: it compares the contracts written in the streamlets' descriptors.

## A name, and a schema's fingerprint

A contract is a name, such as `cart-events.v1`. It may also name a schema, a file on the
streamlet's classpath, usually a JSON Schema; the descriptor then carries the schema's
**fingerprint**: `sha256:` and the hex SHA-256 of the schema in RFC 8785 canonical form, as ankka
computes it for a topic it declares.

/// tab | Scala

```scala
val in  = inlet("in", Contract("cart-events.v1", schema = "cart-events.v1.json"), Codec.bytes)
val out = outlet("out", Contract("clean-carts.v1"), Codec.json[CleanCart])
```

///

/// tab | Java

```java
final Inlet<CleanCart> in = inlet("in", Contract.of("clean-carts.v1"), Codec.json(CleanCart.class));
final Outlet<CartCount> out = outlet("out", Contract.of("cart-counts.v1"), Codec.json(CartCount.class));
```

///

Two contracts agree when their names are equal and, where both state a fingerprint, the
fingerprints are equal too. A port that states no schema connects to any port of the same name.
The streamlets on either side of a topic may run on different engines; the contract is all they
share.

The contract is a promise between the streamlet that writes a topic and those that read it.
Nothing checks a record against its schema as it passes; the codec a port declares decodes it, and
a record it cannot decode is the streamlet's to drop or fail on. A new version of a contract is a
new name, `cart-events.v2`, which does not connect to readers of `cart-events.v1` until they move
to it.

`flow verify` refuses a topic whose ports disagree, naming both ports and both contracts:

```text
'cleaner.out' (contract clean-carts.v1) is not compatible with 'counter.in' (contract clean-carts.v2).
```

## ankka's topics

A topic an ankka project declares has a contract of its own, named and fingerprinted the same way.
`flow verify --ankka-topics` checks every port on such a topic against the project's declaration;
[Read and write ankka topics](../build/ankka-topics.md) shows how.

## What contracts do not do

Schema registries, Avro and Protobuf framing, and compatibility rules between versions of a
schema are not part of a contract.

---
title: Write a blueprint
description: Write the blueprint that wires a pipeline's streamlets together over Kafka topics, from naming the streamlets to checking the result with flow verify.
kind: guide
related: [reference/blueprint.md, concepts/topics.md, concepts/contracts.md, reference/cli.md]
---

# Write a blueprint

A blueprint says which streamlets a pipeline runs and which topic connects which ports. It is checked
against the streamlets' descriptors, so write it once every streamlet's descriptor exists: each SDK
writes one with its `--descriptor <file>` mode. A blueprint never names an engine: each streamlet's
descriptor does, so one blueprint wires streamlets of different engines.

This guide builds the blueprint of the sample pipeline: a Pekko Streams streamlet cleaning the cart
events another system publishes, and a Flink streamlet counting each cart's clean events in windows
of time.

## Start with the descriptors

Collect every streamlet's descriptor into one directory, the sample's `flow/`. Each declares the
streamlet's name, its engine, its ports and each port's contract:

| Descriptor | Engine | Port | Contract |
|---|---|---|---|
| `cart-cleaner` | `pekko-streams` | inlet `in` | `cart-events.v1` |
| | | outlet `out` | `clean-carts.v1` |
| `cart-counter` | `flink` | inlet `in` | `clean-carts.v1` |
| | | outlet `out` | `cart-counts.v1` |

## Name the pipeline and its streamlets

```hocon
blueprint {
  name = carts
  streamlets {
    cleaner = cart-cleaner
    counter = cart-counter
  }
}
```

`name` is the pipeline id. It prefixes the Kafka names of the topics the pipeline creates, and its
consumer groups, so choose it once. Each entry under `streamlets` gives a streamlet a name in this
pipeline (`cleaner`) and says which descriptor it runs (`cart-cleaner`). Ports are addressed through
the pipeline's name for the streamlet: `cleaner.in`, `cleaner.out`, `counter.in`, `counter.out`.

## Connect the input

The cart events are published by something else, so the topic is unmanaged: the pipeline reads it and
never creates, alters, deletes or writes to it. An unmanaged topic says what it is called in Kafka and
where it lives:

```hocon
cart-events {
  managed    = false
  topic.name = "shop.cart-events.v1"
  cluster    = shop
  consumers  = [cleaner.in]
  consumer-config { auto.offset.reset = earliest }
}
```

`cluster = shop` names a Kafka cluster Secret installed beside the operator; it could instead be
`bootstrap.servers`, and either can be changed at deploy time without editing the blueprint.

## Connect the streamlets

The topics between the streamlets, and after the last, belong to the pipeline, so they are managed,
the default. The operator creates each one with the partitions and replication it declares, and its
Kafka name is `<pipeline>.<topic id>`: `carts.clean-carts` and `carts.cart-counts`.

```hocon
clean-carts {
  producers  = [cleaner.out]
  consumers  = [counter.in]
  partitions = 3
}
```

Every port on a topic must carry the same contract, whatever the engines of their streamlets. Between
managed topics a Flink streamlet writes each result once; see
[Delivery and failure](../concepts/delivery.md).

## The whole blueprint

<!-- include: samples/cart-pipeline/blueprint.conf -->
```hocon
# The sample pipeline: the shop's cart events, cleaned, then counted per cart in windows of time.
blueprint {
  name = carts
  streamlets {
    cleaner = cart-cleaner
    counter = cart-counter
  }
  topics {
    cart-events {
      managed    = false
      topic.name = "shop.cart-events.v1"
      cluster    = shop
      consumers  = [cleaner.in]
      consumer-config { auto.offset.reset = earliest }
    }
    clean-carts {
      producers  = [cleaner.out]
      consumers  = [counter.in]
      partitions = 3
    }
    cart-counts {
      producers  = [counter.out]
      partitions = 3
    }
  }
}
```

## Verify it

```bash
flow verify blueprint.conf --descriptors flow
```

`flow verify` needs no image, no network and no language runtime. It checks every streamlet, port,
contract, topic and parameter, and prints every problem it finds in one pass, one per line, exiting
with `1`; a blueprint that verifies prints a summary and exits with `0`:

```text
verified: 2 streamlets, 3 topics
```

The problems it most often finds while a blueprint is being written:

- a port path that does not exist, with the ports the streamlet does have as a suggestion;
- an inlet left unconnected;
- two ports on one topic with different contracts, whatever their engines;
- an unmanaged topic listed with producers, or with neither `bootstrap.servers` nor `cluster`.

An outlet left unconnected is allowed and printed as a note. The full list of rules is on
[Blueprint](../reference/blueprint.md). Once it verifies, the blueprint is ready for
`flow generate`, which [Deploy a pipeline](../deploy/deploy-a-pipeline.md) describes.

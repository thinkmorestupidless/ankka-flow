# Write a blueprint

> Write the blueprint that wires a pipeline's streamlets together over Kafka topics, from naming the streamlets to checking the result with flow verify.

Source: https://flow.ankka.cloud/build/blueprints/
A blueprint says which streamlets a pipeline runs and which topic connects which ports. It is checked
against the streamlets' descriptors, so write it once every streamlet's descriptor exists: each SDK
writes one when the streamlet is built, and for Python that is `uv run descriptor`, which writes
`flow/descriptor.json`.

This guide builds the blueprint of the cart router sample: one streamlet reading cart events that
another system publishes, and routing each event to one of two outlets.

## Start with the descriptors

Collect every streamlet's descriptor into one directory. Each descriptor declares the streamlet's
name, its ports and each port's contract; the cart router's declares:

| Port | Kind | Contract |
|---|---|---|
| `in` | inlet | `json cart-events.v1` |
| `valid` | outlet | `json cart-events.v1` |
| `review` | outlet | `json cart-events.v1` |

The blueprint refers to streamlets by the `name` in their descriptor, here `cart-router`.

## Name the pipeline and its streamlets

```hocon
blueprint {
  name = cart
  streamlets {
    router = cart-router
  }
}
```

`name` is the pipeline id. It prefixes the Kafka names of the topics the pipeline creates, and its
consumer groups, so choose it once. Each entry under `streamlets` gives a streamlet a name in this
pipeline (`router`) and says which descriptor it runs (`cart-router`). Ports are addressed through the
pipeline's name for the streamlet: `router.in`, `router.valid`, `router.review`.

## Connect the input

The cart events are published by something else, so the topic is unmanaged: the pipeline reads it and
never creates, alters, deletes or writes to it. An unmanaged topic says what it is called in Kafka and
where it lives:

```hocon
cart-events {
  managed           = false
  topic.name        = "shop.cart-events.v1"
  bootstrap.servers = "kafka:9092"
  consumers         = [router.in]
  consumer-config { auto.offset.reset = earliest }
}
```

`bootstrap.servers` could instead be `cluster = shop`, naming a Kafka cluster Secret installed beside
the operator; the choice can also be changed at deploy time without editing the blueprint.

## Connect the outputs

The outlets' topics belong to the pipeline, so they are managed, the default. The operator creates
each one with the partitions and replication it declares, and its Kafka name is
`<pipeline>.<topic id>`: `cart.valid-carts` and `cart.review-carts`.

```hocon
valid-carts {
  producers  = [router.valid]
  partitions = 3
  replicas   = 1
}
```

A downstream streamlet would read `valid-carts` by listing its inlet under `consumers`. Every inlet on
the topic must carry the same contract as every outlet on it.

## The whole blueprint

```hocon
blueprint {
  name = cart
  streamlets {
    router = cart-router
  }
  topics {
    # Published by something else (an ankka service in a cluster, produce.py on a laptop):
    # the platform consumes it and never creates, alters or deletes it.
    cart-events {
      managed           = false
      topic.name        = "shop.cart-events.v1"
      bootstrap.servers = "kafka:9092"
      consumers         = [router.in]
      consumer-config { auto.offset.reset = earliest }
    }
    valid-carts {
      producers  = [router.valid]
      partitions = 3
      replicas   = 1
    }
    review-carts {
      producers  = [router.review]
      partitions = 3
      replicas   = 1
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
verified: 1 streamlets, 3 topics
```

The problems it most often finds while a blueprint is being written:

- a port path that does not exist, with the ports the streamlet does have as a suggestion;
- an inlet left unconnected;
- two ports on one topic with different contracts;
- an unmanaged topic listed with producers, or with neither `bootstrap.servers` nor `cluster`.

An outlet left unconnected is allowed and printed as a note. The full list of rules is on
[Blueprint](../reference/blueprint.md). Once it verifies, the blueprint is ready for
`flow generate`, which [Deploy a pipeline](../deploy/deploy-a-pipeline.md) describes.

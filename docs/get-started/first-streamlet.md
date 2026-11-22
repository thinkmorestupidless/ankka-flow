---
title: Your first streamlet
description: Run the sample cart pipeline on a laptop — a Pekko Streams streamlet that cleans cart events and a Flink streamlet that counts them — against Kafka in a container, with flow local writing each streamlet's settings.
kind: tutorial
related: [build/pekko-streams-streamlet.md, build/flink-streamlet.md, build/testing.md, concepts/engines.md, get-started/deploy-locally.md]
---

# Your first streamlet

The sample in
[`samples/cart-pipeline`](https://github.com/thinkmorestupidless/ankka-flow/tree/main/samples/cart-pipeline)
is a pipeline of two streamlets, one for each engine. This tutorial tests them, verifies their
blueprint, and runs both on a laptop against Kafka in a container, with no cluster.

You need `flow` on your `PATH`, Docker, a JDK 21 and sbt, and a clone of the repository;
[Install the tools](install.md) lists them. Commands run from the repository's root.

## The pipeline

| Streamlet | Engine | What it does |
|---|---|---|
| `cleaner` | Pekko Streams, in Scala | reads the shop's cart events from `shop.cart-events.v1`, drops each that does not decode or whose total is above its parameter `max-total`, and writes the rest, keyed by their cart, to `clean-carts` |
| `counter` | Flink, in Java | reads `clean-carts` and counts each cart's events in tumbling windows of the time each event carries, one minute unless its parameter `window` says otherwise, writing the counts to `cart-counts` |

The blueprint wires them over three topics. `cart-events` is the shop's topic, which the pipeline
reads and never creates; `clean-carts` and `cart-counts` are the pipeline's own:

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

| Path | What it is |
|---|---|
| `cleaner/` | the cleaner: its declaration, its main and its test |
| `counter/` | the counter: its job, its state type, its test and the `Dockerfile` of its image |
| `flow/cleaner.json`, `flow/counter.json` | each streamlet's descriptor, committed |
| `schemas/` | the JSON schema of the cart events the cleaner reads |
| `produce.sh`, `verify.sh` | send fifty cart events, and check what the pipeline wrote for them |
| `k8s/` | the Secrets a cluster needs for this pipeline |
| `ankka/` | the same pipeline over an ankka project's topics |

## Test both streamlets

```bash
sbt cartCleaner/test cartCounter/test
```

Neither test needs Kafka. The cleaner's runs its stage over records in memory with the Pekko Streams
SDK's harness; the counter's runs its job on a local Flink with the Flink testkit's harness.
[Test a streamlet](../build/testing.md) covers both.

## Check the descriptors and verify the blueprint

```bash
sbt cartCleaner/descriptorCheck cartCounter/descriptorCheck
flow verify samples/cart-pipeline/blueprint.conf --descriptors samples/cart-pipeline/flow
```

A descriptor is what `flow` knows about a streamlet: its engine, its ports with their contracts, and
its parameters. Each check fails when the committed file differs from what the streamlet declares;
`sbt cartCleaner/descriptor` and `sbt cartCounter/descriptor` rewrite them. `flow verify` checks the
blueprint against the descriptors, the contracts on each topic among them, and prints
`verified: 2 streamlets, 3 topics`.

## Start Kafka

```bash
docker compose up -d kafka
```

This is a single-node Kafka on `localhost:9092` that creates a topic the first time it is used.

## Write the streamlets' settings

On a cluster the operator gives each streamlet a file of settings: its topics, its consumer groups,
its parameters and how to reach Kafka. On a laptop, `flow local` writes the same files:

```bash
flow local samples/cart-pipeline/blueprint.conf --descriptors samples/cart-pipeline/flow \
  --bootstrap-servers localhost:9092 -o /tmp/carts
```

It verifies the blueprint as `flow verify` does, then writes `/tmp/carts/cleaner/streamlet.properties`
and `/tmp/carts/counter/streamlet.properties` and prints each path. Every topic is on the one broker
`--bootstrap-servers` names, whatever cluster the blueprint gives it, and has the Kafka name it would
have on a cluster: the pipeline's own topics are `carts.clean-carts` and `carts.cart-counts`. Every
outlet writes at least once. `flow local` touches no broker and no cluster.

## Run the streamlets

Each streamlet is a command on the host, reading its settings from the directory `FLOW_CONFIG_DIR`
names. In one terminal:

```bash
FLOW_CONFIG_DIR=/tmp/carts/cleaner sbt cartCleaner/run
```

And in another:

```bash
FLOW_CONFIG_DIR=/tmp/carts/counter sbt cartCounter/run
```

The cleaner runs as it would in a pod. The counter runs the same main as in its image, which finds
`local=true` in its settings and runs the job on Flink's local environment inside the process. It
logs once that its state is kept only while that process runs.

## Send events and check the results

In a third terminal:

```bash
samples/cart-pipeline/produce.sh localhost:9092 50
samples/cart-pipeline/verify.sh localhost:9092 50
```

`produce.sh` writes fifty events for ten carts, one event per cart in each minute, and makes every
tenth one something that is not a cart event. `verify.sh` reads both of the pipeline's topics and
exits 0 when:

- `carts.clean-carts` holds the forty-five cart events, each keyed by its cart, and none of the
  other five;
- `carts.cart-counts` holds one count for each cart and each minute that has closed.

A window closes once the counter has read an event ten seconds past its end, so the last minute stays
open, and its counts are written only when later events arrive.

## Stop and start the counter

Stop the counter with Ctrl-C and start it again with the same command. It starts with no state: the
windows that were open are gone. It reads `carts.clean-carts` from the offsets its consumer group
committed at its last checkpoint, which it takes in memory every ten seconds while it runs, so what
it read before then is not read again, and a window that was open when it stopped is counted only
from there. On a cluster the same streamlet keeps its state in checkpoints and carries open
windows over a restart; [Deploy to a local cluster](deploy-locally.md) runs it there.

## Clean up

Stop both streamlets, then:

```bash
docker compose down
```

## Where to go from here

- [Choosing an engine](../concepts/engines.md) says which stages are Pekko Streams streamlets and
  which are Flink streamlets.
- [Write a Pekko Streams streamlet](../build/pekko-streams-streamlet.md) and
  [Write a Flink streamlet](../build/flink-streamlet.md) go through each of the sample's streamlets.
- [Deploy to a local cluster](deploy-locally.md) runs the same pipeline on kind.

---
title: Choosing an engine
description: Which need selects a Flink streamlet, which a Pekko Streams streamlet, and which an ankka consumer instead of a pipeline, and what each promises about delivery.
kind: concept
related: [concepts/pipelines.md, concepts/delivery.md, build/flink-streamlet.md, build/pekko-streams-streamlet.md]
---

# Choosing an engine

A streamlet is written against one engine's own API and run by that engine. The contracts, the
wiring and the deploy are the same for every engine, so one blueprint can wire streamlets of
different engines over one topic. The choice is made per stage, from what the stage needs.

| The stage needs | Write it as | What it promises |
|---|---|---|
| windows, state kept across records, records that arrive late, joins across streams | a Flink streamlet | each record counted once between topics the pipeline manages; at least once into a topic it does not |
| neither windows nor state: filter, clean, reshape, route, enrich by a call | a Pekko Streams streamlet | at least once: a record is committed only after everything it produced is written |
| one service reading one topic and acting on each message for one key | an ankka consumer, not a pipeline | what ankka's consumer promises |

## A Flink streamlet

Choose Flink when the answer depends on more than one record: a count per minute, a session that
ends after a quiet spell, a running total, the join of two streams. Flink keeps that state in
checkpoints in a state store, restores it when a pod fails, and carries it over to a new image
whose state types changed in the ways Flink allows. Between topics the pipeline manages, its
outlets write in Kafka transactions tied to those checkpoints, so a record is counted once and a
reader downstream sees a window once, one checkpoint interval after it closes.

## A Pekko Streams streamlet

Choose Pekko Streams when each record is handled on its own: dropped, cleaned, reshaped, routed to
one of several outlets, or enriched by a call to another service. It starts in seconds and runs as
a plain Deployment with as many replicas as the load needs. It has no windows and no state beyond
the record in hand; a stage that needs either is a Flink streamlet. A record whose results are not
all written is read again, so a stage downstream may see a result more than once.

## An ankka consumer

A single service reacting to the messages of one topic, each for one key, keeping its state in an
entity, is an ankka consumer and should stay one. A pipeline earns its place when a design needs
several stages whose contracts are checked against each other before anything runs, topics the
platform creates and owns, lag reported for each stage, or computation over sets and time.

See [Delivery](delivery.md) for what each engine does when something fails, and
[Limitations](../reference/limitations.md) for what neither does.

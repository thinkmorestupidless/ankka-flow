---
title: Adding an engine
description: What running streamlets on another engine takes — an engine id in the descriptor, an SDK held to the descriptor fixtures, and a rendering in the operator — and what proves each.
kind: contributing
related: [concepts/engines.md, reference/descriptor.md, reference/streamlet-settings.md, reference/operator.md, contributing/building.md]
---

# Adding an engine

A streamlet is written against one engine's own API and run by that engine. ankka-flow runs two,
Pekko Streams and Apache Flink, and what is shared between them is small: the descriptor that says
what a streamlet declares, the blueprint that wires streamlets over topics, the settings file each
streamlet is given, and the `AnkkaFlow` resource. An engine is added in three places: an id in the
descriptor, an SDK whose descriptors equal the fixtures' bytes, and a rendering in the operator. The
[Pekko Streams SDK](https://github.com/thinkmorestupidless/ankka-flow/tree/main/sdks/pekko-streams)
and the [Flink SDK](https://github.com/thinkmorestupidless/ankka-flow/tree/main/sdks/flink) are the
worked examples, one in Scala and one in Java.

## An engine id in the descriptor

The descriptor names its engine in `streamlet.engine`, and `flow` and the operator refuse an engine
they do not know. The ids are the `Engine` enum in the `descriptor` module
(`descriptor/src/main/scala/…/descriptor/Descriptor.scala`): `pekko-streams` and `flink`. A new engine
adds its id there, and with it every refusal that lists the known engines lists the new one.

An engine that needs to know something about a streamlet before it runs it carries that in a section
of the descriptor named for the engine, as a Flink streamlet's `flink` section carries its entry class
and Flink's version. `DescriptorValidation` requires such a section on the engine's streamlets and
refuses it on any other. Everything else in a descriptor — names, ports, contracts, parameters — is
the same for every engine; [Descriptor](../reference/descriptor.md) defines it.

## Descriptor fixtures

`descriptor/fixtures/declarations/*.md` describe streamlet declarations in prose, each naming the
engine it is written for, and `descriptor/fixtures/descriptors/<name>.json` holds the exact bytes
each must produce. A new engine adds declarations of its own, at least one with every kind of port and
parameter its SDK can declare, and their descriptors.

The `descriptor` module's `DescriptorFixturesSuite` writes every fixture from the model and compares
the bytes. For a deliberate change to the canonical form, it rewrites the files instead:

```bash
sbt -Dflow.fixtures.regenerate=on 'descriptor/testOnly *DescriptorFixturesSuite'
```

## An SDK held to the fixtures

An SDK is a library in the engine's own language. It has its own API for the engine's logic, and
these, which every SDK has:

1. **Declaration.** A streamlet declares its name, its inlets and outlets with their contracts and
   codecs, and its typed parameters, explicitly. The SDK never discovers streamlets by scanning.
2. **The descriptor writer.** The declaration as canonical JSON, with the engine's id. An SDK in a
   JVM language may use the `descriptor` module's writer, as the Pekko Streams SDK does; one that
   cannot carry Scala writes the bytes itself, as the Flink SDK does. A contract with a schema is
   fingerprinted as ankka fingerprints it; `descriptor/fixtures/contracts/fingerprints.json` holds
   ankka's own values, and an SDK that fingerprints for itself compares every one with them, as the
   Flink SDK's `CanonicalTest` does.
3. **The main's modes.** Given `--descriptor <file>` the streamlet's main writes its descriptor;
   given `--descriptor-check <file>` it fails when the file differs from the declaration. Otherwise it
   runs.
4. **The settings.** At run time the streamlet reads `streamlet.properties` from the directory
   `FLOW_CONFIG_DIR` names, refuses settings that do not match its declared ports and parameters, and
   connects each port to its topic with the Kafka properties given. With `local=true` it runs on a
   laptop with no cluster. [Streamlet settings](../reference/streamlet-settings.md) defines the file.
5. **Delivery.** What the engine promises, said plainly and proven by a suite against a real Kafka:
   at the least, no offset committed before everything made from its record is written.
6. **A harness.** A way to run the streamlet's logic in a test with no broker and no cluster.

The SDK's suite declares each fixture of its engine with the `sdk` block pinned to
`{"name": "fixture", "version": "0.0.0"}` and asserts that the bytes equal the fixture's file. It also
asserts that every fixture naming its engine is covered, and only those, so a fixture added later
cannot go unwritten. The Pekko Streams SDK's `DescriptorFixturesSuite` and the Flink SDK's
`DescriptorFixturesTest` are the two such suites.

## A rendering in the operator

`Rendering.render` turns an `AnkkaFlow` resource and what the operator observed into actions, and
does no I/O; the executors perform the actions. A new engine adds:

- **Its workload.** What runs a streamlet of the engine: a Deployment for Pekko Streams, a
  `FlinkDeployment` for Flink (`FlinkRendering`), each named `flow-<pipeline>-<streamlet>` and given
  the streamlet's settings Secret. The engine joins the set of engines the operator runs; until then a
  resource with a streamlet of it is refused, naming the engine.
- **Its prerequisites.** Anything the cluster must have before such a streamlet can run, each checked
  before anything is applied and refused with a condition of its own, as Flink's
  `FlinkOperatorMissing` and `StateStoreMissing` are.
- **Its lifecycle.** How the operator reads the workload's readiness into the streamlet's status and
  the pipeline's phase and conditions (`LifecycleRules`), and what a reset does to a streamlet of the
  engine (`Reset`): a consumer group moved back to the start, or a new state epoch. Lag is read from
  the inlets' consumer groups, and is the same for every engine.
- **Its deploy-time settings.** Any key only the engine takes, under `flow.streamlets.<name>`, read by
  the `blueprint` module's `Overrides` and refused on a streamlet of another engine.

The `operator` module's rendering suites prove the actions; the cluster suite, `FlowClusterSuite`,
proves a pipeline with a streamlet of the engine becomes `Ready`, moves records, reports its lag and
can be reset, on k3s.

## And the rest

An engine is finished when a reader can use it: a sample streamlet in `samples/` with its tests and
image build, a guide under `build/` and an SDK reference under `reference/`, a row on
[Choosing an engine](../concepts/engines.md), its delivery promise on
[Limitations](../reference/limitations.md), and a skill for coding agents.

---
title: Pekko Streams SDK
description: "The types of ankka-flow-pekko-streams: PekkoStreamlet's port and parameter factories, Stage, Logic, Emit, codecs, contracts, records, PekkoStreamlets' modes and PekkoHarness."
kind: reference
languages: [scala]
related: [build/pekko-streams-streamlet.md, reference/descriptor.md, reference/streamlet-settings.md]
---

# Pekko Streams SDK

`com.thinkmorestupidless::ankka-flow-pekko-streams`, package
`com.thinkmorestupidless.ankka.flow.pekko`, for Scala 3.3 or later on Java 21.

## PekkoStreamlet

`abstract class PekkoStreamlet(name: String, description: String = "")`, extended by an `object`.

| Member | What it does |
|---|---|
| `inlet(name, contract, codec)` | declares an inlet; an `Inlet[A]` |
| `outlet(name, contract, codec)` | declares an outlet; an `Outlet[A]`, which makes emits: `out(key, value)` or `out(record)` |
| `stringParameter`, `intParameter`, `doubleParameter`, `booleanParameter` | a parameter of that type: `key`, `default`, `description` |
| `durationParameter` | a duration such as `1m` or `500ms`, read as a `FiniteDuration` |
| `memorySizeParameter` | a size such as `64MiB`, read as a number of bytes |
| `from(inlet)(stage => …)` | the inlet's stage, as a `Logic` |
| `logic(ctx: Context): Logic` | what the streamlet does, given its parameters |

Ports and parameters appear in the descriptor in the order declared.

## Stage, Logic, Emit

- `Stage[A]` is a `FlowWithContext[Record[A], Offset, Record[A], Offset, NotUsed]`: the inlet's
  records with the offset of each carried as context. Its operators keep each element with the
  offset it came from.
- What a stage returns has elements of type `Emit`, `Seq[Emit]` or `Option[Emit]`. `Nil` and `None`
  write nothing and still let the offset be committed.
- `Logic`: one stage per inlet, joined with `and`. `Logic.empty` gives no stage.
- An offset is committed once every emit of its record, and of every record before it on the same
  partition, is acknowledged by the broker.

## Context

| Member | Returns |
|---|---|
| `ctx(parameter)` | the parameter's deployed value, or its default |
| `ctx.system` | the streamlet's `ActorSystem`, for a stage that calls out with `mapAsync` |

## Codec, Contract, Record

- `Codec.json[A]` reads and writes JSON through a jsoniter-scala `JsonValueCodec[A]` in scope;
  `Codec.string` reads and writes UTF-8 text; `Codec.bytes` passes bytes through. A record that does
  not decode fails the stream.
- `Contract(name)` names a contract; `Contract(name, schema)` adds the schema document, a classpath
  resource whose fingerprint goes into the descriptor.
- `Record[A](key: Option[String], value: A, headers)`; `withValue(b)` keeps the key and headers.

## PekkoStreamlets

`PekkoStreamlets.run(streamlet, args)` is the process's `main`:

| Arguments | What happens |
|---|---|
| none | reads `streamlet.properties` from the directory `FLOW_CONFIG_DIR` names, refuses settings that do not match the declared ports and parameters, and runs one stream per inlet until stopped |
| `--descriptor <file>` | writes the descriptor to the file and exits |
| `--descriptor-check <file>` | exits non-zero when the file differs from the declaration |

A failed stream restarts from the last commit after a backoff of one to thirty seconds,
indefinitely. On a stop the streams drain: what is in flight is written and committed.

## PekkoHarness

| Member | What it does |
|---|---|
| `PekkoHarness(streamlet, "key" -> "value", …)` | a harness with those parameter values, the rest at their defaults |
| `run(inlet -> records, …)` | runs each inlet's stage over its records; a `Result` |
| `result(outlet)` | what the outlet was written, in order |
| `result.committed` | the positions whose offsets would be committed |

Each record passes through its inlet's codec on the way in, and each emit through its outlet's on
the way out, so a codec that cannot do either fails the run as it would fail the stream.

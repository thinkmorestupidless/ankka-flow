# Flink SDK

> The classes of ankka-flow-flink and ankka-flow-flink-testkit: FlinkStreamlet's port and parameter factories, FlinkContext, codecs, contracts, records, FlinkStreamlets' modes and FlinkHarness.

Source: https://flow.ankka.cloud/reference/flink-sdk/
`com.thinkmorestupidless:ankka-flow-flink`, package `com.thinkmorestupidless.ankka.flow.flink`,
for Java 17 or later and Flink 2.2. Flink's modules are `provided`: the image supplies them.

## FlinkStreamlet

The base class of a streamlet. Its constructor takes the streamlet's name and, optionally, a
description; both go into the descriptor.

| Member | What it does |
|---|---|
| `inlet(name, contract, codec)` | declares an inlet; returns an `Inlet<A>` |
| `outlet(name, contract, codec)` | declares an outlet; returns an `Outlet<A>` |
| `stringParameter`, `intParameter`, `doubleParameter`, `booleanParameter` | a parameter of that type: key, default, optional description |
| `durationParameter(key, default, description)` | a duration such as `1m` or `500ms`; read as `java.time.Duration` |
| `memorySizeParameter(key, default, description)` | a size such as `64MiB`; read as a number of bytes |
| `build(FlinkContext ctx)` | the job: read the inlets, transform, write the outlets |
| `entryClass()` | the class Flink starts; the streamlet's own class unless overridden |

Ports and parameters are declared as fields and appear in the descriptor in the order declared.

## FlinkContext

| Method | Returns |
|---|---|
| `env()` | the job's `StreamExecutionEnvironment` |
| `read(inlet)` | the inlet as a `DataStream<Record<A>>`, with no watermarks |
| `read(inlet, watermarks)` | the same, with a `WatermarkStrategy<Record<A>>` for event time |
| `write(outlet, stream)` | connects a `DataStream<Record<A>>` to the outlet's topic |
| `get(parameter)` | the parameter's deployed value, or its default |

Each source and sink has a stable `uid`, so a new image of the streamlet finds their state.

## Codec, Contract, Record

- `Codec.json(Class<A>)` reads and writes JSON with Jackson; `Codec.string()` reads and writes UTF-8
  text; `Codec.bytes()` passes bytes through.
- `Contract.of(name)` names a contract; `Contract.of(name, schemaResource)` adds the schema
  document, read from the classpath, whose fingerprint goes into the descriptor.
- `Record<A>(key, value, headers, timestamp)`: the Kafka key, the decoded value, the headers and the
  Kafka timestamp in milliseconds. `Record.of(key, value, timestamp)` makes one with no headers;
  `withValue(other)` keeps everything else.

## FlinkStreamlets

`FlinkStreamlets.run(streamlet, args)` is the streamlet's `main`:

| Arguments | What happens |
|---|---|
| none | reads `streamlet.properties` from the directory `FLOW_CONFIG_DIR` names, refuses settings that do not match the declared ports and parameters, builds the job and runs it |
| `--descriptor <file>` | writes the descriptor to the file and exits |
| `--descriptor-check <file>` | exits non-zero when the file differs from the declaration |

Settings that say `local=true`, as `flow local` writes them, run the job on a local Flink in the
same process, checkpointing in memory every ten seconds so that offsets are committed; its state
lasts only while the process runs.

## FlinkHarness

`com.thinkmorestupidless:ankka-flow-flink-testkit`, package
`com.thinkmorestupidless.ankka.flow.flink.testkit`.

| Method | What it does |
|---|---|
| `FlinkHarness.of(streamlet)` | a harness for the streamlet, every parameter at its default |
| `parameter(key, value)` | sets a parameter as the deployed text would |
| `input(inlet, records)` | the records the inlet reads; an inlet given none reads nothing |
| `run()` | runs the job to the end of its inputs; returns a `Result` |
| `Result.records(outlet)` | what the outlet wrote, in order |

A job that fails throws `FlinkHarness.Failed`, carrying the job's root cause.

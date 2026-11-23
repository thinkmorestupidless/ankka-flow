# Contract: `flow`, as this feature changes it

## Unchanged in form

`flow verify`, `flow generate`, `flow reset`, `flow version`, `flow mcp`, `flow mcp install`.
`flow version` prints `flow <v>, descriptor 2.0`.

## Removed

`flow init`, with its templates.

## `--ankka-topics <project>=<file>`

On `verify`, `generate` and `local`; repeatable, one per project. The file is what `ankka
projects topics list -p <project> -o json` prints: a JSON array whose elements have `name` and,
when the topic declares one, `contract: {"name", "fingerprint"}`. Other fields are ignored.

| Case | Outcome |
|---|---|
| port and topic agree | verified |
| names differ, or both state a fingerprint and they differ | refused: `<streamlet>.<port> declares contract <a> and ankka topic <project>/<name> declares <b>` |
| the project does not declare the topic | refused: `ankka topic <project>/<name> is not declared; project <project> declares: <names>` |
| the topic declares no contract | refused: `ankka topic <project>/<name> declares no contract: declare one with ankka projects topics set <name> --contract … --schema …` |
| no file for a project the blueprint names | verified, with the note `ankka topics of project '<project>' were not checked: pass --ankka-topics <project>=<file>` |
| a file for a project the blueprint does not name | refused, naming it |
| a topic's `contract` present and not `null`, but not an object, or with no string `name`, or a `fingerprint` that is not a string | refused as a file flow cannot read: `--ankka-topics <option>: '<file>': topic '<name>': a contract must be an object` (or `must have a name`, or `a contract's fingerprint must be a string`) |

Always, with or without a file: `ankka` with `managed = true` is refused (`an ankka topic is the
ankka project's: a pipeline never creates it`), as is `ankka` with `topic.name` or
`bootstrap.servers`.

## Blueprint

```hocon
blueprint.topics.checkouts { ankka = "checkout/cart-checkouts", consumers = [cleaner.in] }
blueprint.topics.cleaned   { ankka = "checkout/clean-checkouts", producers = [cleaner.out] }
```

The generated resource's topic has `name: checkout.cart-checkouts`, `managed: false`,
`ankka: {project, name}` and `cluster: ankka`, or the cluster `flow.ankka.cluster` or the topic's
own `cluster` names.

## Deploy-time configuration

```hocon
flow.ankka.cluster = "pipelines"                      # the Kafka cluster Secret that is the machine
flow.streamlets.cleaner { replicas = 3 }              # Pekko Streams
flow.streamlets.counter {                             # Flink
  parallelism = 2
  flink { checkpoint-interval = 10s, task-manager { cpu = 1, memory = 2048m }, job-manager { cpu = 1, memory = 1024m } }
  config { window = 1m }
}
```

`replicas` on a Flink streamlet may only be `0` (stopped) or `1`. `parallelism` or `flink` on a
Pekko Streams streamlet is refused, naming the key and the engine. `consumer-config.flow.batch` is
no longer a setting and is refused by name.

## `flow local`

```text
flow local <blueprint.conf> --descriptors DIR --bootstrap-servers HOST:PORT -o DIR
           [--conf FILE]... [--ankka-topics P=FILE]... [--ankka-bootstrap-servers HOST:PORT]
```

Verifies as `flow verify` does, then writes `<-o>/<streamlet>/streamlet.properties` for every
streamlet (`contracts/streamlet-settings.md`) with `local=true`, every topic on
`--bootstrap-servers` in plaintext, every outlet `at-least-once`, groups and topic names as a
cluster would have them. A blueprint with an ankka topic needs `--ankka-bootstrap-servers`, or is
refused saying so. Prints one line per file written. Touches no cluster and no broker.

## `flow mcp` tools

| Tool | Change |
|---|---|
| `pipeline_lag` | reads `status.streamlets[].inlets[]`; one row per streamlet and inlet: `streamlet  engine  inlet  topic  group  lag`; `lag` is `unknown` when the status has none |
| `pipeline_logs` | the `container` input is removed; returns the recent lines of every pod of the streamlet (for Flink, its job manager and task managers), each under a `--- <pod>` line |
| `get_pipeline` | also shows the conditions and each streamlet's engine |
| others | unchanged |

# CLI

> Every command, option, message and exit code of the flow CLI, which verifies blueprints, writes a laptop's streamlet settings, generates the AnkkaFlow resource and requests resets.

Source: https://flow.ankka.cloud/reference/cli/
`flow` verifies a blueprint, writes the settings to run its streamlets on a laptop, writes the
`AnkkaFlow` resource a cluster runs, and requests resets. It is one native executable with no JVM to install, for macOS (Apple silicon and Intel) and Linux (x64 and
arm64). Homebrew installs it from the tap ankka's CLI ships through; every release also attaches an
archive and a checksum per platform, and [Install the tools](../get-started/install.md) covers both:

```bash
brew install thinkmorestupidless/tap/ankka-flow
flow version
```

`flow version` prints the CLI's version and the descriptor version it reads, for example
`flow 0.6.0, descriptor 2.0`. A platform outside the four builds the CLI from source;
[Build ankka-flow from source](../contributing/building.md) covers that build, which behaves the same.

| exit code | meaning |
|---|---|
| `0` | success, or `--help` |
| `1` | refused or failed; every problem is on stderr, one per line |
| `2` | usage error: an unknown command or option, or a missing argument |

`verify`, `generate` and `local` need no image, no network and no engine. `reset` needs a
kubeconfig.

## `flow verify`

```text
flow verify <blueprint.conf> [--descriptors <dir>] [--conf <file>]... [--ankka-topics <project>=<file>]...
```

| option | meaning |
|---|---|
| `<blueprint.conf>` | the blueprint, HOCON; see [the blueprint reference](blueprint.md) |
| `--descriptors <dir>` | a directory whose `*.json` files are streamlet descriptors; other files are ignored |
| `--conf <file>` | deploy-time configuration, HOCON; repeatable, later files win; see [Configure at deploy time](../deploy/configuration.md) |
| `--ankka-topics <project>=<file>` | an ankka project's topic declarations, as `ankka projects topics list -p <project> -o json` prints them; repeatable, one per project; see [Read and write ankka topics](../build/ankka-topics.md) |

It reads and validates every descriptor, parses the blueprint, checks it against the descriptors, and
checks the `--conf` files against the result. On success it prints
`verified: <n> streamlets, <m> topics` on stdout. Otherwise it refuses, listing every problem it found in
one pass:

| problem | message |
|---|---|
| the directory is missing or empty | `descriptors: '<dir>' is not a directory`, `descriptors: '<dir>' holds no *.json files` |
| a descriptor does not parse or fails validation (no engine or an unknown one, a bad name, a port declared twice, a malformed contract, an unsupported descriptor version) | `descriptor <file>: <problem>`; [Descriptor](descriptor.md#refusals) lists them |
| two descriptors declare one streamlet | `2 descriptors declare streamlet '<name>'` |
| the blueprint is not a file, or not HOCON | `blueprint: '<path>' is not a file`, `The blueprint file has an invalid format: …` |
| a streamlet names a descriptor nobody declares | `Streamlet '<name>' names descriptor '<descriptor>', which no descriptor declares.` |
| two streamlets share a name, or a name is illegal | `Duplicate streamlet names detected: …`, `Invalid streamlet name '<name>'. …` |
| a port path names nothing | `'<path>' does not point to a known streamlet inlet or outlet, please try <suggestion>.` |
| a producer is not an outlet, or a consumer is not an inlet | `'<path>' is not a valid producer for topic '<id>', must be an outlet.` (and the consumer form) |
| two ports on one topic carry different contracts | `'<path>' (contract <contract>) is not compatible with '<path>' (contract <contract>).` |
| an inlet is connected to nothing | `Inlet <streamlet>.<port> is not connected.` |
| a port is bound to more than one topic | `'<path>' is bound to more than one topic: <ids>.` |
| an unmanaged topic has producers | `Topic '<id>' is not managed but has producers <paths>; the platform only reads topics it does not own.` |
| an unmanaged topic names no brokers and no cluster | `Topic '<id>' is not managed and names no bootstrap.servers or cluster.` |
| an ankka topic that is not `<project>/<name>` | `Topic '<id>' names ankka topic '<value>', which is not <project>/<name>.` |
| an ankka topic that is managed, or names `topic.name` or `bootstrap.servers` | `Topic '<id>' is an ankka topic, and an ankka topic is the ankka project's: a pipeline never creates it.`, `Topic '<id>' is an ankka topic and names <key>; ankka names its Kafka topic and its brokers.` |
| a port's contract is not the ankka topic's | `<streamlet>.<port> declares contract <a> and ankka topic <project>/<name> declares <b>` |
| the project does not declare the ankka topic | `ankka topic <project>/<name> is not declared; project <project> declares: <names>` |
| the ankka topic declares no contract | `ankka topic <project>/<name> declares no contract: declare one with ankka projects topics set <name> --contract … --schema …` |
| `--ankka-topics` names a project no topic does | `--ankka-topics names project '<project>', which no topic of the blueprint does` |
| `--ankka-topics` not of the form `<project>=<file>` | `--ankka-topics <option>: expected <project>=<file>` |
| an `--ankka-topics` file that cannot be read, or is not the JSON array ankka prints | `--ankka-topics <option>: cannot read '<file>': …`, or `--ankka-topics <option>: '<file>': expected the JSON array ankka projects topics list -o json prints` |
| a topic in that file whose `contract` flow cannot read | `--ankka-topics <option>: '<file>': topic '<name>': a contract must be an object`, `… must have a name` or `… a contract's fingerprint must be a string` |
| a topic sets `consumer-config.flow.batch` | `Topic '<id>' sets consumer-config.flow.batch, which is no longer a setting; remove it.` |
| an illegal topic or Kafka cluster name | `'<name>' is not a valid topic name, …`, `Invalid Kafka cluster name '<name>'. …` |
| `--conf` does not parse | `--conf <file>: <reason>` |
| `--conf` names a topic or streamlet the blueprint does not | `overrides name unknown topic '<id>'`, `overrides name unknown streamlet '<name>'` |
| `--conf` sets a parameter the descriptor does not declare | `streamlet '<name>': parameter '<key>' is not declared` |
| a parameter has no default and no value | `streamlet '<name>': parameter '<key>' has no default and no value` |
| a parameter's value is not its type | `streamlet '<name>': parameter '<key>' = <value> is not a <type>` |
| `replicas` is not a number, or is negative | `streamlet '<name>': replicas must be a number`, `… must not be negative` |
| a Flink streamlet's `replicas` is not 0 or 1 | `streamlet '<name>': a flink streamlet's replicas may only be 0 or 1` |
| `parallelism` is not a number of at least 1 | `streamlet '<name>': parallelism must be a number of at least 1` |
| a `flink` setting is unknown or not its type | `streamlet '<name>': flink.<key> is not a setting`, `… flink.checkpoint-interval = <v> is not a duration`, and the like for `cpu` and `memory` |
| a Pekko Streams streamlet sets `parallelism` or `flink` | `Streamlet '<name>' runs on pekko-streams, which takes no '<key>'; it is a setting of flink streamlets.` |

An outlet connected to nothing is allowed. It is printed on stderr as a note
(`note: Outlet <streamlet>.<port> is not connected.`) and does not change the exit code.

An ankka project that the blueprint names and `--ankka-topics` does not gets a note too:
`note: ankka topics of project '<project>' were not checked: pass --ankka-topics <project>=<file>`.

## `flow generate`

```text
flow generate <blueprint.conf> [--descriptors <dir>] [--conf <file>]... [--ankka-topics <project>=<file>]...
              [--images <file>] [--image <name>=<ref>]...
              [--pipeline <id>] [--version <v>] [-n|--namespace <ns>] [-o|--output <file>]
              [--delete-managed-topics]
```

Everything `verify` does, then it writes the `AnkkaFlow` resource as YAML, to stdout or to `--output`
(which prints `wrote <file>` on stderr). Deploy-time configuration is merged over the blueprint here, so
the resource says exactly what will run; the operator adds only the Kafka cluster settings and the
state store.

| option | meaning |
|---|---|
| `--images <file>` | a HOCON map of streamlet name to image reference |
| `--image <name>=<ref>` | one streamlet's image; repeatable; wins over `--images` |
| `--pipeline <id>` | the pipeline id; default `blueprint.name`, else the blueprint's file name up to its first `.` |
| `--version <v>` | `spec.version`; default `git describe --tags --always --dirty` in the blueprint's directory, else `unversioned` |
| `-n`, `--namespace <ns>` | `metadata.namespace`; without it the resource has none and `kubectl` uses its current namespace |
| `-o`, `--output <file>` | write here instead of stdout |
| `--delete-managed-topics` | `spec.onDelete.managedTopics: Delete`: deleting the resource deletes the topics the pipeline created, and their records; without it, `Keep` |

The resource's name and `spec.pipeline` are both the pipeline id. On top of `verify`'s problems it
refuses when:

| problem | message |
|---|---|
| a streamlet has no image | `Streamlet '<name>' has no image.` |
| an `--images` file does not parse | `images: <reason>` |
| an `--image` is not `name=ref` | `--image '<value>' is not name=reference` |
| the pipeline id is not a DNS label of at most 40 characters | `pipeline id '<id>' must be 1-40 of [a-z0-9-], not starting or ending with '-'` |

An images file:

```hocon
# The image of each streamlet in the blueprint, by its name there: what `flow generate --images` reads.
cleaner = "registry.example.com/cart-cleaner:0.6.0"
counter = "registry.example.com/cart-counter:0.6.0"
```

A typical deployment:

```bash
flow generate blueprint.conf --descriptors flow --conf prod.conf \
  --image cleaner=registry.example.com/cart-cleaner:1.2 -n shop | kubectl apply -f -
```

Every topic is written as the blueprint and `--conf` say; an ankka topic is written unmanaged, with
`ankka: {project, name}` and the cluster `flow.ankka.cluster`, `ankka` or its own `cluster` names.

See [the resource reference](resource.md) for every field `generate` writes.

## `flow local`

```text
flow local <blueprint.conf> [--descriptors <dir>] [--conf <file>]... [--ankka-topics <project>=<file>]...
           --bootstrap-servers <host:port> -o|--output <dir> [--ankka-bootstrap-servers <host:port>]
```

Everything `verify` does, then it writes `<dir>/<streamlet>/streamlet.properties` for every streamlet,
the [settings](streamlet-settings.md) it runs with on a laptop, and prints `wrote <file>` for each. Set
`FLOW_CONFIG_DIR` to a streamlet's directory and start it.

| option | meaning |
|---|---|
| `--bootstrap-servers <host:port>` | the broker every topic is on, reached in plaintext |
| `-o`, `--output <dir>` | the directory to write each streamlet's directory in |
| `--ankka-bootstrap-servers <host:port>` | the broker every ankka topic is on, such as ankka's local platform's; required when the blueprint has an ankka topic |

The settings carry `local=true`, the namespace `local`, the topic names and consumer groups a cluster
would give them, the parameters with `--conf` applied, and every outlet at least once. It touches no
cluster and no broker. Without `--ankka-bootstrap-servers`, a blueprint with an ankka topic is refused:
`the blueprint reads or writes ankka topics: pass --ankka-bootstrap-servers HOST:PORT, the broker of ankka's local platform`.

## `flow reset`

```text
flow reset <pipeline> [--streamlet <name>]... [-n|--namespace <ns>]
```

Records a request that the named streamlets reread their inputs from the earliest offset, and that a
Flink streamlet among them start from an empty state. The operator carries it out; see
[Rebuild from the start](../deploy/reset.md).

| option | meaning |
|---|---|
| `<pipeline>` | the `AnkkaFlow`'s name |
| `--streamlet <name>` | a streamlet to reset; repeatable; default every streamlet with an inlet |
| `-n`, `--namespace <ns>` | the pipeline's namespace; default the kubeconfig's current namespace |

It reads the `AnkkaFlow` and the pods labelled with its pipeline, then refuses when:

| problem | message |
|---|---|
| Kubernetes cannot be reached | `cannot reach Kubernetes: <reason>` |
| no such pipeline | `no pipeline '<pipeline>' in namespace '<ns>'` |
| a named streamlet does not exist, or has no inlets | `cannot reset offsets: no streamlet [<name>]`, `… streamlet [<name>] has no inlets, so no consumer groups to reset` |
| no streamlet has an inlet | `pipeline <pipeline> has no streamlets with inlets to reset` |
| a target's `replicas` is not 0, or it still has pods | `cannot reset offsets while streamlets are running: [<name>] is not scaled to 0; [<name>] still has <n> pod(s). Stop them first: …` |

Otherwise it writes the annotation `flow.ankka.thinkmorestupidless.com/reset-offsets` with
`{"id":"<uuid>","streamlets":[…]}` and prints `reset requested for '<pipeline>': <uuid>`. An empty
`streamlets` list means every streamlet with an inlet.

## `flow mcp`

```text
flow mcp
```

Serves a Model Context Protocol server over standard input and output, for a coding agent: `flow`'s
abilities as tools and this documentation as resources. Standard output carries only the protocol;
started at a terminal, it prints a notice on standard error and waits for a client. [Work with a coding
agent](../get-started/coding-agents.md#the-mcp-server) lists the tools and how to connect a client.

It reads `flow.toml` from the directory it is started in — the project's root, when Claude Code starts
it — and that file's `context` and `namespace` are the only cluster the cluster tools touch:

```toml
context   = "kind-ankka"
namespace = "shop"
```

Without the file, or with either key empty, every cluster tool answers with a refusal saying what to
write there, and the tools that need no cluster still work. The kubeconfig's current context is never
used.

| tool | hints | does |
|---|---|---|
| `verify_blueprint` | read-only | what `flow verify` prints, refusals included |
| `generate_resource` | read-only | the YAML `flow generate` writes; nothing is applied |
| `flow_version` | read-only | what `flow version` prints |
| `search_docs`, `read_doc` | read-only | this version's documentation, also served as `ankka-flow://docs/<path>` resources |
| `list_pipelines`, `get_pipeline` | read-only, on the named cluster | the namespace's pipelines; one pipeline's status, conditions, each streamlet's engine, and its events |
| `pipeline_logs` | read-only, on the named cluster | the recent lines of every pod of a streamlet, each under a `--- <pod>` line; for a Flink streamlet, its job manager and task managers |
| `pipeline_lag` | read-only, on the named cluster | one row per streamlet and inlet from the status: `streamlet  engine  inlet  topic  group  lag`, the lag `unknown` where the status has none |
| `apply_pipeline` | destructive, on the named cluster | verify, generate and create or update the resource, as `flow generate | kubectl apply` would |
| `reset_pipeline` | destructive, on the named cluster | what `flow reset` requests, with its refusals |

A tool's failure is answered as an error result the agent can read; the session continues.

## `flow mcp install`

```text
flow mcp install [--client code|desktop] [--scope user|project] [--dir <project>]
                 [--command <path>] [--force] [--dry-run]
```

Tells an MCP client how to start `flow mcp`.

| option | default | meaning |
|---|---|---|
| `--client` | `code` | `code` for Claude Code, `desktop` for Claude Desktop |
| `--scope` | `user` | `user`: every project, for you, through `claude mcp add --scope user`; `project`: a `.mcp.json` in the project, to commit |
| `--dir <project>` | `.` | the project, for `--scope project` |
| `--command <path>` | the first `flow` on `PATH` | the `flow` Claude Desktop starts; Desktop does not see a shell's `PATH`, so the entry holds an absolute path, and for the JVM build a `JAVA_HOME` |
| `--force` | — | replace an existing `ankka-flow` entry |
| `--dry-run` | — | print what would change; change nothing |

Every write merges: other servers and settings in the file are kept in their order, and an existing
server named `ankka-flow` that differs is left as it is and shown, unless `--force`.

It exits 0 when it changed what it said, or had nothing to change, and 1 with the reason on stderr
when it could not: `--scope project` with `--client desktop`, Claude Desktop on an operating system
without it, a configuration file that is not valid JSON (left as it is), no `flow` to name for Desktop,
or a `claude` command that failed. When `claude` is not on `PATH`, `--scope user` changes nothing and
prints the command to run; that is exit 0.

## `flow version`

```text
flow version
```

Prints the CLI's version and the descriptor version it reads, as
`flow <version>, descriptor <major>.<minor>`.

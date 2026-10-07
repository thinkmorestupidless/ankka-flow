# CLI

> Every command, option, message and exit code of the flow CLI, which starts streamlet projects, verifies blueprints, generates the AnkkaFlow resource and requests resets.

Source: https://flow.ankka.cloud/reference/cli/
`flow` starts a streamlet project, verifies a blueprint, writes the `AnkkaFlow` resource a cluster runs,
and requests resets. It is
one native executable with no JVM to install, for macOS (Apple silicon and Intel) and Linux (x64 and
arm64). Homebrew installs it from the tap ankka's CLI ships through; every release also attaches an
archive and a checksum per platform, and [Install the tools](../get-started/install.md) covers both:

```bash
brew install thinkmorestupidless/tap/ankka-flow
flow version
```

`flow version` prints the CLI's version and the protocol version it writes, for example
`flow 0.1.0, protocol 1.0`. A platform outside the four builds the CLI from source;
[Build ankka-flow from source](../contributing/building.md) covers that build, which behaves the same.

| exit code | meaning |
|---|---|
| `0` | success, or `--help` |
| `1` | refused or failed; every problem is on stderr, one per line |
| `2` | usage error: an unknown command or option, or a missing argument |

`init`, `verify` and `generate` need no image, no network and no language runtime. `reset` needs a
kubeconfig.

## `flow init`

```text
flow init <name> [--language scala|python] [--dir <directory>] [--package <package>]
```

Writes a new streamlet project from templates carried inside `flow`; it needs no network and no copy
of the repository.

| option | default | meaning |
|---|---|---|
| `<name>` | — | the streamlet's and the pipeline's name |
| `--language`, `-l` | `scala` | `scala` or `python` |
| `--dir` | `<name>` | where to write; it must not exist, or be empty |
| `--package` | Scala: the name without `-`; Python: the name with `_` for `-` | the Scala package (dotted, lower case) or the Python module |

It refuses, writing nothing and exiting 2, with one line per problem on stderr:

| refused | message |
|---|---|
| a name that is not 1–63 of `[a-z0-9-]`, or that starts or ends with `-` | `name '<name>' must be 1-63 of [a-z0-9-], not starting or ending with '-'` |
| a name longer than 40 characters | `name '<name>' must be at most 40 characters: it becomes the pipeline id` |
| a name that does not start with a letter | `name '<name>' must start with a letter: it becomes a class, a package and a module` |
| a Scala package that is not dotted lower-case identifiers | `'<package>' is not a Scala package: …` |
| a Python module that is not a lower-case identifier, or is a keyword | `'<package>' is not a Python module: …` |
| a directory that holds files | `<directory> is not empty: flow init writes a new project into an empty or new directory` |
| a language other than `scala` or `python` | the usage, as for any wrong option |

On success it prints the number of files written and the next commands: change into the directory,
run the tests, check the descriptor, and `flow verify blueprint.conf --descriptors flow`. It exits 1,
writing nothing, only if a template would leave a `{{token}}` unreplaced, which the CLI's own tests
rule out.

The project holds:

| | Scala | Python |
|---|---|---|
| the streamlet | `src/main/scala/<package>/<Class>.scala` | `src/<module>/streamlet.py` |
| its entry point | `src/main/scala/<package>/Main.scala` | `src/<module>/main.py` |
| its test | `src/test/scala/<package>/<Class>Suite.scala` | `tests/test_streamlet.py` |
| the build and the image | `build.sbt`, `project/`; `sbt Docker/publishLocal` | `pyproject.toml`, `Dockerfile` |
| the descriptor command | `sbt descriptor`, `sbt descriptorCheck` | `uv run descriptor`, `uv run descriptor --check` |

and, for both, `flow/descriptor.json`, `flow/streamlet.conf`, `blueprint.conf`, `docker-compose.yml`,
`k8s/in-cluster.conf`, `README.md`, `.gitignore`, `.github/workflows/ci.yml` and the ankka-flow skills
under `.claude/skills/`. `<Class>` is the name in UpperCamelCase.

The streamlet has one inlet `in` and one outlet `out`, both JSON with the schema name `<name>.v1`, and
a string parameter `greeting` (default `hello, ankka-flow`). It adds `"greeting": <greeting>` to each
JSON object it reads and emits it with the record's key and headers; a value that is not a JSON object
fails the batch. The blueprint reads the unmanaged topic `<name>.in` and writes the managed topic `out`
(`<name>.out` in Kafka).

Versions follow the `flow` that wrote the project:

- the Scala project depends on `ankka-flow-sdk` at `flow`'s version;
- the Python project pins `ankka-flow`, and the committed descriptor names its SDK version, at the
  release's version — or `0.0.0` from a `flow` built between releases, the version the SDKs report
  then;
- `docker-compose.yml` runs `ghcr.io/thinkmorestupidless/ankka-flow-sidecar` at `flow`'s version, with
  `+` written as `-` (a Docker tag cannot hold `+`).

A project written by a `flow` built from source between releases names a version no registry holds:
build it against the repository's SDK, or write it with a released `flow`.

## `flow verify`

```text
flow verify <blueprint.conf> [--descriptors <dir>] [--conf <file>]...
```

| option | meaning |
|---|---|
| `<blueprint.conf>` | the blueprint, HOCON; see [the blueprint reference](blueprint.md) |
| `--descriptors <dir>` | a directory whose `*.json` files are streamlet descriptors; other files are ignored. Optional when every streamlet is built in (`builtin/<name>`), whose descriptors the CLI already knows |
| `--conf <file>` | deploy-time configuration, HOCON; repeatable, later files win; see [Configure at deploy time](../deploy/configuration.md) |

It reads and validates every descriptor, parses the blueprint, checks it against the descriptors, and
checks the `--conf` files against the result. On success it prints
`verified: <n> streamlets, <m> topics` on stdout. Otherwise it refuses, listing every problem it found in
one pass:

| problem | message |
|---|---|
| the directory is missing or empty | `descriptors: '<dir>' is not a directory`, `descriptors: '<dir>' holds no *.json files` |
| a descriptor does not parse or fails validation (a bad name, a port declared twice, a fingerprint that does not match its schema name, an unsupported protocol version) | `descriptor <file>: <problem>` |
| two descriptors declare one streamlet | `2 descriptors declare streamlet '<name>'` |
| the blueprint is not a file, or not HOCON | `blueprint: '<path>' is not a file`, `The blueprint file has an invalid format: …` |
| a streamlet names a descriptor nobody declares | `Streamlet '<name>' names descriptor '<descriptor>', which no descriptor declares.` |
| two streamlets share a name, or a name is illegal | `Duplicate streamlet names detected: …`, `Invalid streamlet name '<name>'. …` |
| a port path names nothing | `'<path>' does not point to a known streamlet inlet or outlet, please try <suggestion>.` |
| a producer is not an outlet, or a consumer is not an inlet | `'<path>' is not a valid producer for topic '<id>', must be an outlet.` (and the consumer form) |
| an outlet and an inlet on one topic carry different contracts | `'<outlet>' (<contract>) is not compatible with '<inlet>' (<contract>).` |
| a port uses a format other than `json` | `'<path>' uses format '<format>', which this version does not support; the only contract format is json.` |
| an inlet is connected to nothing | `Inlet <streamlet>.<port> is not connected.` |
| a port is bound to more than one topic | `'<path>' is bound to more than one topic: <ids>.` |
| an unmanaged topic has producers | `Topic '<id>' is not managed but has producers <paths>; the platform only reads topics it does not own.` |
| an unmanaged topic names no brokers and no cluster | `Topic '<id>' is not managed and names no bootstrap.servers or cluster.` |
| an illegal topic or Kafka cluster name | `'<name>' is not a valid topic name, …`, `Invalid Kafka cluster name '<name>'. …` |
| `--conf` does not parse | `--conf <file>: <reason>` |
| `--conf` names a topic or streamlet the blueprint does not | `overrides name unknown topic '<id>'`, `overrides name unknown streamlet '<name>'` |
| `--conf` sets a parameter the descriptor does not declare | `streamlet '<name>': parameter '<key>' is not declared` |
| a parameter has no default and no value | `streamlet '<name>': parameter '<key>' has no default and no value` |
| a parameter's value is not its type | `streamlet '<name>': parameter '<key>' = <value> is not a <type>` |
| `replicas` is not a number, or is negative | `streamlet '<name>': replicas must be a number`, `… must not be negative` |

An outlet connected to nothing is allowed. It is printed on stderr as a note
(`note: Outlet <streamlet>.<port> is not connected.`) and does not change the exit code.

A topic with a port of the graph delta contract gets a note too, saying whether it is compacted:
`note: Topic '<id>' carries graph deltas and is compacted (cleanup.policy = compact).`, or, for a
blueprint that set another policy or a topic that is not managed, what that means for rebuilding the
graph. [Blueprint](blueprint.md#delta-topics) lists the four notes.

## `flow generate`

```text
flow generate <blueprint.conf> [--descriptors <dir>] [--conf <file>]...
              [--images <file>] [--image <name>=<ref>]...
              [--pipeline <id>] [--version <v>] [-n|--namespace <ns>] [-o|--output <file>]
              [--delete-managed-topics]
```

Everything `verify` does, then it writes the `AnkkaFlow` resource as YAML, to stdout or to `--output`
(which prints `wrote <file>` on stderr). Deploy-time configuration is merged over the blueprint here, so
the resource says exactly what will run; the operator adds only the sidecar image and Kafka cluster
settings.

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
| a streamlet has no image | `Streamlet '<name>' has no image.` A built-in streamlet needs none |
| an image is given for a built-in streamlet | `Streamlet '<name>' is built in and takes no image.` |
| an `--images` file does not parse | `images: <reason>` |
| an `--image` is not `name=ref` | `--image '<value>' is not name=reference` |
| the pipeline id is not a DNS label of at most 40 characters | `pipeline id '<id>' must be 1-40 of [a-z0-9-], not starting or ending with '-'` |

An images file:

```hocon
router = "ghcr.io/example/cart-router:0.3.1"
sink   = "ghcr.io/example/cart-sink:0.3.1"
```

A typical deployment:

```bash
flow generate blueprint.conf --descriptors flow --conf prod.conf \
  --image router=registry.example.com/cart-router:1.2 -n shop | kubectl apply -f -
```

For a managed topic that carries graph deltas and sets no `cleanup.policy`, `generate` writes
`cleanup.policy: compact` into the topic's `topicConfig`, and prints the same note `verify` does. Every
other topic is written as the blueprint and `--conf` say.

See [the resource reference](resource.md) for every field `generate` writes.

## `flow reset`

```text
flow reset <pipeline> [--streamlet <name>]... [-n|--namespace <ns>]
```

Records a request that the named streamlets reread their inputs from the earliest offset. The operator
carries it out; see [Rebuild from the start](../deploy/reset.md).

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
namespace = "greeter"
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
| `list_pipelines`, `get_pipeline`, `pipeline_logs`, `pipeline_lag` | read-only, on the named cluster | the namespace's pipelines; one pipeline's status and events; a streamlet's process or sidecar log; consumer lag per inlet partition from each sidecar's metrics |
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
server named `ankka-flow` that differs is left as it is and shown, unless `--force`. A project written
by `flow init` already has its `.mcp.json`, so it needs no install.

It exits 0 when it changed what it said, or had nothing to change, and 1 with the reason on stderr
when it could not: `--scope project` with `--client desktop`, Claude Desktop on an operating system
without it, a configuration file that is not valid JSON (left as it is), no `flow` to name for Desktop,
or a `claude` command that failed. When `claude` is not on `PATH`, `--scope user` changes nothing and
prints the command to run; that is exit 0.

## `flow version`

```text
flow version
```

Prints the CLI's version and the protocol version its resources carry, as
`flow <version>, protocol <major>.<minor>`.

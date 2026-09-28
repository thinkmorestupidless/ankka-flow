# CLI

> Every command, option, message and exit code of the flow CLI, which verifies blueprints, generates the AnkkaFlow resource and requests resets.

Source: https://flow.ankka.cloud/reference/cli/
`flow` verifies a blueprint against streamlet descriptors, writes the `AnkkaFlow` resource the operator
runs, and asks the operator to reset a pipeline's consumer groups. It needs a JVM. Nothing installs it:
from a clone of the repository, `just cli` (or `sbt cli/stage`) builds it at
`cli/target/universal/stage/bin/flow`, and that directory goes on your `PATH`:

```bash
just cli
export PATH="$PWD/cli/target/universal/stage/bin:$PATH"
```

| exit code | meaning |
|---|---|
| `0` | success, or `--help` |
| `1` | refused or failed; every problem is on stderr, one per line |
| `2` | usage error: an unknown command or option, or a missing argument |

`verify` and `generate` need no image, no network and no language runtime. `reset` needs a kubeconfig.

## `flow verify`

```text
flow verify <blueprint.conf> --descriptors <dir> [--conf <file>]...
```

| option | meaning |
|---|---|
| `<blueprint.conf>` | the blueprint, HOCON; see [the blueprint reference](blueprint.md) |
| `--descriptors <dir>` | a directory whose `*.json` files are streamlet descriptors; other files are ignored |
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

## `flow generate`

```text
flow generate <blueprint.conf> --descriptors <dir> [--conf <file>]...
              [--images <file>] [--image <name>=<ref>]...
              [--pipeline <id>] [--version <v>] [-n|--namespace <ns>] [-o|--output <file>]
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
router = "ghcr.io/example/cart-router:0.3.1"
sink   = "ghcr.io/example/cart-sink:0.3.1"
```

A typical deployment:

```bash
flow generate blueprint.conf --descriptors flow --conf prod.conf \
  --image router=registry.example.com/cart-router:1.2 -n shop | kubectl apply -f -
```

`spec.onDelete.managedTopics` is always written as `Keep`. See [the resource reference](resource.md) for
every field `generate` writes.

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

## `flow version`

```text
flow version
```

Prints the CLI's version and the protocol version its resources carry, as
`flow <version>, protocol <major>.<minor>`.

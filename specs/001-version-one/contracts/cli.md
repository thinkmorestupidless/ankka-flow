# Contract: the CLI

**Feature**: [spec.md](../spec.md) | **Research**: R6, R12, R14

`flow` is a decline command. `sbt cli/stage` produces `cli/target/universal/stage/bin/flow`;
GraalVM is a later feature. Exit codes: 0 ok, 1 refused or failed (problems on stderr, one per
line), 2 usage error.

## `flow verify`

```
flow verify <blueprint.conf> --descriptors <dir> [--conf <overrides.conf>]...
```

Reads every `*.json` in `--descriptors` as a descriptor (contracts/descriptor.md), validates each,
parses the blueprint, and verifies (FR-004, FR-006). Prints `verified: <n> streamlets, <m> topics`
on success. Refuses, listing every problem in one pass, when the conditions below hold. The message
shapes are the design's; the exact wording the code writes is on `docs/reference/cli.md`, which the
build keeps honest (for example `Inlet cart.router.in is not connected.`, `Streamlet 'sink' has no
image.`).

| problem | message shape |
|---|---|
| descriptor fails validation | `descriptor <file>: <problem>` (bad fingerprint, duplicate port, bad name…) |
| streamlet names a descriptor that does not exist | `streamlet <name>: no descriptor named <descriptor>` |
| two streamlets share a name | `duplicate streamlet name <name>` |
| a port path names a streamlet or port no descriptor declares | `topic <id>: <path> not found; did you mean <suggestion>?` |
| an outlet and inlet on one topic differ in format or fingerprint | `topic <id>: <outlet path> (<format> <schema_name>) is not compatible with <inlet path> (<format> <schema_name>)` |
| a format other than `json` | `topic <id>: <path> uses format <format>, which this version does not support` |
| an inlet is connected to nothing | `inlet <path> is not connected to any topic` |
| a port is bound to several topics | `<path> is bound to topics <ids>` |
| an unmanaged topic has producers | `topic <id> is not managed but has producers <paths>` |
| an inlet on an unmanaged topic with no `bootstrap.servers` and no `cluster` | `topic <id> is not managed and names no brokers or cluster` |
| a topic name or cluster name is illegal | as Cloudflow's messages |
| a config parameter has no default and no value in `--conf` | `streamlet <name>: parameter <key> has no default and no value` |
| a value in `--conf` does not parse as its type | `streamlet <name>: parameter <key> = <value> is not a <type>` |
| `--conf` names a topic or streamlet the blueprint does not | `overrides name unknown topic <id>` |
| (generate only) a streamlet has no image | `streamlet <name>: no image` |

Unconnected outlets are printed as `note: outlet <path> is not connected` and do not refuse
(S2.2). `verify` never needs an image and never needs a network.

## `flow generate`

```
flow generate <blueprint.conf> --descriptors <dir> [--images <file>] [--image name=ref]... [--conf <file>]... [--pipeline <id>] [--version <v>] [--namespace <ns>] [-o <file>] [--delete-managed-topics]
```

Everything `verify` does, then writes the `AnkkaFlow` resource (contracts/resource-and-operator.md)
to `-o` or stdout. `--pipeline` defaults to the blueprint's `blueprint.name`, or the file's base
name; `--version` defaults to `git describe --tags --always --dirty` in the blueprint's directory,
or `unversioned`. Deploy-time overrides from `--conf` are applied over the blueprint here (R12), so
the emitted resource is exactly what will run except for what only the cluster knows (its Kafka
secrets). Applying it needs nothing else (S2.5). `--images` and `--image` combine, and `--image`
wins for the same streamlet. The pipeline id must be 1–40 characters of `[a-z0-9-]`, not starting or
ending with `-`. `spec.onDelete.managedTopics` is `Keep` unless `--delete-managed-topics` is given,
which writes `Delete`.

`--conf` files are HOCON:

```hocon
flow.topics.valid-carts { partitions = 12, topic { retention.ms = 604800000 } }
flow.topics.cart-events { bootstrap.servers = "shop-kafka:9092" }
flow.streamlets.router { replicas = 3, config { review-threshold = 250 } }
```

## `flow reset`

```
flow reset <pipeline> [--streamlet <name>]... [-n <namespace>]
```

Needs a kubeconfig. Refuses (exit 1) when Kubernetes cannot be reached or the pipeline's `AnkkaFlow`
does not exist. Otherwise reads it, then refuses when a named streamlet does not
exist or has no inlets, when any target's `replicas` is not 0 (`cannot reset offsets while streamlets are running: [router] is not scaled to 0 …`), or when a target still has pods (`router still
has 2 pod(s)`). Otherwise patches the annotation
`flow.ankka.thinkmorestupidless.com/reset-offsets` with `{"id":"<uuid>","streamlets":[…]}` and
prints the id. The operator carries it out (FR-023). With no `--streamlet`, every streamlet with an
inlet is a target.

## `flow version`

Prints the CLI's version and the protocol version it emits (`flow 0.1.0, protocol 1.0`).

## Tests

`cli/src/test`: a munit suite per command over the fixtures in `protocol/fixtures` and blueprints
under `cli/src/test/resources/blueprints/` (the cart pipeline; one blueprint per refusal above);
`reset`'s guards against a fabric8 mock server (`CliResetSuite`, carrying the assertions of
Cloudflow's `CliWorkflowSpec` lines 380–445).

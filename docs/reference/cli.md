# The `flow` CLI

`sbt cli/stage` builds `cli/target/universal/stage/bin/flow`. Exit codes: `0` ok, `1` refused or
failed (every problem on stderr, one per line), `2` usage error.

## `flow verify`

```
flow verify <blueprint.conf> --descriptors <dir> [--conf <overrides.conf>]...
```

Reads every `*.json` in `--descriptors` as a descriptor and checks the blueprint against them. It
needs no image, no network and no language runtime. It refuses, listing every problem in one pass,
when:

- a descriptor is invalid (a bad name, a port declared twice, a format other than `json`, a
  fingerprint that does not match its schema name);
- a streamlet names a descriptor that does not exist;
- a port path names a port no descriptor declares (the message suggests the ports that exist);
- an outlet and an inlet on one topic have different contracts (the message names both ports and
  both contracts);
- an inlet is connected to nothing;
- a port is bound to more than one topic;
- an unmanaged topic has producers, or names no brokers and no cluster;
- a topic name or cluster name is illegal;
- a parameter has no default and no value, or a value that is not its type;
- `--conf` names a topic, streamlet or parameter the blueprint does not.

An outlet connected to nothing is allowed and printed as a note.

## `flow generate`

```
flow generate <blueprint.conf> --descriptors <dir> (--images <file> | --image name=ref ...)
              [--conf <file>]... [--pipeline <id>] [--version <v>] [-n <namespace>] [-o <file>]
```

Everything `verify` does, then writes the `AnkkaFlow` resource as YAML. The pipeline id comes from
`--pipeline`, else `blueprint.name`, else the file name. The version comes from `--version`, else
`git describe`. Deploy-time configuration is merged over the blueprint here, so the resource says
exactly what will run:

```hocon
flow.topics.valid-carts { partitions = 12, topic { retention.ms = 604800000 } }
flow.streamlets.router  { replicas = 3, config { review-threshold = 250 } }
```

```bash
flow generate blueprint.conf --descriptors flow --image router=registry/cart-router:1.2 -n shop | kubectl apply -f -
```

## `flow reset`

```
flow reset <pipeline> [--streamlet <name>]... [-n <namespace>]
```

Requests that the named streamlets (default: every streamlet with an inlet) reread their inputs
from the earliest offset. It refuses unless every target is scaled to zero (`replicas: 0`) and has
no pods left. The operator carries the request out, records one event per consumer group, and
marks it done so it is never repeated.

## `flow version`

Prints the CLI's version and the protocol version it writes.

# Descriptor

> The descriptor file an SDK writes from a streamlet's declaration — its engine, ports, contracts and parameters in canonical JSON — and every rule flow and the operator check it against.

Source: https://flow.ankka.cloud/reference/descriptor/
A descriptor is what `flow` and the operator know about a streamlet: its name, its engine, its
ports and the contract each carries, and its parameters. The streamlet's SDK writes it from the
declaration in code, and it is never written by hand:

- a blueprint is verified against the descriptors in the `--descriptors` directory;
- `flow generate` copies each streamlet's descriptor into the resource;
- the streamlet, when it starts, refuses settings that do not match its declaration.

## Example

The sample's Flink streamlet:

```json
{
  "descriptor_version": "2.0",
  "sdk": {
    "name": "ankka-flow-flink",
    "version": "0.0.0"
  },
  "streamlet": {
    "config_parameters": [
      {
        "default_value": "1m",
        "description": "The length of each counting window.",
        "key": "window",
        "type": "DURATION"
      }
    ],
    "description": "Counts each cart's events in windows of the time they carry.",
    "engine": "flink",
    "flink": {
      "entry_class": "cart.counter.CartCounter",
      "version": "2.2"
    },
    "inlets": [
      {
        "contract": {
          "name": "clean-carts.v1"
        },
        "name": "in"
      }
    ],
    "name": "cart-counter",
    "outlets": [
      {
        "contract": {
          "name": "cart-counts.v1"
        },
        "name": "out"
      }
    ]
  }
}
```

## Fields

| Field | Rule |
|---|---|
| `descriptor_version` | `MAJOR.MINOR`; `flow` and the operator read major `2` |
| `sdk.name`, `sdk.version` | the SDK that wrote the file, such as `ankka-flow-flink` |
| `streamlet.name` | 1 to 63 of `a-z`, `0-9` and `-`, not starting or ending with `-`; what a blueprint's `streamlets` entry names |
| `streamlet.description` | optional |
| `streamlet.engine` | required: `pekko-streams` or `flink` |
| `streamlet.flink` | required when the engine is `flink`, and refused otherwise: `entry_class`, the class whose `main` runs the job, and `version`, Flink's `MAJOR.MINOR` (`2.2`) |
| `inlets[]`, `outlets[]` | each a `name` matching `[a-z][a-z0-9-]{0,62}`, unique across both lists, and a `contract` |
| `contract.name` | required; 2 to 100 of `a-z`, `0-9`, `.`, `_` and `-`, starting and ending with a letter or digit |
| `contract.fingerprint` | optional; `sha256:` and 64 lowercase hex digits: the SHA-256 of the schema document in RFC 8785 canonical form, as ankka computes it |
| `config_parameters[]` | each a `key` matching `[a-z][a-z0-9-]*`, unique, a `type`, an optional `default_value` that parses as the type, and an optional `description` |
| `config_parameters[].type` | `STRING` (omitted, being the default), `INTEGER`, `DOUBLE`, `BOOLEAN`, `DURATION`, `MEMORY_SIZE` |

Two ports connect when their contracts do: the same name and, where both state one, the same
fingerprint. See [Contracts](../concepts/contracts.md).

## Canonical JSON

Every SDK writes the same bytes for the same declaration:

1. field names in snake_case;
2. keys sorted at every level;
3. ports sorted by name, parameters by key;
4. a field at its default omitted: a `STRING` type, an absent fingerprint, default or description;
5. two-space indentation and one trailing newline.

## Writing and checking it

An SDK's main writes the descriptor and checks it:

```bash
sbt "cartCleaner/runMain cart.cleaner.Main --descriptor flow/cleaner.json"
sbt "cartCleaner/runMain cart.cleaner.Main --descriptor-check flow/cleaner.json"
```

`--descriptor-check` exits `1` with the difference when the file is not what the declaration
writes. Run it in the streamlet's build, so a declaration and its committed descriptor cannot drift.

## Refusals

`flow verify`, `flow generate`, `flow local` and the operator refuse a descriptor alike, every problem
at once:

| Descriptor | Message |
|---|---|
| no `engine`, or a `protocol_version` field | `<file>: a descriptor must name an engine (pekko-streams, flink)` |
| an engine `flow` does not know | `<file>: engine 'spark' is not one flow knows (pekko-streams, flink)` |
| `flink` missing on a Flink streamlet, or present on another | `<file>: a flink streamlet's descriptor has a flink section, and no other has` |
| a major version other than 2 | `<file>: descriptor version 3.0 is not one this flow reads (2.x)` |
| a name, port, contract or parameter that breaks a rule above | the rule, naming the field |

## Fixtures

[`descriptor/fixtures/descriptors`](https://github.com/thinkmorestupidless/ankka-flow/blob/main/descriptor/fixtures/descriptors)
holds the exact bytes each declaration in `descriptor/fixtures/declarations` must produce. Every
SDK's tests write each declaration and compare the bytes.

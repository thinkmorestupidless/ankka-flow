# The descriptor file

A streamlet's descriptor is what its SDK writes from the streamlet's declaration: its name, the
engine it is written for, its ports and their contracts, and its parameters. It is the only thing
`flow` and the operator know about a streamlet. The same declaration produces the same bytes in
every SDK, which the fixtures prove. It is never written by hand: a main writes it with
`--descriptor <path>` and checks it with `--descriptor-check <path>`.

It is read in two places: `flow` verifies a blueprint against it and copies it into the resource,
and the operator decides from it how the streamlet runs.

## Canonical JSON

1. Field names are snake_case: `descriptor_version`, `config_parameters`, `default_value`,
   `entry_class`.
2. Object keys are sorted lexicographically (byte order) at every level.
3. `inlets` and `outlets` are sorted by `name`; `config_parameters` by `key`. The SDK sorts; a
   declaration order is never meaningful.
4. Enumerations are their names (`"INTEGER"`), never numbers.
5. A field at its default is omitted: an empty string, an empty list, an absent fingerprint, the
   type `STRING`.
6. Two-space indentation, `": "` and `,\n` separators, LF line endings, UTF-8, exactly one trailing
   newline, no BOM, non-ASCII characters written as themselves.

Scala: `descriptor`'s `DescriptorJson.write`. Java: the Flink SDK's `DescriptorWriter`. Both are
tested against every fixture.

## Example: the cart counter

```json
{
  "descriptor_version": "2.0",
  "sdk": {
    "name": "fixture",
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

A fixture's `sdk` block is pinned to `{"name": "fixture", "version": "0.0.0"}` so that one file is
the expected output of every SDK. Outside the fixture test an SDK writes its own name and version.

## Fields

| Field | Rule |
|---|---|
| `descriptor_version` | `MAJOR.MINOR`; `flow` and the operator read major 2 |
| `streamlet.name` | 1 to 63 of `[a-z0-9-]`, not starting or ending with `-` |
| `streamlet.engine` | required: `pekko-streams` or `flink` |
| `streamlet.flink` | present exactly when the engine is `flink`: `entry_class`, the class whose `main` runs the job, and `version`, Flink's `MAJOR.MINOR` (`2.2`) |
| `inlets[].name`, `outlets[].name` | `[a-z][a-z0-9-]{0,62}`, unique across inlets and outlets together |
| `contract.name` | required; `[a-z0-9][a-z0-9._-]{0,98}[a-z0-9]`, the rule ankka gives a contract's name |
| `contract.fingerprint` | optional; `sha256:` and the lowercase hex SHA-256 of the schema document rendered under RFC 8785, as ankka computes it |
| `config_parameters[].key` | `[a-z][a-z0-9-]*`, unique |
| `config_parameters[].type` | `STRING` (omitted), `INTEGER`, `DOUBLE`, `BOOLEAN`, `DURATION`, `MEMORY_SIZE` |
| `config_parameters[].default_value` | parses as its type: `DURATION` as HOCON durations, `MEMORY_SIZE` as HOCON sizes |

Two ports connect over a topic when their contracts' names are equal and, where both state a
fingerprint, the fingerprints are equal.

## Refusals

`flow verify`, `flow generate`, `flow local` and the operator refuse alike:

| Descriptor | Message |
|---|---|
| no `engine`, or a `protocol_version` field | `a descriptor must name an engine (pekko-streams, flink)` |
| an unknown engine | `engine 'spark' is not one flow knows (pekko-streams, flink)` |
| `flink` missing or misplaced | `a flink streamlet's descriptor has a flink section, and no other has` |
| major version not 2 | `descriptor version 3.0 is not one this flow reads (2.x)` |

## Fixtures

`fixtures/descriptors/` holds the expected bytes and `fixtures/declarations/` what each declares.

| fixture | engine | declares |
|---|---|---|
| `minimal` | `pekko-streams` | one inlet, one outlet, no parameters, no description |
| `sink` | `pekko-streams` | inlets only, no outlets |
| `every-type` | `flink` | one parameter of every type, one required (no default), unicode in a description |
| `many-ports` | `pekko-streams` | five inlets and five outlets declared out of order, to prove sorting |
| `cart-cleaner` | `pekko-streams` | the sample pipeline's cleaner, with a fingerprinted inlet |
| `cart-counter` | `flink` | the sample pipeline's counter, above |

Each SDK's test suite declares the fixtures of its engine in its own language and asserts the
written bytes equal the fixture. `descriptor`'s own suite parses each fixture back and writes it
unchanged.

`fixtures/contracts/` is ankka's set of schema documents and their fingerprints, copied from the
tag its README names, so both canonicalisers are held to ankka's.

# Contract: the descriptor, version 2.0

The file an SDK writes from a streamlet's declaration, and the only thing `flow` and the operator
know about a streamlet. Canonical JSON as `descriptor/DESCRIPTOR.md` defines it: keys sorted,
two-space indent, a trailing newline, snake_case, a field at its default omitted, ports sorted by
name, parameters by key.

```json
{
  "descriptor_version": "2.0",
  "sdk": {
    "name": "ankka-flow-flink",
    "version": "0.6.0"
  },
  "streamlet": {
    "config_parameters": [
      {
        "default_value": "1m",
        "key": "window",
        "type": "DURATION"
      }
    ],
    "engine": "flink",
    "flink": {
      "entry_class": "com.example.CartCounter",
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
          "fingerprint": "sha256:9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08",
          "name": "cart-counts.v1"
        },
        "name": "out"
      }
    ]
  }
}
```

| Field | Rule |
|---|---|
| `descriptor_version` | `MAJOR.MINOR`; `flow` and the operator accept major 2 |
| `streamlet.engine` | required: `pekko-streams` or `flink` |
| `streamlet.flink` | required when the engine is `flink`, refused otherwise; `entry_class` is the class whose `main` runs the job; `version` is Flink's `MAJOR.MINOR` |
| `contract.name` | required; `[a-z0-9][a-z0-9._-]{0,98}[a-z0-9]` |
| `contract.fingerprint` | optional; `sha256:` and the lowercase hex SHA-256 of the schema document under RFC 8785, as ankka computes it |
| `config_parameters[].type` | `STRING` (omitted), `INTEGER`, `DOUBLE`, `BOOLEAN`, `DURATION`, `MEMORY_SIZE` |

Refusals, by `flow verify`, `flow generate`, `flow local` and the operator alike:

| Descriptor | Message |
|---|---|
| no `engine`, or a `protocol_version` field | `<file>: a descriptor must name an engine (pekko-streams, flink)` |
| an unknown engine | `<file>: engine 'spark' is not one flow knows (pekko-streams, flink)` |
| `flink` missing or misplaced | `<file>: a flink streamlet's descriptor has a flink section, and no other has` |
| major version not 2 | `<file>: descriptor version 3.0 is not one this flow reads (2.x)` |

An SDK's main writes it: `<main> --descriptor <path>`, and `--descriptor-check <path>` exits 1
with a diff when the file differs from the declaration. Fixtures:
`descriptor/fixtures/descriptors/*.json`, which each SDK reproduces byte for byte.

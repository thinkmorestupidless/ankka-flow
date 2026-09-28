# Descriptor

> The descriptor file a streamlet's SDK writes from its declaration — its fields, the canonical JSON every SDK produces byte for byte, contract fingerprints, and the validation rules.

Source: https://flow.ankka.cloud/reference/descriptor/
A streamlet's descriptor is its declaration — name, inlets, outlets, contracts and parameters — as a
JSON file, conventionally `flow/descriptor.json`. The streamlet's SDK writes it at build time; it is
never written by hand. It is the protocol's discovery `Spec` message in canonical form, so the same
declaration produces the same bytes in every language.

It is read in three places:

- `flow verify` and `flow generate` check a blueprint against the descriptors of its streamlets.
- `flow generate` copies each descriptor's `streamlet` object into the `AnkkaFlow` resource, and the
  operator mounts it into the sidecar.
- The sidecar compares the process's answer to `Discover` with the deployed descriptor's `streamlet`
  object, field by field, and refuses to start on any difference.

## Fields

| field | meaning |
|---|---|
| `protocol_version` | `MAJOR.MINOR` of the streamlet protocol the SDK speaks, `1.0` |
| `sdk.name`, `sdk.version` | the SDK that wrote the file, such as `ankka-flow-python` |
| `streamlet.name` | the name a blueprint refers to |
| `streamlet.description` | free text |
| `streamlet.inlets[]`, `streamlet.outlets[]` | ports: `name` and `contract` |
| `contract.format` | `json`, the only format |
| `contract.schema_name` | the contract's name, such as `cart-events.v1` |
| `contract.fingerprint` | Base64 of the SHA-256 of the UTF-8 schema name, standard alphabet, padded |
| `streamlet.config_parameters[]` | parameters: `key`, `description`, `type`, `default_value` |
| `type` | `STRING`, `INTEGER`, `DOUBLE`, `BOOLEAN`, `DURATION` or `MEMORY_SIZE` |
| `default_value` | the default as text; absent when the parameter must be set at deploy time |

The fingerprint is computed from the schema's name, not from any schema content: two ports connect
when their format and fingerprint are equal. See [Contracts](../concepts/contracts.md).

## Canonical JSON

1. Field names are the proto field names, in snake_case: `protocol_version`, `schema_name`,
   `config_parameters`, `default_value`.
2. Object keys are sorted lexicographically, in byte order, at every level.
3. `inlets` and `outlets` are sorted by `name`, and `config_parameters` by `key`. Declaration order is
   never meaningful.
4. Enums are written as their names (`"INTEGER"`), never as numbers.
5. A field at its proto3 default is omitted: an empty string, `0`, `false`, an empty list, an unset
   optional field. A `STRING` parameter therefore has no `type` field, and a parameter with no default
   has no `default_value`.
6. Two-space indentation, `": "` and `,` plus newline as separators, LF line endings, UTF-8 with no
   byte-order mark, and exactly one trailing newline.

In Python this is
`json.dumps(MessageToDict(spec, preserving_proto_field_name=True), sort_keys=True, indent=2, ensure_ascii=False) + "\n"`.

## Example

The cart router's descriptor: one inlet, two outlets sharing its contract, one integer parameter.

```json
{
  "protocol_version": "1.0",
  "sdk": {
    "name": "fixture",
    "version": "0.0.0"
  },
  "streamlet": {
    "config_parameters": [
      {
        "default_value": "100",
        "description": "Carts with a total above this go to the review outlet.",
        "key": "review-threshold",
        "type": "INTEGER"
      }
    ],
    "description": "Routes cart events to the valid or review outlet.",
    "inlets": [
      {
        "contract": {
          "fingerprint": "nXhoFwNZSB7DKScFuZUtZ1gnAYkIxadumsXNHWwbLfM=",
          "format": "json",
          "schema_name": "cart-events.v1"
        },
        "name": "in"
      }
    ],
    "name": "cart-router",
    "outlets": [
      {
        "contract": {
          "fingerprint": "nXhoFwNZSB7DKScFuZUtZ1gnAYkIxadumsXNHWwbLfM=",
          "format": "json",
          "schema_name": "cart-events.v1"
        },
        "name": "review"
      },
      {
        "contract": {
          "fingerprint": "nXhoFwNZSB7DKScFuZUtZ1gnAYkIxadumsXNHWwbLfM=",
          "format": "json",
          "schema_name": "cart-events.v1"
        },
        "name": "valid"
      }
    ]
  }
}
```

This is the fixture every SDK must reproduce, so its `sdk` block is pinned to
`{"name": "fixture", "version": "0.0.0"}`. A descriptor written for a real streamlet names its SDK
and version.

## Validation

The CLI and the sidecar apply the same rules and report every problem at once:

- `streamlet.name` is 1 to 63 of `[a-z0-9-]` and does not start or end with `-`.
- Port names match `[a-z][a-z0-9-]{0,62}` and are unique across inlets and outlets together.
- `contract.format` is `json`, and `contract.fingerprint` equals the fingerprint of `schema_name`.
- Parameter keys match `[a-z][a-z0-9-]*` and are unique; a `default_value` parses as its `type`, with
  `DURATION` and `MEMORY_SIZE` in HOCON's duration and size syntax.
- `protocol_version` is `MAJOR.MINOR`, both unsigned integers.

## Fixtures

The repository's
[`protocol/fixtures/descriptors`](https://github.com/thinkmorestupidless/ankka-flow/blob/main/protocol/fixtures/descriptors)
holds the exact bytes each declaration in `protocol/fixtures/declarations` must produce. Every SDK
declares each streamlet in its own language and asserts that its writer produces these bytes.

| fixture | declares |
|---|---|
| `minimal` | one inlet, one outlet, no parameters, no description |
| `cart-router` | the example on this page |
| `every-type` | one parameter of every type, one of them required, Unicode in a description |
| `many-ports` | five inlets and five outlets declared out of order, to prove sorting |
| `sink` | inlets only, no outlets |
| `conformance` | the reference streamlet of the conformance suite |

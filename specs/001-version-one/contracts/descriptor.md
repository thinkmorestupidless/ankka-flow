# Contract: the descriptor file (`protocol/DESCRIPTOR.md`)

**Feature**: [spec.md](../spec.md) | **Protocol**: [protocol.md](./protocol.md) | **Research**: R4, R5

A streamlet's descriptor is the discovery `Spec` message written as JSON by the SDK at build time
(FR-001, FR-002). The same declaration produces the same bytes in every language, checked by the
fixtures. It is never written by hand; the CLI refuses one whose fingerprints do not match their
schema names.

## Canonical JSON

1. Field names are the proto field names (snake_case): `protocol_version`, `schema_name`,
   `config_parameters`, `default_value`.
2. Object keys are sorted lexicographically (byte order) at every level.
3. `inlets` and `outlets` are sorted by `name`; `config_parameters` by `key`. The SDK sorts; a
   declaration order is never meaningful.
4. Enums are their names (`"INTEGER"`), never numbers.
5. A field at its proto3 default is omitted (`""`, `0`, `false`, an empty list, an unset
   `optional`). `ConfigType.STRING` is therefore omitted, which is the rule, not a bug.
6. Two-space indentation, `": "` and `,\n` separators, LF line endings, UTF-8, exactly one trailing
   newline, no BOM.

Python: `json.dumps(MessageToDict(spec, preserving_proto_field_name=True), sort_keys=True,
indent=2, ensure_ascii=False) + "\n"`. Scala: `protocol`'s `DescriptorJson.write(spec)`, which is
tested against every fixture.

## Example: the cart router

`protocol/fixtures/declarations/cart-router.md` describes the declaration; every SDK's sample
declares it and `protocol/fixtures/descriptors/cart-router.json` is the expected output:

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

A fixture's `sdk` block is pinned to `{"name": "fixture", "version": "0.0.0"}` so that one file is
the expected output of every SDK. Outside the fixture test an SDK writes its real name and version.

## Fixtures

| fixture | declares |
|---|---|
| `minimal` | one inlet, one outlet, no parameters, no description |
| `cart-router` | the sample above |
| `every-type` | one parameter of every `ConfigType`, one required (no default), unicode in a description |
| `many-ports` | five inlets and five outlets declared out of order, to prove sorting |
| `sink` | inlets only, no outlets |
| `conformance` | the reference streamlet of contracts/conformance.md |

Each SDK's test suite declares each fixture's streamlet in its own language and asserts the
written bytes equal the fixture. The Scala `protocol` test does the same for `DescriptorJson` and
also parses each fixture back and re-writes it unchanged (FR-002, S5.2).

## Validation (the CLI and the sidecar apply the same rules)

- `streamlet.name` matches `[a-z0-9-]{1,63}` and does not start or end with `-`.
- Port names match `[a-z][a-z0-9-]{0,62}` and are unique across inlets and outlets together.
- `contract.format` is `json` (version one); `contract.fingerprint` equals
  `Base64(SHA-256(UTF-8(schema_name)))` with standard alphabet and padding.
- Parameter keys match `[a-z][a-z0-9-]*` and are unique; a `default_value` parses as its `type`
  (`DURATION` as HOCON durations, `MEMORY_SIZE` as HOCON sizes).
- `protocol_version` is `MAJOR.MINOR` with both parts unsigned integers.

# conformance

The reference streamlet every SDK ships for the conformance suite.

- name: `conformance`
- description: `The conformance reference streamlet.`
- inlet `in`: json, schema name `conformance.v1`
- inlet `side`: json, schema name `conformance-side.v1`
- outlet `out`: json, schema name `conformance.v1`
- outlet `other`: json, schema name `conformance-other.v1`
- parameter `mode`: STRING, default `echo`, description `Unused by the suite; proves a string parameter arrives.`
- parameter `factor`: INTEGER, default `1`, description `How many times the multiply key emits.`

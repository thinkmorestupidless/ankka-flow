# every-type

- name: `every-type`
- description: `Every parameter type, and "unicode": café ✓` (a straight double quote around
  `unicode`, a precomposed é, U+2713 check mark)
- inlet `in`: json, schema name `every.v1`
- parameters, declared in this order:
  - `a-string`: STRING, default `hello`, description `A string.`
  - `an-integer`: INTEGER, default `42`, description `An integer.`
  - `a-double`: DOUBLE, default `0.5`, description `A double.`
  - `a-boolean`: BOOLEAN, default `true`, description `A boolean.`
  - `a-duration`: DURATION, default `100 ms`, description `A duration.`
  - `a-memory-size`: MEMORY_SIZE, default `1 MiB`, description `A memory size.`
  - `required`: STRING, no default, description `Required: no default, so it must be set at deploy time.`

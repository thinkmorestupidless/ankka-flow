# ankka-flow

Streaming pipelines beside [ankka](https://github.com/thinkmorestupidless/ankka): streamlets with
typed inlets and outlets, wired by a blueprint over Kafka topics, each streamlet's logic in any
language, in its own container, with a Pekko sidecar beside it that owns everything Kafka.

It descends from [Cloudflow](https://github.com/lightbend/cloudflow) by way of the
[thinkmorestupidless fork](https://github.com/thinkmorestupidless/cloudflow), which moved it to
Apache Pekko. Neither is a dependency; what was proven there is carried over piece by piece with
its tests.

Nothing is built yet. Start with `specs/001-version-one/spec.md` (what version one does) and
`docs/design/version-one.md` (how).

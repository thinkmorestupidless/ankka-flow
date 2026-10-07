"""Adds a greeting to each JSON object it reads, keeping the record's key and headers."""

from collections.abc import Iterable

from ankka_flow import Batch, Emit, JsonInlet, JsonOutlet, StringParameter, Streamlet, json


class {{class}}(Streamlet):
    name = "{{name}}"
    description = "Adds a greeting to each JSON object it reads."
    inlet = JsonInlet("in", schema_name="{{name}}.v1")
    out = JsonOutlet("out", schema_name="{{name}}.v1")
    greeting = StringParameter(
        "greeting",
        default="hello, ankka-flow",
        description="The greeting added to each record.",
    )

    def process(self, batch: Batch) -> Iterable[Emit]:
        text = self.config[self.greeting]
        for record in batch:
            # The SDK decodes nothing; reading the value as JSON is this streamlet's choice. A value
            # that is not a JSON object fails the batch, and the sidecar delivers it again.
            event = json.loads(record.value)
            if not isinstance(event, dict):
                raise ValueError(f"not a JSON object: {record.value!r}")
            event["greeting"] = text
            yield self.out.emit(record, value=json.dumps(event))  # same key, same headers

from collections.abc import Iterable

from ankka_flow import Batch, Emit, JsonInlet, JsonOutlet, Streamlet


class Echo(Streamlet):
    name = "{{name}}"
    description = "Forwards every record unchanged."
    inlet = JsonInlet("in", schema_name="{{name}}.input.v1")
    out = JsonOutlet("out", schema_name="{{name}}.input.v1")

    def process(self, batch: Batch) -> Iterable[Emit]:
        for record in batch:
            yield self.out.emit(record)

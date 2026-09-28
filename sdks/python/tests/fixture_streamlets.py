"""The six declarations of protocol/fixtures/declarations, in Python."""

from __future__ import annotations

from collections.abc import Iterable

from ankka_flow import (
    Batch,
    BooleanParameter,
    DoubleParameter,
    DurationParameter,
    Emit,
    IntegerParameter,
    JsonInlet,
    JsonOutlet,
    MemorySizeParameter,
    StringParameter,
    Streamlet,
)


class _NoOp(Streamlet):
    name = "no-op"

    def process(self, batch: Batch) -> Iterable[Emit]:
        return ()


class Minimal(_NoOp):
    name = "minimal"
    inlet = JsonInlet("in", schema_name="minimal.v1")
    outlet = JsonOutlet("out", schema_name="minimal.v1")


class CartRouter(_NoOp):
    name = "cart-router"
    description = "Routes cart events to the valid or review outlet."
    inlet = JsonInlet("in", schema_name="cart-events.v1")
    valid = JsonOutlet("valid", schema_name="cart-events.v1")
    review = JsonOutlet("review", schema_name="cart-events.v1")
    threshold = IntegerParameter(
        "review-threshold",
        default=100,
        description="Carts with a total above this go to the review outlet.",
    )


class EveryType(_NoOp):
    name = "every-type"
    description = 'Every parameter type, and "unicode": café ✓'
    inlet = JsonInlet("in", schema_name="every.v1")
    a_string = StringParameter("a-string", default="hello", description="A string.")
    an_integer = IntegerParameter("an-integer", default=42, description="An integer.")
    a_double = DoubleParameter("a-double", default=0.5, description="A double.")
    a_boolean = BooleanParameter("a-boolean", default=True, description="A boolean.")
    a_duration = DurationParameter("a-duration", default="100 ms", description="A duration.")
    a_memory_size = MemorySizeParameter("a-memory-size", default="1 MiB", description="A memory size.")
    required = StringParameter(
        "required", description="Required: no default, so it must be set at deploy time."
    )


class ManyPorts(_NoOp):
    name = "many-ports"
    in_e = JsonInlet("in-e", schema_name="many.v1")
    in_c = JsonInlet("in-c", schema_name="many.v1")
    in_a = JsonInlet("in-a", schema_name="many.v1")
    in_d = JsonInlet("in-d", schema_name="many.v1")
    in_b = JsonInlet("in-b", schema_name="many.v1")
    out_3 = JsonOutlet("out-3", schema_name="many.v1")
    out_1 = JsonOutlet("out-1", schema_name="many.v1")
    out_5 = JsonOutlet("out-5", schema_name="many.v1")
    out_2 = JsonOutlet("out-2", schema_name="many.v1")
    out_4 = JsonOutlet("out-4", schema_name="many.v1")


class Sink(_NoOp):
    name = "sink"
    description = "Consumes cart events and writes them elsewhere."
    inlet = JsonInlet("in", schema_name="cart-events.v1")


class Conformance(_NoOp):
    name = "conformance"
    description = "The conformance reference streamlet."
    inlet = JsonInlet("in", schema_name="conformance.v1")
    side = JsonInlet("side", schema_name="conformance-side.v1")
    out = JsonOutlet("out", schema_name="conformance.v1")
    other = JsonOutlet("other", schema_name="conformance-other.v1")
    mode = StringParameter(
        "mode", default="echo", description="Unused by the suite; proves a string parameter arrives."
    )
    factor = IntegerParameter("factor", default=1, description="How many times the multiply key emits.")


FIXTURES: dict[str, type[Streamlet]] = {
    "minimal": Minimal,
    "cart-router": CartRouter,
    "every-type": EveryType,
    "many-ports": ManyPorts,
    "sink": Sink,
    "conformance": Conformance,
}

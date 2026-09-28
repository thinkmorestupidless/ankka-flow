"""The streamlet base class and the one function that runs a batch through it."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterable, Iterator
from typing import Any, ClassVar

from .parameters import Config, Parameter
from .ports import JsonInlet, JsonOutlet, Port
from .records import Batch, Emit


class Streamlet(ABC):
    """Declare `name`, ports and parameters as class attributes, and implement `process`.

    `process` is called once per batch on a worker thread. The sidecar keeps one batch in flight
    per partition, so it never runs twice for one partition at once, but it may run concurrently
    for different partitions. Returning acknowledges the batch; raising fails it; yielding nothing
    for a record skips it.
    """

    name: ClassVar[str]
    description: ClassVar[str] = ""

    _inlets: ClassVar[list[JsonInlet]] = []
    _outlets: ClassVar[list[JsonOutlet]] = []
    _parameters: ClassVar[list[Parameter[Any]]] = []

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        seen: dict[str, object] = {}
        for klass in reversed(cls.__mro__):
            for attr, value in vars(klass).items():
                if isinstance(value, (Port, Parameter)):
                    seen[attr] = value
        values = list(seen.values())
        cls._inlets = [v for v in values if isinstance(v, JsonInlet)]
        cls._outlets = [v for v in values if isinstance(v, JsonOutlet)]
        cls._parameters = [v for v in values if isinstance(v, Parameter)]
        ports = [p.name for p in cls._inlets + cls._outlets]
        dupes = sorted({n for n in ports if ports.count(n) > 1})
        if dupes:
            raise TypeError(f"{cls.__name__}: port(s) declared more than once: {', '.join(dupes)}")
        keys = [p.key for p in cls._parameters]
        dupe_keys = sorted({k for k in keys if keys.count(k) > 1})
        if dupe_keys:
            raise TypeError(f"{cls.__name__}: parameter(s) declared more than once: {', '.join(dupe_keys)}")

    def __init__(self) -> None:
        self.config: Config = Config.resolve(self.parameters(), {})

    @classmethod
    def inlets(cls) -> list[JsonInlet]:
        return list(cls._inlets)

    @classmethod
    def outlets(cls) -> list[JsonOutlet]:
        return list(cls._outlets)

    @classmethod
    def parameters(cls) -> list[Parameter[Any]]:
        return list(cls._parameters)

    def configure(self, values: dict[str, object]) -> None:
        self.config = Config.resolve(self.parameters(), values)

    @abstractmethod
    def process(self, batch: Batch) -> Iterable[Emit]:
        """Answer a batch with the emits it produces."""


class UndeclaredOutlet(Exception):
    def __init__(self, outlet: str) -> None:
        super().__init__(f"emit to undeclared outlet '{outlet}'")
        self.outlet = outlet


def run_batch(streamlet: Streamlet, batch: Batch) -> Iterator[Emit]:
    """Run `process`, checking every emit names a declared outlet. Shared by server and harness."""
    declared = {o.name for o in streamlet.outlets()}
    result = streamlet.process(batch)
    if result is None:
        return
    for emit in result:
        if not isinstance(emit, Emit):
            raise TypeError(f"process yielded {type(emit).__name__}, not an Emit")
        if emit.outlet not in declared:
            raise UndeclaredOutlet(emit.outlet)
        yield emit

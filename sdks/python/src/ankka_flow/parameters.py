"""Configuration parameters, typed, with HOCON-style durations and memory sizes."""

from __future__ import annotations

import math
import re
from collections.abc import Mapping
from datetime import timedelta
from typing import Any, Generic, TypeVar, cast

T = TypeVar("T")

# The ConfigType enum of discovery.proto, by name.
STRING, INTEGER, DOUBLE, BOOLEAN, DURATION, MEMORY_SIZE = (
    "STRING",
    "INTEGER",
    "DOUBLE",
    "BOOLEAN",
    "DURATION",
    "MEMORY_SIZE",
)

_NUMBER_UNIT = re.compile(r"^\s*([+-]?(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?)\s*([A-Za-z]*)\s*$")

_DURATION_UNITS: dict[str, float] = {}
for _names, _ns in (
    (("ns", "nano", "nanos", "nanosecond", "nanoseconds"), 1),
    (("us", "micro", "micros", "microsecond", "microseconds"), 1_000),
    (("", "ms", "milli", "millis", "millisecond", "milliseconds"), 1_000_000),
    (("s", "second", "seconds"), 1_000_000_000),
    (("m", "minute", "minutes"), 60 * 1_000_000_000),
    (("h", "hour", "hours"), 3600 * 1_000_000_000),
    (("d", "day", "days"), 86400 * 1_000_000_000),
):
    for _n in _names:
        _DURATION_UNITS[_n] = _ns


def _memory_units() -> dict[str, int]:
    units: dict[str, int] = {"": 1, "B": 1, "b": 1, "byte": 1, "bytes": 1}
    prefixes = [
        ("K", "k", "kilo", "kibi"),
        ("M", "m", "mega", "mebi"),
        ("G", "g", "giga", "gibi"),
        ("T", "t", "tera", "tebi"),
        ("P", "p", "peta", "pebi"),
        ("E", "e", "exa", "exbi"),
    ]
    for power, (upper, lower, decimal, binary) in enumerate(prefixes, start=1):
        for name in (upper, lower, upper + "i", upper + "iB", binary + "byte", binary + "bytes"):
            units[name] = 1024**power
        for name in (upper + "B" if upper != "K" else "kB", decimal + "byte", decimal + "bytes"):
            units[name] = 1000**power
    return units


_MEMORY_UNITS = _memory_units()


def parse_duration(text: str) -> timedelta:
    """A HOCON duration: `100 ms`, `5m`, `1.5 s`, a bare number is milliseconds."""
    m = _NUMBER_UNIT.match(text)
    if not m or m.group(2) not in _DURATION_UNITS:
        raise ValueError(f"'{text}' is not a duration")
    nanos = float(m.group(1)) * _DURATION_UNITS[m.group(2)]
    return timedelta(microseconds=nanos / 1000)


def parse_memory_size(text: str) -> int:
    """A HOCON memory size in bytes: `1 MiB`, `512k`, `10MB`, a bare number is bytes."""
    m = _NUMBER_UNIT.match(text)
    if not m or m.group(2) not in _MEMORY_UNITS:
        raise ValueError(f"'{text}' is not a memory size")
    size = float(m.group(1)) * _MEMORY_UNITS[m.group(2)]
    if size < 0 or not math.isfinite(size):
        raise ValueError(f"'{text}' is not a memory size")
    return int(size)


class Parameter(Generic[T]):
    """A declared configuration parameter. `config[param]` returns its value as `T`."""

    type_name: str = STRING

    def __init__(self, key: str, *, default: T | str | None = None, description: str = "") -> None:
        self.key = key
        self.description = description
        self.default_value: str = "" if default is None else self.render(default)
        if self.default_value:
            self.parse(self.default_value)  # refuse a default that is not a value of the type

    def render(self, value: T | str) -> str:
        return str(value)

    def parse(self, text: str) -> T:
        raise NotImplementedError

    def from_json(self, value: object) -> T:
        """A value as `Start.config_json` carries it."""
        if isinstance(value, str):
            return self.parse(value)
        return self.parse(self.render(cast(T, value)))

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.key!r})"


class StringParameter(Parameter[str]):
    type_name = STRING

    def parse(self, text: str) -> str:
        return text


class IntegerParameter(Parameter[int]):
    type_name = INTEGER

    def parse(self, text: str) -> int:
        try:
            return int(text.strip())
        except ValueError:
            raise ValueError(f"'{text}' is not an integer") from None

    def from_json(self, value: object) -> int:
        if isinstance(value, bool):
            raise ValueError(f"'{value}' is not an integer")
        if isinstance(value, int):
            return value
        if isinstance(value, float) and value.is_integer():
            return int(value)
        return self.parse(str(value))


class DoubleParameter(Parameter[float]):
    type_name = DOUBLE

    def parse(self, text: str) -> float:
        try:
            return float(text.strip())
        except ValueError:
            raise ValueError(f"'{text}' is not a double") from None

    def from_json(self, value: object) -> float:
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return float(value)
        return self.parse(str(value))


class BooleanParameter(Parameter[bool]):
    type_name = BOOLEAN

    def render(self, value: bool | str) -> str:
        if isinstance(value, bool):
            return "true" if value else "false"
        return value

    def parse(self, text: str) -> bool:
        t = text.strip()
        if t == "true":
            return True
        if t == "false":
            return False
        raise ValueError(f"'{text}' is not a boolean")

    def from_json(self, value: object) -> bool:
        if isinstance(value, bool):
            return value
        return self.parse(str(value))


class DurationParameter(Parameter[timedelta]):
    type_name = DURATION

    def render(self, value: timedelta | str) -> str:
        if isinstance(value, timedelta):
            micros = value // timedelta(microseconds=1)
            return f"{micros // 1000} ms" if micros % 1000 == 0 else f"{micros} us"
        return value

    def parse(self, text: str) -> timedelta:
        return parse_duration(text)


class MemorySizeParameter(Parameter[int]):
    type_name = MEMORY_SIZE

    def render(self, value: int | str) -> str:
        return str(value)

    def parse(self, text: str) -> int:
        return parse_memory_size(text)

    def from_json(self, value: object) -> int:
        if isinstance(value, int) and not isinstance(value, bool):
            return value
        return self.parse(str(value))


class Config:
    """A streamlet's resolved configuration, typed by its parameters."""

    def __init__(self, values: Mapping[str, Any]) -> None:
        self._values = dict(values)

    @staticmethod
    def resolve(parameters: list[Parameter[Any]], values: Mapping[str, object]) -> Config:
        """Declared defaults overlaid by `values` (JSON values, strings, or typed Python values).

        A key not declared is refused. A parameter with no default and no value stays absent, and
        reading it raises `KeyError`.
        """
        by_key = {p.key: p for p in parameters}
        unknown = sorted(set(values) - set(by_key))
        if unknown:
            raise KeyError(f"no parameter declared for {', '.join(unknown)}")
        resolved: dict[str, Any] = {}
        for p in parameters:
            if p.key in values:
                v = values[p.key]
                resolved[p.key] = v if _already_typed(p, v) else p.from_json(v)
            elif p.default_value:
                resolved[p.key] = p.parse(p.default_value)
        return Config(resolved)

    def __getitem__(self, param: Parameter[T]) -> T:
        try:
            return cast(T, self._values[param.key])
        except KeyError:
            raise KeyError(f"parameter '{param.key}' has no default and no value") from None

    def get(self, key: str) -> Any:
        return self._values.get(key)

    def as_dict(self) -> dict[str, Any]:
        return dict(self._values)


def _already_typed(p: Parameter[Any], v: object) -> bool:
    return (isinstance(p, DurationParameter) and isinstance(v, timedelta)) or (
        isinstance(p, BooleanParameter) and isinstance(v, bool)
    )

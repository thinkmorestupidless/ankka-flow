"""ankka-flow: write streamlets in Python. The sidecar owns Kafka; this package owns the protocol."""

from . import json
from .descriptor import PROTOCOL_VERSION
from .parameters import (
    BooleanParameter,
    Config,
    DoubleParameter,
    DurationParameter,
    IntegerParameter,
    MemorySizeParameter,
    Parameter,
    StringParameter,
)
from .ports import JsonInlet, JsonOutlet
from .records import Batch, Emit, Record
from .server import serve
from .streamlet import Streamlet

__version__ = "0.0.0"

__all__ = [
    "PROTOCOL_VERSION",
    "Batch",
    "BooleanParameter",
    "Config",
    "DoubleParameter",
    "DurationParameter",
    "Emit",
    "IntegerParameter",
    "JsonInlet",
    "JsonOutlet",
    "MemorySizeParameter",
    "Parameter",
    "Record",
    "StringParameter",
    "Streamlet",
    "json",
    "serve",
]

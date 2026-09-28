"""The descriptor: the discovery Spec, and its canonical JSON (protocol/DESCRIPTOR.md)."""

from __future__ import annotations

import json
import re
from typing import Any

from google.protobuf.json_format import MessageToDict

from ._proto.ankka.flow.v1 import discovery_pb2 as d
from .parameters import Parameter
from .ports import Port, fingerprint
from .streamlet import Streamlet

PROTOCOL_VERSION = "1.0"
SDK_NAME = "ankka-flow-python"

_STREAMLET_NAME = re.compile(r"^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$")
_PORT_NAME = re.compile(r"^[a-z][a-z0-9-]{0,62}$")
_PARAM_KEY = re.compile(r"^[a-z][a-z0-9-]*$")


def _sdk_version() -> str:
    from . import __version__

    return __version__


def spec_for(
    streamlet: Streamlet | type[Streamlet],
    *,
    sdk_name: str = SDK_NAME,
    sdk_version: str | None = None,
) -> d.Spec:
    """The Spec a streamlet declares: ports sorted by name, parameters by key."""
    cls = streamlet if isinstance(streamlet, type) else type(streamlet)

    def port(p: Port) -> d.Port:
        return d.Port(
            name=p.name,
            contract=d.Contract(format=p.format, schema_name=p.schema_name, fingerprint=p.fingerprint),
        )

    def param(p: Parameter[Any]) -> d.ConfigParameter:
        return d.ConfigParameter(
            key=p.key,
            description=p.description,
            type=p.type_name,
            default_value=p.default_value,
        )

    return d.Spec(
        protocol_version=PROTOCOL_VERSION,
        sdk=d.SdkInfo(name=sdk_name, version=_sdk_version() if sdk_version is None else sdk_version),
        streamlet=d.StreamletDescriptor(
            name=cls.name,
            description=cls.description,
            inlets=[port(p) for p in sorted(cls.inlets(), key=lambda p: p.name)],
            outlets=[port(p) for p in sorted(cls.outlets(), key=lambda p: p.name)],
            config_parameters=[param(p) for p in sorted(cls.parameters(), key=lambda p: p.key)],
        ),
    )


def write(spec: d.Spec) -> str:
    """Canonical JSON: the bytes every SDK writes for the same declaration."""
    as_dict = MessageToDict(spec, preserving_proto_field_name=True)
    return json.dumps(as_dict, sort_keys=True, indent=2, ensure_ascii=False) + "\n"


def validate(spec: d.Spec) -> list[str]:
    """The rules of DESCRIPTOR.md *Validation*; the CLI and the sidecar apply the same ones."""
    s = spec.streamlet
    problems: list[str] = []
    if not _STREAMLET_NAME.match(s.name):
        problems.append(
            f"streamlet name '{s.name}' must be 1-63 of [a-z0-9-], not starting or ending with '-'"
        )
    ports = [("inlet", p) for p in s.inlets] + [("outlet", p) for p in s.outlets]
    names = [p.name for _, p in ports]
    for kind, p in ports:
        if not _PORT_NAME.match(p.name):
            problems.append(f"{kind} name '{p.name}' must match [a-z][a-z0-9-]{{0,62}}")
        c = p.contract
        if c.format != "json":
            problems.append(f"{kind} '{p.name}' uses format '{c.format}', which this version does not support")
        if c.fingerprint != fingerprint(c.schema_name):
            problems.append(f"{kind} '{p.name}' fingerprint does not match schema name '{c.schema_name}'")
    for n in sorted({n for n in names if names.count(n) > 1}):
        problems.append(f"port '{n}' is declared {names.count(n)} times")
    for param in s.config_parameters:
        if not _PARAM_KEY.match(param.key):
            problems.append(f"parameter key '{param.key}' must match [a-z][a-z0-9-]*")
    return problems

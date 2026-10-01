"""Graph deltas (`ankka.graph-delta.v1`): the records the built-in merge sink reads.

A delta is a statement of one graph element's whole state: a node, an edge, or a tombstone marking
one deleted, with the element's id and a version that rises with its source's history. Its record
key is its **element key** — `node:<id>` or `edge:<id>` — because a compacted topic keeps the last
record per key, and the sink refuses a delta under any other key.

`GraphDeltaOutlet` builds the record, key included, so a mapper never chooses a key:

    class CheckoutGraph(Streamlet):
        deltas = GraphDeltaOutlet("deltas")

        def process(self, batch):
            for record in batch:
                yield self.deltas.node(record, id="cart:1", version=7, labels=["Cart"])

`read` parses an emitted record back into a `Delta`, for a mapper's tests.
"""

from __future__ import annotations

import math
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any

from . import json
from .ports import JsonOutlet
from .records import Emit, Record

__all__ = [
    "SCHEMA_NAME",
    "Delta",
    "GraphDeltaOutlet",
    "PropertyValue",
    "edge_key",
    "node_key",
    "read",
]

SCHEMA_NAME = "ankka.graph-delta.v1"

Scalar = str | int | float | bool
PropertyValue = Scalar | list[str] | list[int] | list[float] | list[bool]

_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_RESERVED = frozenset({"id", "_version", "_deleted"})
_INT64_MIN, _INT64_MAX = -(2**63), 2**63 - 1


def node_key(id: str) -> bytes:  # noqa: A002 - `id` is the contract's word
    """The record key of a node's deltas: `node:<id>`."""
    return f"node:{id}".encode()


def edge_key(id: str) -> bytes:  # noqa: A002
    """The record key of an edge's deltas: `edge:<id>`."""
    return f"edge:{id}".encode()


@dataclass(frozen=True)
class Delta:
    """One graph delta, as `read` returns it.

    `kind` is `"node"`, `"edge"` or `"tombstone"`; `element` is `"node"` or `"edge"`, the kind of
    element the delta describes or marks. `type`, `from_id` and `to_id` are set for an edge and an
    edge's tombstone. `key` is the element key the record carries.
    """

    kind: str
    element: str
    id: str
    version: int
    labels: tuple[str, ...] = ()
    type: str | None = None
    from_id: str | None = None
    to_id: str | None = None
    properties: dict[str, Any] = field(default_factory=dict)
    key: bytes = b""


# ── validation: what the sink would refuse, refused here ────────────────────────────────────────


def _id(name: str, value: object) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{name} must be a non-empty string, not {value!r}")
    return value


def _version(value: object) -> int:
    # A bool is an int to Python and is never a version.
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= _INT64_MAX:
        raise ValueError(f"version must be a non-negative integer that fits 64 bits, not {value!r}")
    return value


def _identifier(name: str, value: object) -> str:
    if not isinstance(value, str) or not _IDENTIFIER.fullmatch(value):
        raise ValueError(f"{name} must be an identifier ([A-Za-z_][A-Za-z0-9_]*), not {value!r}")
    return value


def _labels(value: object) -> list[str]:
    if isinstance(value, (str, bytes)) or not isinstance(value, Iterable):
        raise ValueError(f"labels must be a list of identifiers, not {value!r}")
    return [_identifier("a label", label) for label in value]


def _scalar_kind(value: object) -> str | None:
    """How the sink reads a scalar: text, flag, integer or decimal; None when it refuses it."""
    if isinstance(value, bool):
        return "flag"
    if isinstance(value, str):
        return "text"
    if isinstance(value, int):
        return "integer" if _INT64_MIN <= value <= _INT64_MAX else None
    if isinstance(value, float):
        if not math.isfinite(value):
            return None
        if value.is_integer():  # written `2.0`, which the sink reads as the integer 2
            return "integer" if _INT64_MIN <= value <= _INT64_MAX else None
        return "decimal"
    return None


def _properties(value: object) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise ValueError(f"properties must be a mapping, not {value!r}")
    out: dict[str, Any] = {}
    for name, v in value.items():
        if not isinstance(name, str):
            raise ValueError(f"property name {name!r} must be a string")
        if name in _RESERVED:
            raise ValueError(f"property '{name}' is reserved")
        if isinstance(v, (list, tuple)):
            kinds = {_scalar_kind(item) for item in v}
            if not v or None in kinds or len(kinds) != 1:
                raise ValueError(
                    f"property '{name}' must be a scalar or a non-empty list of one kind of scalar, not {v!r}"
                )
            out[name] = list(v)
        elif _scalar_kind(v) is None:
            raise ValueError(
                f"property '{name}' must be a string, number, boolean "
                f"or a non-empty list of one of those, not {v!r}"
            )
        else:
            out[name] = v
    return out


# ── the outlet ───────────────────────────────────────────────────────────────────────────────────


class GraphDeltaOutlet(JsonOutlet):
    """An outlet of graph deltas. It builds each record with its element key; none is passed in.

    The `record` each method takes is the input the delta was derived from: its headers and offset
    are carried, so a delta is traceable to its source. `None` builds a record with no headers.
    """

    def __init__(self, name: str) -> None:
        super().__init__(name, schema_name=SCHEMA_NAME)

    def node(
        self,
        record: Record | None,
        *,
        id: str,  # noqa: A002
        version: int,
        labels: Iterable[str] = (),
        properties: Mapping[str, PropertyValue] | None = None,
    ) -> Emit:
        """The node's whole state at `version`: exactly these labels and these properties."""
        delta = {
            "kind": "node",
            "id": _id("id", id),
            "version": _version(version),
            "labels": _labels(labels),
            "properties": _properties(properties),
        }
        return self.emit(record, value=json.dumps(delta), key=node_key(id))

    def edge(
        self,
        record: Record | None,
        *,
        id: str,  # noqa: A002
        version: int,
        type: str,  # noqa: A002
        from_id: str,
        to_id: str,
        properties: Mapping[str, PropertyValue] | None = None,
    ) -> Emit:
        """The edge's whole state at `version`, from node `from_id` to node `to_id`."""
        delta = {
            "kind": "edge",
            "id": _id("id", id),
            "version": _version(version),
            "type": _identifier("type", type),
            "from": _id("from_id", from_id),
            "to": _id("to_id", to_id),
            "properties": _properties(properties),
        }
        return self.emit(record, value=json.dumps(delta), key=edge_key(id))

    def tombstone_node(self, record: Record | None, *, id: str, version: int) -> Emit:  # noqa: A002
        """Marks the node deleted at `version`. It stays in the graph, marked."""
        delta = {"kind": "tombstone", "element": "node", "id": _id("id", id), "version": _version(version)}
        return self.emit(record, value=json.dumps(delta), key=node_key(id))

    def tombstone_edge(
        self,
        record: Record | None,
        *,
        id: str,  # noqa: A002
        version: int,
        type: str,  # noqa: A002
        from_id: str,
        to_id: str,
    ) -> Emit:
        """Marks the edge deleted at `version`; its type and endpoints say where to find it."""
        delta = {
            "kind": "tombstone",
            "element": "edge",
            "id": _id("id", id),
            "version": _version(version),
            "type": _identifier("type", type),
            "from": _id("from_id", from_id),
            "to": _id("to_id", to_id),
        }
        return self.emit(record, value=json.dumps(delta), key=edge_key(id))


# ── reading, for tests ───────────────────────────────────────────────────────────────────────────


def _read_version(value: object) -> int:
    # The sink accepts any whole JSON number, so `1e3` (a float here) is version 1000.
    if isinstance(value, float) and value.is_integer():
        value = int(value)
    return _version(value)


def read(record: Record) -> Delta:
    """The delta a record carries. Raises `ValueError` when the record is not a delta, or when its
    key is not its element's key — what the sink would refuse."""
    try:
        body = json.loads(record.value)
    except ValueError as e:
        raise ValueError("not a JSON object") from e
    if not isinstance(body, dict):
        raise ValueError("not a JSON object")
    kind = body.get("kind")
    if not isinstance(kind, str):
        raise ValueError("kind missing")
    id_ = _id("id", body.get("id"))
    version = _read_version(body.get("version"))

    def endpoints() -> tuple[str, str, str]:
        return (
            _identifier("type", body.get("type")),
            _id("from", body.get("from")),
            _id("to", body.get("to")),
        )

    if kind == "node":
        delta = Delta(
            kind="node",
            element="node",
            id=id_,
            version=version,
            labels=tuple(_labels(body.get("labels", []))),
            properties=_properties(body.get("properties")),
            key=node_key(id_),
        )
    elif kind == "edge":
        type_, from_id, to_id = endpoints()
        delta = Delta(
            kind="edge",
            element="edge",
            id=id_,
            version=version,
            type=type_,
            from_id=from_id,
            to_id=to_id,
            properties=_properties(body.get("properties")),
            key=edge_key(id_),
        )
    elif kind == "tombstone":
        element = body.get("element")
        if element == "node":
            delta = Delta(kind="tombstone", element="node", id=id_, version=version, key=node_key(id_))
        elif element == "edge":
            type_, from_id, to_id = endpoints()
            delta = Delta(
                kind="tombstone",
                element="edge",
                id=id_,
                version=version,
                type=type_,
                from_id=from_id,
                to_id=to_id,
                key=edge_key(id_),
            )
        else:
            raise ValueError("tombstone needs element 'node' or 'edge'")
    else:
        raise ValueError(f"unknown kind '{kind}'")

    if record.key is None:
        raise ValueError(f"no key; this delta's element key is '{delta.key.decode()}'")
    if record.key != delta.key:
        found = record.key.decode("utf-8", errors="replace")
        raise ValueError(f"key '{found}' is not this delta's element key '{delta.key.decode()}'")
    return delta

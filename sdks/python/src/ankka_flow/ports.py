"""Inlets and outlets with JSON contracts, verified by schema name (research R4)."""

from __future__ import annotations

import base64
import hashlib

from .records import Emit, Headers, Record

JSON_FORMAT = "json"


def fingerprint(schema_name: str) -> str:
    """Base64(SHA-256(UTF-8(schema_name))), standard alphabet, padded. Part of the protocol."""
    return base64.b64encode(hashlib.sha256(schema_name.encode("utf-8")).digest()).decode("ascii")


class Port:
    """A named, typed port. The name is the wire name; the Python attribute name is irrelevant."""

    format: str = JSON_FORMAT

    def __init__(self, name: str, *, schema_name: str) -> None:
        if not schema_name:
            raise ValueError(f"port '{name}' needs a schema_name")
        self.name = name
        self.schema_name = schema_name

    @property
    def fingerprint(self) -> str:
        return fingerprint(self.schema_name)

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.name!r}, schema_name={self.schema_name!r})"


class JsonInlet(Port):
    """An inlet carrying JSON values named by `schema_name`."""


_UNSET: object = object()


class JsonOutlet(Port):
    """An outlet carrying JSON values named by `schema_name`."""

    def emit(
        self,
        record: Record | None = None,
        *,
        value: bytes | None = None,
        key: bytes | None | object = _UNSET,
        headers: Headers | None = None,
    ) -> Emit:
        """An emit to this outlet.

        `emit(record)` forwards the record unchanged: same key, headers and bytes. Keyword arguments
        replace parts of it, or build a new record when no record is given.
        """
        if record is None:
            if value is None:
                raise ValueError("emit needs a record or a value")
            out = Record(
                value=value,
                key=None if key is _UNSET else key,  # type: ignore[arg-type]
                headers=list(headers or []),
            )
        else:
            out = Record(
                value=record.value if value is None else value,
                key=record.key if key is _UNSET else key,  # type: ignore[arg-type]
                headers=list(record.headers if headers is None else headers),
                offset=record.offset,
                timestamp_ms=record.timestamp_ms,
            )
        return Emit(self.name, out)

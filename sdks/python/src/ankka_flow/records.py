"""The values a streamlet sees: records as Kafka holds them, batches, and emits."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass, field

Headers = list[tuple[str, bytes]]


@dataclass(frozen=True)
class Record:
    """A Kafka record: value bytes, an optional key, and ordered headers. Nothing is decoded."""

    value: bytes
    key: bytes | None = None
    headers: Headers = field(default_factory=list)
    offset: int = -1
    timestamp_ms: int = 0


@dataclass(frozen=True)
class Batch:
    """Records from one partition of one inlet, in offset order."""

    inlet: str
    partition: int
    records: list[Record]

    def __iter__(self) -> Iterator[Record]:
        return iter(self.records)

    def __len__(self) -> int:
        return len(self.records)


@dataclass(frozen=True)
class Emit:
    """A record for an outlet. Emits are sent as they are yielded, before the batch's ack."""

    outlet: str
    record: Record

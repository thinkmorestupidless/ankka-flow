"""Run a streamlet over in-memory inlets and outlets: no Kafka, no sidecar, no gRPC (FR-025).

The harness applies the protocol's rules: one batch per partition in offset order, emits recorded
only when the batch succeeds, an undeclared outlet or an exception fails the batch and discards
its emits, as the sidecar would.
"""

from __future__ import annotations

import zlib
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field

from ..records import Batch, Headers, Record
from ..streamlet import Streamlet, run_batch

__all__ = ["Failure", "Harness", "InletQueue", "OutletRecords"]


@dataclass
class Failure:
    batch: Batch
    error: BaseException


@dataclass
class InletQueue:
    name: str
    pending: list[tuple[bytes | None, bytes, Headers]] = field(default_factory=list)

    def put(self, *, value: bytes, key: bytes | None = None, headers: Headers | None = None) -> None:
        self.pending.append((key, value, list(headers or [])))


@dataclass
class OutletRecords:
    name: str
    records: list[Record] = field(default_factory=list)


def _default_partition(key: bytes | None) -> int:
    return 0


class Harness:
    def __init__(self, streamlet: Streamlet, config: Mapping[str, object] | None = None) -> None:
        self.streamlet = streamlet
        streamlet.configure(dict(config or {}))
        self._inlets = {i.name: InletQueue(i.name) for i in streamlet.inlets()}
        self._outlets = {o.name: OutletRecords(o.name) for o in streamlet.outlets()}
        self._offsets: dict[tuple[str, int], int] = {}
        self.skipped: list[Record] = []
        self.failures: list[Failure] = []
        self.batches: list[Batch] = []

    def inlet(self, name: str) -> InletQueue:
        if name not in self._inlets:
            raise KeyError(f"no inlet '{name}'; declared: {sorted(self._inlets)}")
        return self._inlets[name]

    def outlet(self, name: str) -> OutletRecords:
        if name not in self._outlets:
            raise KeyError(f"no outlet '{name}'; declared: {sorted(self._outlets)}")
        return self._outlets[name]

    def run(
        self,
        partitions: Callable[[bytes | None], int] | None = None,
        max_records: int | None = None,
    ) -> None:
        """Process everything put so far: per inlet, per partition, batches in offset order.

        `partitions` places a key on a partition (default: everything on partition 0);
        `max_records` bounds a batch (default: one batch per partition).
        """
        place = partitions or _default_partition
        for inlet in self._inlets.values():
            by_partition: dict[int, list[Record]] = {}
            for key, value, headers in inlet.pending:
                part = place(key)
                offset = self._offsets.get((inlet.name, part), 0)
                self._offsets[(inlet.name, part)] = offset + 1
                by_partition.setdefault(part, []).append(
                    Record(value=value, key=key, headers=headers, offset=offset)
                )
            inlet.pending.clear()
            for part in sorted(by_partition):
                records = by_partition[part]
                size = max_records or len(records)
                for i in range(0, len(records), size):
                    self._run_one(Batch(inlet.name, part, records[i : i + size]))

    def _run_one(self, batch: Batch) -> None:
        self.batches.append(batch)
        try:
            emits = list(run_batch(self.streamlet, batch))
        except Exception as e:  # noqa: BLE001 - a failed batch is what the harness records
            self.failures.append(Failure(batch, e))
            return
        derived = {emit.record.offset for emit in emits}
        self.skipped.extend(r for r in batch if r.offset not in derived)
        for emit in emits:
            self._outlets[emit.outlet].records.append(emit.record)


def hash_partitioner(partitions: int) -> Callable[[bytes | None], int]:
    """A stable key-to-partition function for tests of ordering assumptions."""

    def place(key: bytes | None) -> int:
        return 0 if key is None else zlib.crc32(key) % partitions

    return place

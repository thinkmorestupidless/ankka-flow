from __future__ import annotations

from collections.abc import Iterable
from datetime import timedelta

import pytest

from ankka_flow import (
    Batch,
    DurationParameter,
    Emit,
    IntegerParameter,
    JsonInlet,
    JsonOutlet,
    MemorySizeParameter,
    Streamlet,
    json,
)
from ankka_flow.parameters import parse_duration, parse_memory_size
from ankka_flow.testkit import Harness, hash_partitioner


class Router(Streamlet):
    name = "router"
    inlet = JsonInlet("in", schema_name="cart-events.v1")
    valid = JsonOutlet("valid", schema_name="cart-events.v1")
    review = JsonOutlet("review", schema_name="cart-events.v1")
    threshold = IntegerParameter("review-threshold", default=100)

    def process(self, batch: Batch) -> Iterable[Emit]:
        for r in batch:
            event = json.loads(r.value)
            if event.get("skip"):
                continue
            if event.get("boom"):
                raise ValueError("boom")
            if event.get("rogue"):
                yield Emit("nowhere", r)
                continue
            yield (self.review if event["total"] > self.config[self.threshold] else self.valid).emit(r)


def test_routes_by_total_with_config_override() -> None:
    h = Harness(Router(), config={"review-threshold": 50})
    h.inlet("in").put(key=b"cart-1", value=b'{"total": 10}', headers=[("ce_type", b"ItemAdded")])
    h.inlet("in").put(key=b"cart-2", value=b'{"total": 99}')
    h.run()
    assert [r.key for r in h.outlet("valid").records] == [b"cart-1"]
    assert h.outlet("valid").records[0].headers == [("ce_type", b"ItemAdded")]
    assert h.outlet("review").records[0].headers == []
    assert h.skipped == [] and h.failures == []


def test_skipping_is_recorded() -> None:
    h = Harness(Router())
    h.inlet("in").put(key=b"a", value=b'{"skip": true}')
    h.inlet("in").put(key=b"b", value=b'{"total": 1}')
    h.run()
    assert [r.key for r in h.skipped] == [b"a"]


def test_an_exception_fails_the_batch_and_discards_its_emits() -> None:
    h = Harness(Router())
    h.inlet("in").put(key=b"a", value=b'{"total": 1}')
    h.inlet("in").put(key=b"b", value=b'{"boom": true}')
    h.run()
    assert h.outlet("valid").records == []
    assert len(h.failures) == 1 and str(h.failures[0].error) == "boom"


def test_an_undeclared_outlet_is_refused() -> None:
    h = Harness(Router())
    h.inlet("in").put(value=b'{"rogue": true}')
    h.run()
    assert "nowhere" in str(h.failures[0].error)


def test_partitions_keep_per_key_order() -> None:
    h = Harness(Router())
    for i in range(20):
        h.inlet("in").put(key=f"cart-{i % 4}".encode(), value=json.dumps({"total": i}))
    h.run(partitions=hash_partitioner(3), max_records=2)
    for key in {r.key for r in h.outlet("valid").records}:
        totals = [json.loads(r.value)["total"] for r in h.outlet("valid").records if r.key == key]
        assert totals == sorted(totals)
    assert all(len(b) <= 2 for b in h.batches)


def test_unknown_ports_and_parameters_are_named() -> None:
    h = Harness(Router())
    with pytest.raises(KeyError, match="declared"):
        h.inlet("nope")
    with pytest.raises(KeyError, match="no parameter declared for unknown"):
        Harness(Router(), config={"unknown": 1})


def test_duplicate_ports_are_refused_at_class_creation() -> None:
    with pytest.raises(TypeError, match="more than once: in"):

        class Twice(Streamlet):  # noqa: F841
            name = "twice"
            a = JsonInlet("in", schema_name="x.v1")
            b = JsonOutlet("in", schema_name="x.v1")

            def process(self, batch: Batch) -> Iterable[Emit]:
                return ()


def test_hocon_durations_and_sizes() -> None:
    assert parse_duration("100 ms") == timedelta(milliseconds=100)
    assert parse_duration("5m") == timedelta(minutes=5)
    assert parse_duration("1.5 s") == timedelta(seconds=1.5)
    assert parse_duration("250") == timedelta(milliseconds=250)
    assert parse_memory_size("1 MiB") == 1024 * 1024
    assert parse_memory_size("512k") == 512 * 1024
    assert parse_memory_size("10MB") == 10_000_000
    assert parse_memory_size("64") == 64
    for bad in ("soon", "5 parsecs"):
        with pytest.raises(ValueError):
            parse_duration(bad)
    with pytest.raises(ValueError):
        parse_memory_size("big")


def test_typed_parameters_and_bad_defaults() -> None:
    class Timed(Streamlet):
        name = "timed"
        wait = DurationParameter("wait", default="100 ms")
        size = MemorySizeParameter("size", default="1 MiB")

        def process(self, batch: Batch) -> Iterable[Emit]:
            return ()

    t = Timed()
    assert t.config[Timed.wait] == timedelta(milliseconds=100)
    assert t.config[Timed.size] == 1024 * 1024
    t.configure({"wait": "2s", "size": 10})
    assert t.config[Timed.wait] == timedelta(seconds=2) and t.config[Timed.size] == 10
    with pytest.raises(ValueError):
        IntegerParameter("x", default="1.5")

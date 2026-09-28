"""The server against a scripted sidecar: the protocol's rules from the process's side."""

from __future__ import annotations

import time
from collections.abc import Iterable, Iterator

import pytest

from ankka_flow import Batch, Emit, IntegerParameter, JsonInlet, JsonOutlet, Streamlet, serve
from ankka_flow.server import Server
from sidecar_double import SidecarDouble, record


class Scripted(Streamlet):
    """Behaviour by key, like the conformance reference."""

    name = "scripted"
    inlet = JsonInlet("in", schema_name="s.v1")
    side = JsonInlet("side", schema_name="s.v1")
    out = JsonOutlet("out", schema_name="s.v1")
    other = JsonOutlet("other", schema_name="s.v1")
    factor = IntegerParameter("factor", default=1)

    def process(self, batch: Batch) -> Iterable[Emit]:
        for r in batch:
            k = r.key or b""
            if k == b"echo":
                yield self.out.emit(r)
            elif k == b"fan":
                yield self.out.emit(r)
                yield self.other.emit(r)
            elif k == b"fail":
                raise RuntimeError("asked to fail")
            elif k == b"late":
                time.sleep(0.3)
                yield self.out.emit(r)
            elif k == b"rogue":
                yield Emit("nope", r)
            elif k == b"multiply":
                for i in range(self.config[self.factor]):
                    yield self.out.emit(r, headers=[("n", str(i).encode())])
            elif k == b"unkeyed":
                yield self.out.emit(r, key=None)


@pytest.fixture
def served() -> Iterator[tuple[Server, SidecarDouble]]:
    server = serve(Scripted(), port=0, block=False)
    double = SidecarDouble(server.port)
    yield server, double
    double.close()
    server.stop(0)


def kinds(msgs: list) -> list[str]:  # type: ignore[type-arg]
    return [m.WhichOneof("message") for m in msgs]


def test_binds_loopback_only(served: tuple[Server, SidecarDouble]) -> None:
    import socket

    server, _ = served
    with socket.socket() as sock:
        sock.settimeout(0.5)
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as probe:
            try:
                probe.connect(("192.0.2.1", 9))  # TEST-NET-1: no packet is sent for a UDP connect
                host = probe.getsockname()[0]
            except OSError:
                pytest.skip("no route off this machine to find a non-loopback address")
        if host.startswith("127.") or host == "0.0.0.0":
            pytest.skip("no non-loopback address to probe")
        assert sock.connect_ex((host, server.port)) != 0


def test_discovery_answers_the_descriptor(served: tuple[Server, SidecarDouble]) -> None:
    _, double = served
    spec = double.discover()
    assert spec.protocol_version == "1.0"
    assert spec.sdk.name == "ankka-flow-python"
    assert [p.name for p in spec.streamlet.inlets] == ["in", "side"]


def test_emit_preserves_key_headers_and_bytes_then_acks(served: tuple[Server, SidecarDouble]) -> None:
    _, double = served
    conv = double.run()
    headers = [("ce_type", b"ItemAdded"), ("raw", bytes([0, 255, 42, 127, 128]))]
    bid = conv.batch([record(b"\x00value\xff", key=b"echo", headers=headers)])
    msgs = conv.until_done(bid)
    assert kinds(msgs) == ["emit", "ack"]
    emitted = msgs[0].emit.record
    assert msgs[0].emit.outlet == "out"
    assert emitted.key == b"echo"
    assert emitted.value == b"\x00value\xff"
    assert [(h.key, h.value) for h in emitted.headers] == headers


def test_fan_out_skip_and_nothing_before_start(served: tuple[Server, SidecarDouble]) -> None:
    _, double = served
    conv = double.run()
    assert conv.silent_for(0.3)  # Start alone produces nothing
    bid = conv.batch([record(b"v", key=b"fan"), record(b"v", key=b"skip")])
    msgs = conv.until_done(bid)
    assert kinds(msgs) == ["emit", "emit", "ack"]
    assert [m.emit.outlet for m in msgs[:2]] == ["out", "other"]


def test_an_exception_fails_the_batch_with_its_message(served: tuple[Server, SidecarDouble]) -> None:
    _, double = served
    conv = double.run()
    bid = conv.batch([record(b"v", key=b"fail")])
    msgs = conv.until_done(bid)
    assert kinds(msgs) == ["fail"]
    assert msgs[0].fail.error.message == "asked to fail"


def test_an_undeclared_outlet_fails_the_batch(served: tuple[Server, SidecarDouble]) -> None:
    _, double = served
    conv = double.run()
    msgs = conv.until_done(conv.batch([record(b"v", key=b"rogue")]))
    assert kinds(msgs) == ["fail"]
    assert "nope" in msgs[0].fail.error.message


def test_partitions_run_concurrently(served: tuple[Server, SidecarDouble]) -> None:
    _, double = served
    conv = double.run()
    slow = conv.batch([record(b"v", key=b"late")], partition=0)
    fast = conv.batch([record(b"v", key=b"echo")], partition=1)
    msgs = conv.until_done(slow)
    acks = [m.ack.batch_id for m in msgs if m.WhichOneof("message") == "ack"]
    assert acks == [fast, slow]


def test_config_from_start_and_unkeyed_emit(served: tuple[Server, SidecarDouble]) -> None:
    _, double = served
    conv = double.run(config={"factor": 3})
    msgs = conv.until_done(conv.batch([record(b"v", key=b"multiply")]))
    assert kinds(msgs) == ["emit", "emit", "emit", "ack"]
    assert [m.emit.record.headers[0].value for m in msgs[:3]] == [b"0", b"1", b"2"]
    msgs = conv.until_done(conv.batch([record(b"v", key=b"unkeyed")]))
    assert not msgs[0].emit.record.HasField("key")


def test_stop_completes_the_stream(served: tuple[Server, SidecarDouble]) -> None:
    _, double = served
    conv = double.run()
    bid = conv.batch([record(b"v", key=b"late")])
    conv.stop()
    msgs = []
    while (m := conv.next()) is not None:
        msgs.append(m)
    assert kinds(msgs) == ["emit", "ack"]
    assert msgs[-1].ack.batch_id == bid


def test_a_second_run_supersedes_the_first(served: tuple[Server, SidecarDouble]) -> None:
    _, double = served
    first = double.run(conversation="first")
    first.until_done(first.batch([record(b"v", key=b"echo")]))
    second = double.run(conversation="second")
    assert first.next() is None  # the first stream ends
    msgs = second.until_done(second.batch([record(b"v", key=b"echo")]))
    assert kinds(msgs) == ["emit", "ack"]


def test_break_ack_first_reverses_the_order(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ANKKA_FLOW_BREAK", "ack-first")
    server = serve(Scripted(), port=0, block=False)
    double = SidecarDouble(server.port)
    try:
        conv = double.run()
        bid = conv.batch([record(b"v", key=b"echo")])
        first = conv.next()
        assert first is not None and first.WhichOneof("message") == "ack" and first.ack.batch_id == bid
        second = conv.next()
        assert second is not None and second.WhichOneof("message") == "emit"
    finally:
        double.close()
        server.stop(0)


def test_large_record_round_trips(served: tuple[Server, SidecarDouble]) -> None:
    _, double = served
    conv = double.run()
    big = bytes(range(256)) * (3 * 1024 * 4)  # 3 MiB
    msgs = conv.until_done(conv.batch([record(big, key=b"echo")]))
    assert msgs[0].emit.record.value == big

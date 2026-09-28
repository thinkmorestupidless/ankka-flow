"""A scripted stand-in for the sidecar: dials a served streamlet and drives one Run conversation."""

from __future__ import annotations

import json
import queue
import threading
from collections.abc import Iterator

import grpc

from ankka_flow._proto.ankka.flow.v1 import discovery_pb2 as d
from ankka_flow._proto.ankka.flow.v1 import discovery_pb2_grpc as d_grpc
from ankka_flow._proto.ankka.flow.v1 import payload_pb2 as p
from ankka_flow._proto.ankka.flow.v1 import streamlet_pb2 as s
from ankka_flow._proto.ankka.flow.v1 import streamlet_pb2_grpc as s_grpc

_CLOSE = object()


def record(value: bytes, key: bytes | None = None, headers: list[tuple[str, bytes]] | None = None) -> p.Record:
    r = p.Record(value=value, headers=[p.Header(key=k, value=v) for k, v in headers or []])
    if key is not None:
        r.key = key
    return r


class SidecarDouble:
    def __init__(self, port: int) -> None:
        self.channel = grpc.insecure_channel(f"127.0.0.1:{port}")
        self.discovery = d_grpc.DiscoveryStub(self.channel)
        self.streamlet = s_grpc.StreamletStub(self.channel)

    def discover(self) -> d.Spec:
        spec: d.Spec = self.discovery.Discover(d.SidecarInfo(protocol_version="1.0", sidecar_version="test"))
        return spec

    def run(self, config: dict[str, object] | None = None, conversation: str = "c1") -> Conversation:
        return Conversation(self.streamlet, config or {}, conversation)

    def close(self) -> None:
        self.channel.close()


class Conversation:
    def __init__(self, stub: s_grpc.StreamletStub, config: dict[str, object], cid: str) -> None:
        self.requests: queue.Queue[object] = queue.Queue()
        self.received: queue.Queue[s.FromProcess | None] = queue.Queue()
        self.next_id = 1
        self.requests.put(
            s.ToProcess(
                start=s.Start(
                    conversation_id=cid,
                    pipeline="p",
                    streamlet="st",
                    config_json=json.dumps(config),
                    max_message_bytes=4 * 1024 * 1024,
                )
            )
        )
        self.responses = stub.Run(self._requests())
        threading.Thread(target=self._pump, daemon=True).start()

    def _requests(self) -> Iterator[s.ToProcess]:
        while True:
            msg = self.requests.get()
            if msg is _CLOSE:
                return
            assert isinstance(msg, s.ToProcess)
            yield msg

    def _pump(self) -> None:
        try:
            for msg in self.responses:
                self.received.put(msg)
        except grpc.RpcError:
            pass
        self.received.put(None)

    def batch(self, records: list[p.Record], inlet: str = "in", partition: int = 0) -> int:
        bid = self.next_id
        self.next_id += 1
        self.requests.put(
            s.ToProcess(
                batch=s.Batch(
                    batch_id=bid,
                    inlet=inlet,
                    partition=partition,
                    records=[s.InputRecord(offset=i, timestamp_ms=1, record=r) for i, r in enumerate(records)],
                )
            )
        )
        return bid

    def stop(self) -> None:
        self.requests.put(s.ToProcess(stop=s.Stop(reason="test")))

    def close(self) -> None:
        self.requests.put(_CLOSE)

    def next(self, timeout: float = 5.0) -> s.FromProcess | None:
        return self.received.get(timeout=timeout)

    def until_done(self, batch_id: int, timeout: float = 5.0) -> list[s.FromProcess]:
        """Every message up to and including the ack or fail of `batch_id`."""
        out: list[s.FromProcess] = []
        while True:
            msg = self.next(timeout)
            assert msg is not None, f"stream ended before batch {batch_id} completed: {out}"
            out.append(msg)
            kind = msg.WhichOneof("message")
            if kind in ("ack", "fail") and getattr(msg, kind).batch_id == batch_id:
                return out

    def silent_for(self, seconds: float) -> bool:
        try:
            self.received.get(timeout=seconds)
            return False
        except queue.Empty:
            return True

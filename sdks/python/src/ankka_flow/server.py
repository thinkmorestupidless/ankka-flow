"""The process side of the protocol: Discovery and Streamlet.Run on 127.0.0.1 only (FR-007)."""

from __future__ import annotations

import json
import logging
import os
import queue
import signal
import threading
from collections.abc import Iterator
from concurrent import futures
from typing import Any

import grpc

from ._proto.ankka.flow.v1 import discovery_pb2 as d
from ._proto.ankka.flow.v1 import discovery_pb2_grpc as d_grpc
from ._proto.ankka.flow.v1 import payload_pb2 as p
from ._proto.ankka.flow.v1 import streamlet_pb2 as s
from ._proto.ankka.flow.v1 import streamlet_pb2_grpc as s_grpc
from .descriptor import spec_for
from .records import Batch, Record
from .streamlet import Streamlet, run_batch

log = logging.getLogger("ankka_flow")

DEFAULT_PROCESS_PORT = 9010
LOOPBACK = "127.0.0.1"
MAX_MESSAGE_BYTES = 16 * 1024 * 1024

_END = object()


def _to_record(r: s.InputRecord) -> Record:
    rec = r.record
    return Record(
        value=rec.value,
        key=rec.key if rec.HasField("key") else None,
        headers=[(h.key, h.value) for h in rec.headers],
        offset=r.offset,
        timestamp_ms=r.timestamp_ms,
    )


def _to_proto(record: Record) -> p.Record:
    out = p.Record(value=record.value, headers=[p.Header(key=k, value=v) for k, v in record.headers])
    if record.key is not None:
        out.key = record.key
    elif os.environ.get("ANKKA_FLOW_BREAK") == "keyless-empty-key":
        # Test-only: breaks the rule that a keyless emit carries no key (SC-005).
        out.key = b""
    return out


def _failure_message(e: BaseException) -> str:
    return str(e) or type(e).__name__


class _Conversation:
    """One Run stream. Messages out go through a queue so batches can run on worker threads."""

    def __init__(self, streamlet: Streamlet, workers: futures.Executor) -> None:
        self.streamlet = streamlet
        self.workers = workers
        self.out: queue.Queue[object] = queue.Queue()
        self.ended = threading.Event()
        self.in_flight: set[futures.Future[None]] = set()
        self.lock = threading.Lock()
        self.ack_first = os.environ.get("ANKKA_FLOW_BREAK") == "ack-first"

    def end(self) -> None:
        if not self.ended.is_set():
            self.ended.set()
            self.out.put(_END)

    def send(self, msg: s.FromProcess) -> None:
        if not self.ended.is_set():
            self.out.put(msg)

    def start(self, start: s.Start) -> None:
        values: dict[str, object] = json.loads(start.config_json) if start.config_json else {}
        self.streamlet.configure(values)
        log.info(
            "conversation %s started: pipeline %s, streamlet %s",
            start.conversation_id,
            start.pipeline,
            start.streamlet,
        )

    def submit(self, batch: s.Batch) -> None:
        fut = self.workers.submit(self._run, batch)
        with self.lock:
            self.in_flight.add(fut)
        fut.add_done_callback(self._done)

    def _done(self, fut: futures.Future[None]) -> None:
        with self.lock:
            self.in_flight.discard(fut)

    def drain(self) -> None:
        with self.lock:
            pending = list(self.in_flight)
        futures.wait(pending)

    def _run(self, batch: s.Batch) -> None:
        b = Batch(batch.inlet, batch.partition, [_to_record(r) for r in batch.records])
        emits: list[s.FromProcess] = []
        try:
            for e in run_batch(self.streamlet, b):
                msg = s.FromProcess(
                    emit=s.Emit(batch_id=batch.batch_id, outlet=e.outlet, record=_to_proto(e.record))
                )
                if self.ack_first:
                    emits.append(msg)
                else:
                    self.send(msg)
        except Exception as e:  # noqa: BLE001 - any failure of user code fails the batch
            log.warning("batch %d on %s/%d failed: %s", batch.batch_id, batch.inlet, batch.partition, e)
            self.send(
                s.FromProcess(fail=s.Fail(batch_id=batch.batch_id, error=p.Error(message=_failure_message(e))))
            )
            return
        self.send(s.FromProcess(ack=s.Ack(batch_id=batch.batch_id)))
        for msg in emits:  # only when deliberately broken: ack before emits
            self.send(msg)


class _Servicer(d_grpc.DiscoveryServicer, s_grpc.StreamletServicer):  # type: ignore[misc]
    def __init__(self, streamlet: Streamlet, workers: futures.Executor) -> None:
        self.streamlet = streamlet
        self.workers = workers
        self.spec = spec_for(streamlet)
        self.current: _Conversation | None = None
        self.lock = threading.Lock()

    def Discover(self, request: d.SidecarInfo, context: grpc.ServicerContext) -> d.Spec:
        log.info(
            "discovery from sidecar %s (protocol %s)", request.sidecar_version, request.protocol_version
        )
        return self.spec

    def ReportError(self, request: p.Problems, context: grpc.ServicerContext) -> p.Empty:
        for problem in request.problems:
            log.error("the sidecar refused this process: %s", problem.message)
        return p.Empty()

    def Run(
        self, request_iterator: Iterator[s.ToProcess], context: grpc.ServicerContext
    ) -> Iterator[s.FromProcess]:
        conv = _Conversation(self.streamlet, self.workers)
        with self.lock:
            previous, self.current = self.current, conv
        if previous is not None:
            log.info("a new conversation supersedes the previous one")
            previous.end()
        context.add_callback(conv.end)
        threading.Thread(target=self._read, args=(conv, request_iterator), daemon=True).start()
        while True:
            msg = conv.out.get()
            if msg is _END:
                return
            assert isinstance(msg, s.FromProcess)
            yield msg

    def _read(self, conv: _Conversation, requests: Iterator[s.ToProcess]) -> None:
        started = False
        try:
            for req in requests:
                if conv.ended.is_set():
                    return
                kind = req.WhichOneof("message")
                if kind == "start":
                    conv.start(req.start)
                    started = True
                elif kind == "batch":
                    if not started:
                        log.error("a batch arrived before Start; ending the conversation")
                        break
                    conv.submit(req.batch)
                elif kind == "stop":
                    log.info("stop: %s", req.stop.reason or "no reason given")
                    break
        except grpc.RpcError:
            pass
        except Exception:  # noqa: BLE001
            log.exception("the conversation's reader failed")
        conv.drain()
        conv.end()


class Server:
    """A running process. `port` is the bound port; `stop()` ends it."""

    def __init__(self, server: grpc.Server, port: int) -> None:
        self._server = server
        self.port = port

    def stop(self, grace: float | None = 2.0) -> None:
        self._server.stop(grace).wait()

    def wait(self) -> None:
        self._server.wait_for_termination()


def serve(
    streamlet: Streamlet,
    port: int | None = None,
    block: bool = True,
    workers: int = 32,
) -> Server:
    """Serve the streamlet on 127.0.0.1:`port` (default $FLOW_PROCESS_PORT or 9010).

    With `block=True` this returns only when the server stops (SIGTERM or SIGINT stop it).
    """
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    if port is None:
        port = int(os.environ.get("FLOW_PROCESS_PORT", DEFAULT_PROCESS_PORT))
    options: list[tuple[str, Any]] = [
        ("grpc.max_receive_message_length", MAX_MESSAGE_BYTES),
        ("grpc.max_send_message_length", MAX_MESSAGE_BYTES),
    ]
    batch_workers = futures.ThreadPoolExecutor(max_workers=workers, thread_name_prefix="batch")
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=16, thread_name_prefix="grpc"), options=options)
    servicer = _Servicer(streamlet, batch_workers)
    d_grpc.add_DiscoveryServicer_to_server(servicer, server)
    s_grpc.add_StreamletServicer_to_server(servicer, server)
    bound = server.add_insecure_port(f"{LOOPBACK}:{port}")
    if bound == 0:
        raise OSError(f"could not bind {LOOPBACK}:{port}")
    server.start()
    log.info("streamlet %s serving on %s:%d", type(streamlet).name, LOOPBACK, bound)
    handle = Server(server, bound)
    if block:
        if threading.current_thread() is threading.main_thread():
            def _stop(signum: int, _frame: object) -> None:
                log.info("signal %d: stopping", signum)
                threading.Thread(target=handle.stop, daemon=True).start()

            signal.signal(signal.SIGTERM, _stop)
            signal.signal(signal.SIGINT, _stop)
        handle.wait()
        batch_workers.shutdown(wait=False)
    return handle

"""SC-008: throughput per partition and the latency the sidecar path adds, on a laptop.

Runs against the compose file in this directory (`docker compose up -d` first). It owns the router
process and stops and starts the compose sidecar itself, and it recreates the sample's topics.

  throughput  preload N events with the router and sidecar stopped, start them, and time the drain
              of the router's consumer group: records per second per input partition.
  latency     produce at a steady rate; a plain consumer reads the input and another reads both
              outlets; each value carries its send time, so each side measures send-to-receive.
              The sidecar path's added median is the outlet median less the input median.

Exit 0 when throughput is at least 1,000 records/s per partition and the added median latency is
under 10 ms (SC-008); the numbers are printed either way.
"""

from __future__ import annotations

import argparse
import json
import statistics
import subprocess
import sys
import threading
import time
from pathlib import Path

from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer
from kafka.admin import NewTopic
from kafka.errors import CoordinatorNotAvailableError, GroupIdNotFoundError, UnknownTopicOrPartitionError

INPUT = "shop.cart-events.v1"
OUTLETS = ("cart.valid-carts", "cart.review-carts")
GROUP = "cart.router.in"
HERE = Path(__file__).resolve().parent


def compose(*args: str) -> None:
    subprocess.run(["docker", "compose", *args], cwd=HERE, check=True, capture_output=True)


def reset_topics(bootstrap: str, partitions: int) -> None:
    admin = KafkaAdminClient(bootstrap_servers=bootstrap)
    try:
        if GROUP in {g["group_id"] for g in admin.list_groups()}:
            admin.delete_groups([GROUP])
        try:
            admin.delete_topics([INPUT, *OUTLETS])
        except UnknownTopicOrPartitionError:
            pass
        deadline = time.time() + 30
        while time.time() < deadline and set(admin.list_topics()) & {INPUT, *OUTLETS}:
            time.sleep(0.5)
        admin.create_topics([NewTopic(t, partitions, 1) for t in (INPUT, *OUTLETS)])
    finally:
        admin.close()


def committed(bootstrap: str) -> int:
    """The router group's committed offsets, summed; 0 before its first commit."""
    admin = KafkaAdminClient(bootstrap_servers=bootstrap)
    try:
        offsets = admin.list_group_offsets(GROUP).get(GROUP, {})
        return sum(m.offset for m in offsets.values() if m.offset >= 0)
    except (CoordinatorNotAvailableError, GroupIdNotFoundError):
        return 0  # the group is not there yet, or its coordinator is still loading
    finally:
        admin.close()


def event(i: int, carts: int) -> tuple[bytes, bytes]:
    cart = f"cart-{i % carts}"
    total = (i * 37) % 200
    return cart.encode(), json.dumps({"id": i, "total": total, "sent": time.time()}).encode()


def start_router() -> subprocess.Popen[bytes]:
    return subprocess.Popen([sys.executable, "-m", "cart_router.main"], cwd=HERE, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def throughput(bootstrap: str, count: int, partitions: int) -> float:
    compose("stop", "sidecar")
    reset_topics(bootstrap, partitions)
    producer = KafkaProducer(bootstrap_servers=bootstrap, linger_ms=5, batch_size=256 * 1024)
    for i in range(count):
        key, value = event(i, 1000)
        producer.send(INPUT, key=key, value=value)
    producer.flush()
    producer.close()
    router = start_router()
    try:
        compose("start", "sidecar")
        # The clock starts when the group first commits, so container start-up is not counted.
        deadline = time.time() + 120
        while committed(bootstrap) == 0 and time.time() < deadline:
            time.sleep(0.05)
        start, base = time.time(), committed(bootstrap)
        while committed(bootstrap) < count and time.time() < deadline + 300:
            time.sleep(0.2)
        elapsed = time.time() - start
        return (committed(bootstrap) - base) / elapsed / partitions
    finally:
        router.terminate()
        router.wait()


def latency(bootstrap: str, rate: int, seconds: int) -> tuple[float, float]:
    samples: dict[str, list[float]] = {"input": [], "outlet": []}
    stop = threading.Event()

    def read(name: str, topics: list[str]) -> None:
        consumer = KafkaConsumer(*topics, bootstrap_servers=bootstrap, auto_offset_reset="latest", group_id=None)
        while not stop.is_set():
            for records in consumer.poll(timeout_ms=100).values():
                now = time.time()
                for r in records:
                    samples[name].append((now - json.loads(r.value)["sent"]) * 1000)
        consumer.close()

    router = start_router()
    readers = [threading.Thread(target=read, args=("input", [INPUT])), threading.Thread(target=read, args=("outlet", list(OUTLETS)))]
    try:
        time.sleep(8)  # the sidecar rediscovers the router and rejoins its group
        for t in readers:
            t.start()
        time.sleep(3)
        producer = KafkaProducer(bootstrap_servers=bootstrap, linger_ms=0)
        interval = 1.0 / rate
        next_send = time.time()
        for i in range(rate * seconds):
            key, value = event(i, 1000)
            producer.send(INPUT, key=key, value=value)
            next_send += interval
            time.sleep(max(0.0, next_send - time.time()))
        producer.flush()
        time.sleep(3)
    finally:
        stop.set()
        for t in readers:
            t.join()
        router.terminate()
        router.wait()
    return statistics.median(samples["input"]), statistics.median(samples["outlet"])


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--bootstrap", default="localhost:9094")
    parser.add_argument("--count", type=int, default=60_000)
    parser.add_argument("--partitions", type=int, default=3)
    parser.add_argument("--rate", type=int, default=200, help="records per second in the latency phase")
    parser.add_argument("--seconds", type=int, default=15)
    args = parser.parse_args()

    per_partition = throughput(args.bootstrap, args.count, args.partitions)
    plain, through = latency(args.bootstrap, args.rate, args.seconds)
    added = through - plain
    print(f"| measure | value |\n|---|---|")
    print(f"| records | {args.count} over {args.partitions} partitions |")
    print(f"| throughput per partition | {per_partition:,.0f} records/s |")
    print(f"| median latency, plain consumer | {plain:.1f} ms |")
    print(f"| median latency, through the router | {through:.1f} ms |")
    print(f"| added by the sidecar path | {added:.1f} ms |")
    ok = per_partition >= 1000 and added < 10
    print("SC-008:", "met" if ok else "NOT met")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())

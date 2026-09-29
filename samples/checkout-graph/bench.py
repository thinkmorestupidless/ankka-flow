"""Measure the merge sink's throughput (SC-007): deltas merged per second per inlet partition.

Preloads the deltas topic with the three deltas per notice the mapper writes, starts the sink's
sidecar, and times it from its first commit until its consumer group has committed everything.

    docker compose up -d kafka neo4j
    uv run python bench.py                     # 20,000 notices = 60,000 deltas over 3 partitions
    uv run python bench.py --notices 50000

It starts `sidecar-graph` itself (`docker compose up -d sidecar-graph`) once the topic is loaded,
and stops it again at the end.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time

from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer, TopicPartition
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError
from neo4j import GraphDatabase

DELTAS = "checkouts-graph.graph-deltas"
GROUP = "checkouts-graph.graph.in"
PARTITIONS = 3


def deltas(cart: str, at: int) -> list[tuple[str, dict[str, object]]]:
    cart_id, checkout_id = f"cart:{cart}", f"checkout:{cart}:{at}"
    return [
        (cart_id, {"kind": "node", "id": cart_id, "version": at, "labels": ["Cart"], "properties": {"cartId": cart}}),
        (checkout_id, {"kind": "node", "id": checkout_id, "version": at, "labels": ["Checkout"], "properties": {"cartId": cart}}),
        (
            f"checked-out:{cart}:{at}",
            {"kind": "edge", "id": f"checked-out:{cart}:{at}", "version": at, "type": "CHECKED_OUT",
             "from": cart_id, "to": checkout_id, "properties": {}},
        ),
    ]


def committed(admin: KafkaAdminClient) -> int:
    offsets = admin.list_consumer_group_offsets(GROUP)
    return sum(o.offset for tp, o in offsets.items() if tp.topic == DELTAS and o.offset >= 0)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap", default="localhost:9094")
    parser.add_argument("--notices", type=int, default=20_000)
    parser.add_argument("--carts", type=int, default=2_000)
    args = parser.parse_args()

    with GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "flow-local-password")) as driver:
        driver.execute_query("MATCH (n) CALL (n) { DETACH DELETE n } IN TRANSACTIONS OF 10000 ROWS")

    admin = KafkaAdminClient(bootstrap_servers=args.bootstrap, client_id="checkout-graph-bench")
    if DELTAS in set(admin.list_topics()):
        admin.delete_topics([DELTAS])
        time.sleep(2)
    try:
        admin.create_topics([NewTopic(DELTAS, num_partitions=PARTITIONS, replication_factor=1)])
    except TopicAlreadyExistsError:
        pass
    try:
        admin.delete_consumer_groups([GROUP])
    except Exception:  # the group does not exist yet
        pass

    producer = KafkaProducer(bootstrap_servers=args.bootstrap, linger_ms=20, batch_size=256 * 1024)
    total = 0
    for i in range(args.notices):
        for key, delta in deltas(f"cart-{i % args.carts}", 1_790_000_000_000 + i):
            producer.send(DELTAS, key=key.encode(), value=json.dumps(delta).encode())
            total += 1
    producer.flush()
    consumer = KafkaConsumer(bootstrap_servers=args.bootstrap)
    ends = consumer.end_offsets([TopicPartition(DELTAS, p) for p in range(PARTITIONS)])
    consumer.close()
    assert sum(ends.values()) == total, (ends, total)
    print(f"loaded {total} deltas over {PARTITIONS} partitions")

    subprocess.run(["docker", "compose", "up", "-d", "sidecar-graph"], check=True)
    try:
        started = None
        deadline = time.monotonic() + 900
        while time.monotonic() < deadline:
            done = committed(admin)
            if started is None and done > 0:
                started = time.monotonic()
                first = done
            if done >= total:
                break
            time.sleep(0.2)
        else:
            sys.exit(f"the sink committed only {committed(admin)} of {total} in 15 minutes")
        assert started is not None
        elapsed = time.monotonic() - started
        rate = (total - first) / elapsed
        print(f"{total - first} deltas in {elapsed:.1f}s: {rate:,.0f}/s, {rate / PARTITIONS:,.0f}/s per partition")
    finally:
        subprocess.run(["docker", "compose", "stop", "sidecar-graph"], check=False)
        admin.close()


if __name__ == "__main__":
    main()

"""Write checkout notices to `cart-checkouts` as ankka's shopping cart would, with a plain Kafka client.

Creates the topics first when they are missing: the compose Kafka does not auto-create topics, and
on a laptop there is no operator to create the managed one.

    uv run python produce.py                         # 20 notices over 5 carts to localhost:9094
    uv run python produce.py --count 200 --carts 20
"""

from __future__ import annotations

import argparse
import json
import time
import uuid

from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError

INPUT = "cart-checkouts"
DELTAS = "checkouts-graph.graph-deltas"
PARTITIONS = 3


def ensure_topics(bootstrap: str) -> None:
    admin = KafkaAdminClient(bootstrap_servers=bootstrap, client_id="checkout-graph-admin")
    try:
        missing = [t for t in (INPUT, DELTAS) if t not in set(admin.list_topics())]
        if missing:
            try:
                admin.create_topics([NewTopic(t, num_partitions=PARTITIONS, replication_factor=1) for t in missing])
                print(f"created {', '.join(missing)}")
            except TopicAlreadyExistsError:
                pass
    finally:
        admin.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap", default="localhost:9094")
    parser.add_argument("--count", type=int, default=20)
    parser.add_argument("--carts", type=int, default=5)
    parser.add_argument("--start", type=int, default=1_790_000_000_000, help="the first notice's time, ms")
    args = parser.parse_args()
    ensure_topics(args.bootstrap)
    producer = KafkaProducer(bootstrap_servers=args.bootstrap)
    for i in range(args.count):
        cart = f"cart-{i % args.carts}"
        body = json.dumps({"cartId": cart, "at": args.start + i}).encode()
        headers = [
            ("ce-specversion", b"1.0"),
            ("ce-id", str(uuid.uuid4()).encode()),
            ("ce-type", b"message"),
            ("ce-subject", cart.encode()),
            ("content-type", b"application/json"),
        ]
        producer.send(INPUT, key=cart.encode(), value=body, headers=headers)
    producer.flush()
    print(f"produced {args.count} notices over {args.carts} carts to {INPUT} at {time.strftime('%H:%M:%S')}")


if __name__ == "__main__":
    main()

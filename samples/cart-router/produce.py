"""Write cart events to the router's input topic with a plain Kafka client.

Creates the three topics first when they are missing: the compose Kafka does not auto-create
topics, and on a laptop there is no operator to create the managed ones.

    uv run python produce.py                      # 50 events over 10 carts to localhost:9094
    uv run python produce.py --count 200 --carts 20
"""

from __future__ import annotations

import argparse
import json

from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError

INPUT = "shop.cart-events.v1"
OUTLETS = ("cart.valid-carts", "cart.review-carts")
PARTITIONS = 3


def ensure_topics(bootstrap: str) -> None:
    admin = KafkaAdminClient(bootstrap_servers=bootstrap, client_id="cart-produce-admin")
    try:
        existing = set(admin.list_topics())
        missing = [t for t in (INPUT, *OUTLETS) if t not in existing]
        if missing:
            try:
                admin.create_topics(
                    [NewTopic(name=t, num_partitions=PARTITIONS, replication_factor=1) for t in missing]
                )
                print(f"created {', '.join(missing)}")
            except TopicAlreadyExistsError:
                pass
    finally:
        admin.close()


def event_total(i: int) -> int:
    """Deterministic totals spread either side of the default threshold of 100."""
    return (i * 37) % 200


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--bootstrap", default="localhost:9094")
    parser.add_argument("--count", type=int, default=50)
    parser.add_argument("--start", type=int, default=0, help="the first event's index, to produce in parts")
    parser.add_argument("--carts", type=int, default=10)
    args = parser.parse_args()

    ensure_topics(args.bootstrap)
    producer = KafkaProducer(bootstrap_servers=args.bootstrap, acks="all", client_id="cart-produce")
    for i in range(args.start, args.start + args.count):
        cart = f"cart-{i % args.carts}"
        seq = i // args.carts + 1  # a cart's events are numbered 1, 2, 3 across every part
        event = {"cartId": cart, "seq": seq, "total": event_total(i)}
        producer.send(
            INPUT,
            key=cart.encode(),
            value=json.dumps(event).encode(),
            headers=[
                ("ce_type", b"ItemAdded"),
                ("ce_id", f"{cart}-{seq}".encode()),
                ("ce_source", b"samples/cart-router/produce.py"),
            ],
        )
    producer.flush()
    producer.close()
    print(f"produced {args.count} events over {args.carts} carts to {INPUT}")


if __name__ == "__main__":
    main()

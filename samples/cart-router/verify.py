"""Check the spec's first story against real topics.

Reads the input topic and both outlet topics from the beginning and asserts that every input event
is on exactly the outlet its total chose and never on the other, that each cart's events are on one
partition of each outlet in the order they were produced, and that the three CloudEvents headers
arrived intact. At-least-once delivery may repeat an event after a restart; repeats are reported,
and they must not reorder a cart.

    uv run python verify.py                       # exits 0 when everything holds, 1 otherwise
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections import defaultdict
from dataclasses import dataclass

from kafka import KafkaConsumer, TopicPartition

INPUT = "shop.cart-events.v1"
VALID = "cart.valid-carts"
REVIEW = "cart.review-carts"
HEADERS = ("ce_type", "ce_id", "ce_source")


@dataclass(frozen=True)
class Seen:
    topic: str
    partition: int
    offset: int
    key: bytes | None
    event: dict[str, object]
    headers: tuple[tuple[str, bytes], ...]


def read_all(bootstrap: str, topic: str, expect_at_least: int, timeout: float) -> list[Seen]:
    consumer = KafkaConsumer(bootstrap_servers=bootstrap, enable_auto_commit=False, consumer_timeout_ms=1000)
    try:
        partitions = consumer.partitions_for_topic(topic) or set()
        tps = [TopicPartition(topic, p) for p in sorted(partitions)]
        if not tps:
            return []
        consumer.assign(tps)
        consumer.seek_to_beginning(*tps)
        seen: list[Seen] = []
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            for msg in consumer:
                seen.append(
                    Seen(
                        topic,
                        msg.partition,
                        msg.offset,
                        msg.key,
                        json.loads(msg.value),
                        tuple((k, v) for k, v in msg.headers),
                    )
                )
            if len({(s.event["cartId"], s.event["seq"]) for s in seen}) >= expect_at_least:
                break
        return seen
    finally:
        consumer.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--bootstrap", default="localhost:9094")
    parser.add_argument("--threshold", type=int, default=100, help="the router's review-threshold")
    parser.add_argument("--timeout", type=float, default=60.0)
    args = parser.parse_args()

    inputs = read_all(args.bootstrap, INPUT, 0, 5.0)
    if not inputs:
        print(f"FAIL: nothing on {INPUT}; run produce.py first")
        return 1
    expected = {(i.event["cartId"], i.event["seq"]): i for i in inputs}
    want_review = {k for k, i in expected.items() if int(str(i.event["total"])) > args.threshold}
    want_valid = set(expected) - want_review

    valid = read_all(args.bootstrap, VALID, len(want_valid), args.timeout)
    review = read_all(args.bootstrap, REVIEW, len(want_review), args.timeout)
    problems: list[str] = []
    repeats = 0

    for outlet, records, wanted, other in ((VALID, valid, want_valid, want_review), (REVIEW, review, want_review, want_valid)):
        ids = [(r.event["cartId"], r.event["seq"]) for r in records]
        missing = sorted(wanted - set(ids), key=str)
        wrong = sorted({i for i in ids if i in other}, key=str)
        repeats += len(ids) - len(set(ids))
        if missing:
            problems.append(f"{outlet}: {len(missing)} event(s) missing, e.g. {missing[:3]}")
        if wrong:
            problems.append(f"{outlet}: {len(wrong)} event(s) that belong on the other outlet, e.g. {wrong[:3]}")
        by_cart: dict[object, list[Seen]] = defaultdict(list)
        for r in sorted(records, key=lambda r: (r.partition, r.offset)):
            by_cart[r.event["cartId"]].append(r)
        for cart, rs in by_cart.items():
            parts = {r.partition for r in rs}
            if len(parts) != 1:
                problems.append(f"{outlet}: {cart} is spread over partitions {sorted(parts)}")
            if any(r.key != str(cart).encode() for r in rs):
                problems.append(f"{outlet}: {cart} arrived with a different key")
            first_seen: list[int] = []
            for r in rs:
                s = int(str(r.event["seq"]))
                if s not in first_seen:
                    first_seen.append(s)
            if first_seen != sorted(first_seen):
                problems.append(f"{outlet}: {cart} out of order: {first_seen}")
        for r in records:
            source = expected.get((r.event["cartId"], r.event["seq"]))
            if source is not None and r.headers != source.headers:
                problems.append(f"{outlet}: headers of {r.event['cartId']}-{r.event['seq']} changed: {r.headers}")
                break
            if [k for k, _ in r.headers] != list(HEADERS):
                problems.append(f"{outlet}: header names {[k for k, _ in r.headers]} are not {list(HEADERS)}")
                break

    print(f"input: {len(expected)} events; valid: {len(valid)} records; review: {len(review)} records; repeats: {repeats}")
    for p in problems:
        print(f"FAIL: {p}")
    if not problems:
        print("OK: every event on its outlet, each cart in order on one partition, headers intact")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())

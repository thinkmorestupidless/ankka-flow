"""Check the graph the sink built from `produce.py`'s notices, reading Neo4j with its own driver.

    uv run python verify.py                           # 20 notices over 5 carts, as produce.py's defaults
    uv run python verify.py --count 200 --carts 20

Exits 0 when the graph has one Cart per cart, one Checkout per notice, one CHECKED_OUT edge from
each checkout's cart, and every element's _version equal to its notice's time; waits up to a
minute for the sink to catch up.
"""

from __future__ import annotations

import argparse
import sys
import time

from neo4j import GraphDatabase


def snapshot(session: object) -> tuple[int, int, int, int]:
    carts = session.run("MATCH (c:Cart) RETURN count(c) AS n").single()["n"]  # type: ignore[attr-defined]
    checkouts = session.run("MATCH (k:Checkout) RETURN count(k) AS n").single()["n"]  # type: ignore[attr-defined]
    edges = session.run(  # type: ignore[attr-defined]
        "MATCH (c:Cart)-[r:CHECKED_OUT]->(k:Checkout) WHERE c.cartId = k.cartId RETURN count(r) AS n"
    ).single()["n"]
    wrong = session.run(  # type: ignore[attr-defined]
        "MATCH (k:Checkout) WHERE k._version <> toInteger(split(k.id, ':')[2]) RETURN count(k) AS n"
    ).single()["n"]
    return carts, checkouts, edges, wrong


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--uri", default="bolt://localhost:7687")
    parser.add_argument("--password", default="flow-local-password")
    parser.add_argument("--count", type=int, default=20)
    parser.add_argument("--carts", type=int, default=5)
    args = parser.parse_args()
    expected = (args.carts, args.count, args.count, 0)
    with GraphDatabase.driver(args.uri, auth=("neo4j", args.password)) as driver:
        deadline = time.monotonic() + 60
        seen = (0, 0, 0, 0)
        while time.monotonic() < deadline:
            with driver.session() as session:
                seen = snapshot(session)
            if seen == expected:
                print(f"ok: {seen[0]} carts, {seen[1]} checkouts, {seen[2]} CHECKED_OUT edges, versions as sent")
                return
            time.sleep(1)
    print(f"expected (carts, checkouts, edges, wrong versions) = {expected}, found {seen}", file=sys.stderr)
    sys.exit(1)


if __name__ == "__main__":
    main()

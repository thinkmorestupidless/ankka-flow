import json

from ankka_flow.testkit import Harness, hash_partitioner

from checkout_graph.mapper import CheckoutGraph


def notice(cart: str, at: int) -> bytes:
    """A checkout notice as ankka publishes it: a plain JSON body."""
    return json.dumps({"cartId": cart, "at": at}).encode()


# docs:start mapper-test
def test_a_notice_becomes_a_cart_a_checkout_and_the_edge_between_them() -> None:
    h = Harness(CheckoutGraph())
    h.inlet("in").put(key=b"cart-1", value=notice("cart-1", 1_790_000_000_000), headers=[("ce-id", b"n-1")])
    h.run()
    deltas = [json.loads(r.value) for r in h.outlet("deltas").records]
    assert [(d["kind"], d["id"]) for d in deltas] == [
        ("node", "cart:cart-1"),
        ("node", "checkout:cart-1:1790000000000"),
        ("edge", "checked-out:cart-1:1790000000000"),
    ]
    assert all(d["version"] == 1_790_000_000_000 for d in deltas)
    assert deltas[1]["properties"]["checkedOutAt"] == "2026-09-21T14:13:20.000+00:00"
    assert deltas[2]["from"] == "cart:cart-1" and deltas[2]["to"] == "checkout:cart-1:1790000000000"
    # keyed by element id, with ankka's headers carried along
    assert [r.key for r in h.outlet("deltas").records] == [d["id"].encode() for d in deltas]
    assert all(r.headers == [("ce-id", b"n-1")] for r in h.outlet("deltas").records)
    assert h.skipped == []
    # docs:end mapper-test


def test_a_record_that_is_not_a_notice_is_skipped() -> None:
    h = Harness(CheckoutGraph())
    h.inlet("in").put(key=b"cart-2", value=b"not json")
    h.inlet("in").put(key=b"cart-3", value=b'{"cartId": "cart-3"}')
    h.run()
    assert h.failures == []
    assert h.outlet("deltas").records == []
    assert len(h.skipped) == 2


def test_each_cart_is_mapped_in_order() -> None:
    h = Harness(CheckoutGraph())
    for i in range(30):
        cart = f"cart-{i % 5}"
        h.inlet("in").put(key=cart.encode(), value=notice(cart, 1_790_000_000_000 + i))
    h.run(partitions=hash_partitioner(3), max_records=4)
    carts = [json.loads(r.value) for r in h.outlet("deltas").records if r.key.startswith(b"cart:")]
    for cart in {c["id"] for c in carts}:
        versions = [c["version"] for c in carts if c["id"] == cart]
        assert versions == sorted(versions)


def test_the_committed_descriptors_are_what_the_sdk_and_the_sidecar_write() -> None:
    import pathlib

    here = pathlib.Path(__file__).resolve().parent.parent
    root = here.parent.parent
    assert (here / "flow-mapper" / "descriptor.json").read_text() == (here / "flow" / "descriptor.json").read_text()
    assert (here / "flow-graph" / "descriptor.json").read_text() == (
        root / "protocol" / "fixtures" / "builtin" / "neo4j-merge-sink.json"
    ).read_text()

import json
import pathlib

from ankka_flow.testkit import Harness, hash_partitioner

from cart_router.router import CartRouter

HERE = pathlib.Path(__file__).resolve().parent.parent
FIXTURE = HERE.parent.parent / "protocol" / "fixtures" / "descriptors" / "cart-router.json"


def event(cart: str, total: int) -> bytes:
    return json.dumps({"cartId": cart, "total": total}).encode()


def test_routes_by_total() -> None:
    h = Harness(CartRouter(), config={"review-threshold": 50})
    h.inlet("in").put(key=b"cart-1", value=event("cart-1", 10), headers=[("ce_type", b"ItemAdded")])
    h.inlet("in").put(key=b"cart-2", value=event("cart-2", 99))
    h.run()
    assert [r.key for r in h.outlet("valid").records] == [b"cart-1"]
    assert [r.key for r in h.outlet("review").records] == [b"cart-2"]
    assert h.outlet("valid").records[0].headers == [("ce_type", b"ItemAdded")]


def test_each_cart_stays_in_order() -> None:
    h = Harness(CartRouter())
    for i in range(50):
        cart = f"cart-{i % 10}"
        h.inlet("in").put(key=cart.encode(), value=event(cart, i * 5))
    h.run(partitions=hash_partitioner(3), max_records=7)
    assert h.failures == [] and h.skipped == []
    everything = h.outlet("valid").records + h.outlet("review").records
    assert len(everything) == 50
    for cart in {r.key for r in everything}:
        totals = [json.loads(r.value)["total"] for r in everything if r.key == cart]
        assert sorted(totals) == sorted(set(totals))


def test_committed_descriptor_matches_the_fixture_and_the_declaration() -> None:
    committed = json.loads((HERE / "flow" / "descriptor.json").read_text())
    fixture = json.loads(FIXTURE.read_text())
    assert committed["streamlet"] == fixture["streamlet"]
    assert committed["sdk"]["name"] == "ankka-flow-python"

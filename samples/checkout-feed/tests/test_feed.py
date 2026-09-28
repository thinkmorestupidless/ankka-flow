import json

from ankka_flow.testkit import Harness, hash_partitioner

from checkout_feed.feed import CheckoutFeed


def notice(cart: str, at: int) -> bytes:
    """A checkout notice as ankka publishes it: a plain JSON body."""
    return json.dumps({"cartId": cart, "at": at}).encode()


# docs:start feed-test
def test_turns_notices_into_checkouts() -> None:
    h = Harness(CheckoutFeed())
    h.inlet("in").put(key=b"cart-1", value=notice("cart-1", 1_790_000_000_000), headers=[("ce-id", b"n-1")])
    h.inlet("in").put(key=b"cart-2", value=b"not json")
    h.run()
    [checkout] = h.outlet("checkouts").records
    assert checkout.key == b"cart-1"
    assert checkout.headers == [("ce-id", b"n-1")]
    assert json.loads(checkout.value) == {"cartId": "cart-1", "checkedOutAt": "2026-09-21T14:13:20.000+00:00"}
    assert [r.key for r in h.skipped] == [b"cart-2"]
    # docs:end feed-test


def test_a_notice_missing_a_field_is_skipped_not_failed() -> None:
    h = Harness(CheckoutFeed())
    h.inlet("in").put(key=b"cart-1", value=b'{"cartId": "cart-1"}')
    h.inlet("in").put(key=b"cart-2", value=b'["cart-2"]')
    h.run()
    assert h.failures == []
    assert h.outlet("checkouts").records == []
    assert len(h.skipped) == 2


def test_each_cart_stays_in_order() -> None:
    h = Harness(CheckoutFeed())
    for i in range(40):
        cart = f"cart-{i % 8}"
        h.inlet("in").put(key=cart.encode(), value=notice(cart, 1_790_000_000_000 + i))
    h.run(partitions=hash_partitioner(3), max_records=5)
    everything = h.outlet("checkouts").records
    assert len(everything) == 40
    for cart in {r.key for r in everything}:
        times = [json.loads(r.value)["checkedOutAt"] for r in everything if r.key == cart]
        assert times == sorted(times)

# Test a streamlet

> Test a Python streamlet's logic with the SDK's Harness, which runs process over in-memory inlets and outlets with the protocol's rules and no Kafka, sidecar or gRPC.

Source: https://flow.ankka.cloud/build/testing/
`ankka_flow.testkit.Harness` runs a streamlet's `process` over records held in memory. It builds
batches the way the sidecar would, one partition at a time and in offset order, applies the protocol's
rules, and records what reached each outlet, which records were skipped, and which batches failed.
There is no Kafka, no sidecar and no gRPC, so a test runs in milliseconds with plain `pytest`.

## Route and assert

```python
def test_routes_by_total() -> None:
    h = Harness(CartRouter(), config={"review-threshold": 50})
    h.inlet("in").put(key=b"cart-1", value=event("cart-1", 10), headers=[("ce_type", b"ItemAdded")])
    h.inlet("in").put(key=b"cart-2", value=event("cart-2", 99))
    h.run()
    assert [r.key for r in h.outlet("valid").records] == [b"cart-1"]
    assert [r.key for r in h.outlet("review").records] == [b"cart-2"]
    assert h.outlet("valid").records[0].headers == [("ce_type", b"ItemAdded")]
```

`event` is a helper in the test file that encodes a cart event as JSON bytes.

- `Harness(streamlet, config=...)` applies parameter values as the sidecar's `Start` would. A key the
  streamlet does not declare raises `KeyError`; a parameter left out takes its default.
- `h.inlet(name).put(value=..., key=..., headers=...)` queues a record on a declared inlet. Naming an
  undeclared inlet or outlet raises `KeyError` listing the declared ones.
- `h.run()` processes everything queued so far. It can be called again after more `put`s; offsets carry
  on from where they stopped.
- `h.outlet(name).records` lists the records emitted to that outlet, in order, as `Record` values.

## Partitions and batch sizes

By default every record goes to partition 0 and each partition's records form one batch. Two arguments
to `run` change that, to test what a streamlet assumes about ordering:

- `partitions` is a function from a key to a partition number. `hash_partitioner(n)` from
  `ankka_flow.testkit` spreads keys over `n` partitions by a stable hash, as Kafka keeps one key on one
  partition.
- `max_records` caps the size of a batch, so one partition's records arrive over several batches.

```python
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
```

The Harness runs batches one after another. It does not reproduce concurrency between partitions,
rebalances or redelivery; those belong to the sidecar and are proven by the platform's own suites.

## Skips and failures

The Harness applies the same rules as a running pipeline:

- A batch whose `process` raises, or yields an emit to an undeclared outlet, fails. Its emits are
  discarded and a `Failure(batch, error)` is appended to `h.failures`.
- A record of a successful batch that no emit was derived from is appended to `h.skipped`. An emit is
  derived from a record when it was made with `outlet.emit(record, ...)`; an emit built from `value=`
  alone is not, so its source record counts as skipped.
- `h.batches` lists every batch `process` was given, in order.

Assert on `h.failures` and `h.skipped` as well as on the outlets, so that a record dropped or a batch
failed by mistake does not pass unnoticed. For a streamlet meant to skip records it cannot decode:

```python
h.inlet("in").put(key=b"cart-3", value=b"not json")
h.run()
assert h.failures == []                       # skipped, not raised on
assert [r.key for r in h.skipped] == [b"cart-3"]
```

The cart router raises on a value that is not JSON, so for it this test would find one failure.

## Check the descriptor in CI

A streamlet whose committed descriptor is stale will be refused by `flow verify` or by the sidecar.
Run the check beside the tests:

```bash
uv run pytest -q
uv run descriptor --check     # exits 1 when flow/descriptor.json differs from the declaration
```

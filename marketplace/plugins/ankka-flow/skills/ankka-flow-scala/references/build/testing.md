# Test a streamlet

> Test a streamlet's logic with the SDK's Harness, in Scala or Python, which runs process over in-memory inlets and outlets with the protocol's rules and no Kafka, sidecar or gRPC.

Source: https://flow.ankka.cloud/build/testing/
Each SDK has a `Harness` that runs a streamlet's `process` over records held in memory: in Scala
`com.thinkmorestupidless.ankka.flow.sdk.testkit.Harness`, in Python `ankka_flow.testkit.Harness`. It
builds batches the way the sidecar would, one partition at a time and in offset order, applies the
protocol's rules, and records what reached each outlet, which records were skipped, and which batches
failed. There is no Kafka, no sidecar and no gRPC, so a test runs in milliseconds with the language's
own test runner: munit or any other in Scala, `pytest` in Python. The two harnesses have the same
members and the same rules.

## Route and assert

**Scala**

```scala
test("routes by total") {
  val h = Harness(new CartRouter, Map("review-threshold" -> "50"))
  h.inlet("in")
    .put(
      event("cart-1", 10),
      key = Some(bytes("cart-1")),
      headers = Seq("ce_type" -> bytes("ItemAdded"))
    )
  h.inlet("in").put(event("cart-2", 99), key = Some(bytes("cart-2")))
  h.run()
  assertEquals(h.outlet("valid").records.flatMap(_.keyString), Vector("cart-1"))
  assertEquals(h.outlet("review").records.flatMap(_.keyString), Vector("cart-2"))
  assertEquals(
    h.outlet("valid").records.head.headers.map((k, v) => k -> new String(v, UTF_8)),
    Seq("ce_type" -> "ItemAdded")
  )
}
```

**Python**

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

- The harness is given the streamlet and its parameter values, as the sidecar's `Start` would give
  them. A key the streamlet does not declare is refused; a parameter left out takes its default.
- `inlet(name).put(...)` queues a record on a declared inlet: a value, and optionally a key and
  headers. Naming an undeclared inlet or outlet is refused, listing the declared ones.
- `run()` processes everything queued so far. It can be called again after more `put`s; offsets carry
  on from where they stopped.
- `outlet(name).records` lists the records emitted to that outlet, in order, as `Record` values.

## Partitions and batch sizes

By default every record goes to partition 0 and each partition's records form one batch. Two arguments
to `run` change that, to test what a streamlet assumes about ordering:

- `partitions` is a function from a key to a partition number. The hash partitioner
  (`Harness.hashPartitioner(n)` in Scala, `hash_partitioner(n)` in Python) spreads keys over `n`
  partitions by the CRC-32 of the key, as Kafka keeps one key on one partition. Both SDKs place a key
  on the same partition.
- The batch size cap (`maxRecords` in Scala, `max_records` in Python) bounds a batch, so one
  partition's records arrive over several batches.

**Scala**

```scala
test("each cart stays in order") {
  val h = Harness(new CartRouter)
  (0 until 50).foreach { i =>
    val cart = s"cart-${i % 10}"
    h.inlet("in").put(event(cart, i * 5), key = Some(bytes(cart)))
  }
  h.run(partitions = Harness.hashPartitioner(3), maxRecords = Some(7))
  assert(h.failures.isEmpty && h.skipped.isEmpty)
  val everything = h.outlet("valid").records ++ h.outlet("review").records
  assertEquals(everything.size, 50)
  everything.groupBy(_.keyString).values.foreach { ofOneCart =>
    val totals =
      ofOneCart.map(r => Json.parse(r.valueString).toOption.flatMap(_.field("total")).get)
    assertEquals(totals.distinct.size, totals.size)
  }
}
```

**Python**

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

- A batch whose `process` throws or raises, or emits to an undeclared outlet, fails. Its emits are
  discarded and a `Failure(batch, error)` is added to `failures`.
- A record of a successful batch that no emit was derived from is added to `skipped`. An emit is
  derived from a record when it was made from that record: `outlet.emit(record)`, or in Scala
  `outlet.emit(record.copy(...))` and in Python `outlet.emit(record, ...)`. An emit built from a value
  alone is not, so its source record counts as skipped.
- `batches` lists every batch `process` was given, in order.

Assert on `failures` and `skipped` as well as on the outlets, so that a record dropped or a batch
failed by mistake does not pass unnoticed. For a Python streamlet meant to skip records it cannot
decode:

```python
h.inlet("in").put(key=b"cart-3", value=b"not json")
h.run()
assert h.failures == []                       # skipped, not raised on
assert [r.key for r in h.skipped] == [b"cart-3"]
```

The same test in Scala asserts `h.failures.isEmpty` and `h.skipped.flatMap(_.keyString) ==
Vector("cart-3")`. Neither cart router is such a streamlet: both fail the batch on a value that is
not JSON, so for them this test finds one failure.

## Check the descriptor in CI

A streamlet whose committed descriptor is stale will be refused by `flow verify` or by the sidecar.
Run the check beside the tests:

**Scala**

```bash
sbt test
sbt "runMain com.thinkmorestupidless.ankka.flow.sdk.Descriptor cart.CartRouter flow/descriptor.json --check"
```

In the ankka-flow repository the Scala sample has the two as tasks: `sbt cartRouterScala/test
cartRouterScala/descriptorCheck`.

**Python**

```bash
uv run pytest -q
uv run descriptor --check     # exits 1 when flow/descriptor.json differs from the declaration
```

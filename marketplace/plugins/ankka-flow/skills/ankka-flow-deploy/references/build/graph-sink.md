# Build a graph from a pipeline

> Turn a service's events into a Neo4j graph — choose ids and versions, map events to keyed graph deltas in a streamlet, and wire the built-in Neo4j merge sink behind it.

Source: https://flow.ankka.cloud/build/graph-sink/
A graph built from several services' events is the kind of job a pipeline does well: every service
publishes what happened to it, and a pipeline projects those events into one eventually consistent
graph. ankka-flow splits the job in two. The part that is the same for every domain — merging into
Neo4j so that redelivery, reordering and a rebuild from the start all leave the same graph — is the
built-in [Neo4j merge sink](../reference/neo4j-merge-sink.md). The part that is different every time —
which events become which nodes and edges — is a streamlet you write, which maps events to
[graph deltas](../reference/graph-deltas.md).

This page builds the pipeline in
[`samples/checkout-graph`](https://github.com/thinkmorestupidless/ankka-flow/blob/main/samples/checkout-graph):
ankka's shopping cart publishes a checkout notice to `cart-checkouts` whenever a cart is checked out,
and the graph gains a `Cart`, a `Checkout` and a `CHECKED_OUT` edge between them.

```text
shopping-cart (ankka) ──► cart-checkouts ──► mapper ──► graph-deltas ──► graph (Neo4j merge sink) ──► Neo4j
```

A service that publishes its own graph deltas needs none of this: its pipeline is the sink alone, as
[Fill a graph from an ankka service](graph-from-ankka.md) shows. Reach for a mapper when the graph is
fed by topics the services already publish for other reasons, or by several services at once.

## Decide the ids and the versions

Every element of the graph needs a global id and a version that only rises.

- **Ids** are stable strings, prefixed by the kind of thing so two sources cannot collide:
  `cart:cart-1`, `checkout:cart-1:1790627790360`.
- **Versions** come from the source entity's own history: its sequence number, or an event time in
  milliseconds when there is at most one event per entity per millisecond. The shopping cart's notice
  carries `at`, the time of the checkout, and each cart is checked out once per notice, so `at` is the
  version of everything the notice produces.
- **One writer per element.** Two source entities never write the same element; a node several
  sources describe is modelled as several nodes joined by edges.

## Map events to deltas

The mapper reads the notice, decides what it means for the graph, and emits one delta per element
through a `GraphDeltaOutlet`. The outlet builds each record and gives it its
[element key](../reference/graph-deltas.md#the-record-key), `node:<id>` or `edge:<id>`, so every delta
for one element is applied in order and the topic can be compacted. The mapper never chooses a key.
The sample is Python; a Scala mapper declares the same port with `graphDeltaOutlet("deltas")` and
builds deltas with its `node`, `edge`, `tombstoneNode` and `tombstoneEdge` methods, as
[Write a streamlet in Scala](scala-streamlet.md) and the [Scala SDK reference](../reference/scala-sdk.md#graph-deltas) show:

```python
import logging
from collections.abc import Iterable
from datetime import UTC, datetime

from ankka_flow import Batch, Emit, GraphDeltaOutlet, JsonInlet, Streamlet, json

log = logging.getLogger(__name__)


class CheckoutGraph(Streamlet):
    name = "checkout-graph"
    description = "Maps checkout notices to graph deltas: a cart, a checkout, and the edge between them."
    notices = JsonInlet("in", schema_name="ankka.checkout-notice.v1")
    deltas = GraphDeltaOutlet("deltas")

    def process(self, batch: Batch) -> Iterable[Emit]:
        for record in batch:
            try:
                notice = json.loads(record.value)
                cart, at = str(notice["cartId"]), int(notice["at"])
            except (ValueError, KeyError, TypeError):
                log.warning("skipping a record at offset %d that is not a checkout notice", record.offset)
                continue
            cart_id, checkout_id = f"cart:{cart}", f"checkout:{cart}:{at}"
            checked_out_at = datetime.fromtimestamp(at / 1000, UTC).isoformat(timespec="milliseconds")
            # Each delta is the element's whole state, versioned by the notice's time. The outlet
            # keys each record by its element (`node:<id>`, `edge:<id>`), so every delta for one
            # element is applied in order and a compacted topic keeps the latest of each.
            yield self.deltas.node(record, id=cart_id, version=at, labels=["Cart"], properties={"cartId": cart})
            yield self.deltas.node(
                record,
                id=checkout_id,
                version=at,
                labels=["Checkout"],
                properties={"cartId": cart, "checkedOutAt": checked_out_at},
            )
            yield self.deltas.edge(
                record,
                id=f"checked-out:{cart}:{at}",
                version=at,
                type="CHECKED_OUT",
                from_id=cart_id,
                to_id=checkout_id,
            )
```

Each delta is the element's whole state. Nothing about Neo4j appears in the mapper: it knows the
contract, not the database. A record that is not a notice is skipped by emitting nothing for it,
which is the mapper's decision to make. Passing `record` derives each delta from the input record,
so ankka's CloudEvents headers travel with it, while the key is the element's and not the notice's.
The outlet refuses what the sink would refuse — an empty id, a negative version, a label that is not
an identifier, a nested property — by raising in the mapper, where the mistake is.

A mapper in a language with no SDK helper writes the same JSON and sets the record key itself; the
sink refuses a delta under any other key.

The harness proves the mapping with no Kafka, sidecar or database:

```python
def test_a_notice_becomes_a_cart_a_checkout_and_the_edge_between_them() -> None:
    h = Harness(CheckoutGraph())
    h.inlet("in").put(key=b"cart-1", value=notice("cart-1", 1_790_000_000_000), headers=[("ce-id", b"n-1")])
    h.run()
    records = h.outlet("deltas").records
    deltas = [graph.read(r) for r in records]
    assert [(d.kind, d.id) for d in deltas] == [
        ("node", "cart:cart-1"),
        ("node", "checkout:cart-1:1790000000000"),
        ("edge", "checked-out:cart-1:1790000000000"),
    ]
    assert all(d.version == 1_790_000_000_000 for d in deltas)
    assert deltas[1].properties["checkedOutAt"] == "2026-09-21T14:13:20.000+00:00"
    assert (deltas[2].from_id, deltas[2].to_id) == ("cart:cart-1", "checkout:cart-1:1790000000000")
    # each record is keyed by its element, never by the cart the notice was keyed by
    assert [r.key for r in records] == [
        b"node:cart:cart-1",
        b"node:checkout:cart-1:1790000000000",
        b"edge:checked-out:cart-1:1790000000000",
    ]
    # ankka's headers are carried along, and the notice was not skipped
    assert all(r.headers == [("ce-id", b"n-1")] for r in records)
    assert h.skipped == []
```

## Wire the sink

The sink is a streamlet whose descriptor is built in. Name it `builtin/neo4j-merge-sink`; it needs no
image and no descriptor file:

```hocon
blueprint {
  name = checkouts-graph
  streamlets {
    mapper = checkout-graph
    # Built into the sidecar: no image, and a pod with only the sidecar in it.
    graph  = builtin/neo4j-merge-sink
  }
  topics {
    # Published by the ankka shopping cart's CheckoutNotifier. The platform only reads it.
    cart-checkouts {
      managed    = false
      topic.name = "cart-checkouts"
      cluster    = default
      consumers  = [mapper.in]
      consumer-config { auto.offset.reset = earliest }
    }
    graph-deltas {
      producers  = [mapper.deltas]
      consumers  = [graph.in]
      partitions = 3
      replicas   = 1
    }
  }
}
```

`flow verify` checks the sink's inlet against `mapper.deltas` like any pair of ports: an outlet of any
contract other than `ankka.graph-delta.v1` is refused before anything is deployed.

`graph-deltas` sets no `cleanup.policy`, and it carries graph deltas, so it is compacted by default:
`flow generate` writes `cleanup.policy: compact` into the resource and the operator creates the topic
compacted. It then keeps the latest delta of every element for as long as the pipeline lives.

The sink's `secret` parameter has no default, so verification needs the deploy-time configuration
that names the Secret, as generation does:

```bash
flow verify blueprint.conf --descriptors flow --conf k8s/in-cluster.conf
```

```text
note: Topic 'graph-deltas' carries graph deltas and is compacted (cleanup.policy = compact).
verified: 2 streamlets, 2 topics
```

## Give it a connection

The sink reaches Neo4j through a Secret in the pipeline's namespace, named by its `secret` parameter,
with the keys `uri`, `username`, `password` and optionally `database`. In the sample's deploy-time
configuration:

```hocon
flow.streamlets.graph.config { secret = neo4j-local }
```

The operator mounts the Secret into the sink's sidecar and refuses the resource if the Secret is
missing or incomplete. `just neo4j-up` installs a development Neo4j and the `neo4j-local` Secret on a
local cluster.

## Run it on a laptop

The sample's compose file runs Kafka, Neo4j and two sidecars: the mapper's, which calls the mapper on
the host, and the sink's, which runs the built-in stage from this configuration and a directory of
credential files:

```hocon
# The merge sink's sidecar on the compose network: a stage block and no process. In a cluster the
# operator renders the same file and mounts the connection Secret at /etc/flow/neo4j.
# descriptor.json beside it is the built-in's, a copy of protocol/fixtures/builtin/neo4j-merge-sink.json.
flow {
  pipeline  = checkouts-graph
  streamlet = graph
  config    = { secret = "neo4j-local", transaction-timeout = "30s" }
  stage {
    name = "neo4j-merge-sink"
    neo4j { credentials-dir = "/etc/flow/neo4j" }
  }
  inlets {
    in {
      topic             = "checkouts-graph.graph-deltas"
      bootstrap.servers = "kafka:9092"
      consumer-config { auto.offset.reset = earliest }
      batch { max-records = 500, max-bytes = 1 MiB }
    }
  }
}
```

```bash
(cd ../.. && sbt sidecar/docker:publishLocal)
cd samples/checkout-graph
uv sync && uv run pytest -q && uv run descriptor --check
uv run python produce.py                          # creates the topics, then 20 notices over 5 carts
docker compose up -d
uv run python -m checkout_graph.main &
uv run python verify.py                           # 5 carts, 20 checkouts, 20 edges
uv run python produce.py && uv run python verify.py   # the same notices again: the graph is unchanged
```

The second run is the point: every delta arrives again, every one is stale, and the graph does not
change.

## Deploy it beside ankka

With the shopping cart publishing to `cart-checkouts` (see
[Read an ankka service's topic](ankka-topics.md)) and ankka-flow installed:

```bash
just neo4j-up
docker build -f samples/checkout-graph/Dockerfile -t sample-checkout-graph .
kind load docker-image --name ankka sample-checkout-graph
flow generate samples/checkout-graph/blueprint.conf --descriptors samples/checkout-graph/flow \
  --conf samples/checkout-graph/k8s/in-cluster.conf \
  --image mapper=sample-checkout-graph:latest -n shop | kubectl apply -f -
kubectl -n shop get aflow checkouts-graph -w
```

The sink's pod has one container. Check a cart out through the shopping cart's API, then read the
graph:

```bash
kubectl -n neo4j exec neo4j-0 -- cypher-shell -u neo4j -p flow-local-password \
  'MATCH (c:Cart)-[:CHECKED_OUT]->(k:Checkout) RETURN c.cartId, k.checkedOutAt'
```

## Rebuild it

The graph is a projection of the topics, so it can be rebuilt from them, in two ways.

From the delta topic alone: scale the sink to zero, empty the database, run
`flow reset checkouts-graph --streamlet graph`, and scale it up. The sink reads the compacted topic,
about one record per element, and the mapper is not involved. See
[Rebuild a graph from its delta topic](../deploy/rebuild-a-graph.md).

From the services' events: scale both streamlets to zero, run `flow reset checkouts-graph`, and scale
them up. Every event is mapped and every delta merged again, and the graph ends as it was. This is the
one to use after changing the mapping. See [Rebuild from the start](../deploy/reset.md).

## Watch it

The sink's pod exports `ankka_flow_stage_deltas_written_total`,
`ankka_flow_stage_deltas_stale_total`, `ankka_flow_stage_batches_failed_total` and
`ankka_flow_stage_delete_markers_total` beside the inlet's lag. When Neo4j is unreachable, the pod goes not ready, the lag grows, nothing is committed, and a
`PartitionStalled` warning names the error; when it comes back, the backlog drains. See
[Observe a pipeline](../deploy/observe.md).

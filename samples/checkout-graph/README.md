# The checkout graph

A graph built by a pipeline beside ankka: the shopping cart publishes a checkout notice to
`cart-checkouts` whenever a cart is checked out; a Python streamlet maps each notice to three graph
deltas (a `Cart` node, a `Checkout` node and a `CHECKED_OUT` edge); and the Neo4j merge sink, built
into the sidecar, merges them into Neo4j in one transaction per batch. The walkthrough is
[Build a graph from a pipeline](https://flow.ankka.cloud/build/graph-sink/).

| file | what |
|---|---|
| `src/checkout_graph/mapper.py` | the mapper: notices in, deltas out |
| `flow/descriptor.json` | written by `uv run descriptor`; committed; checked by `uv run descriptor --check` |
| `blueprint.conf` | the pipeline: ankka's topic, unmanaged; the mapper; the deltas topic; `graph = builtin/neo4j-merge-sink` |
| `k8s/in-cluster.conf` | the sink's connection Secret on the kind cluster |
| `docker-compose.yml` | Kafka, Neo4j, the mapper's sidecar and the sink's sidecar, for the laptop |
| `flow-mapper/`, `flow-graph/` | each sidecar's `streamlet.conf` and `descriptor.json` on the compose network |
| `neo4j-secret/` | what the operator mounts from the connection Secret: one file per key (a local-only password) |
| `produce.py` | checkout notices as the shopping cart writes them |
| `verify.py` | reads Neo4j and checks the graph |
| `bench.py` | the sink's throughput per partition |

## On a laptop

```bash
(cd ../.. && sbt sidecar/docker:publishLocal)     # the sidecar image, with the built-in stage
uv sync
uv run pytest -q                                  # the harness: no Kafka, no sidecar
uv run descriptor --check
uv run python produce.py                          # creates the topics, then 20 notices over 5 carts
docker compose up -d                              # kafka, neo4j and both sidecars
uv run python -m checkout_graph.main &            # the mapper on 127.0.0.1:9010
uv run python verify.py                           # exit 0: 5 carts, 20 checkouts, 20 edges
uv run python produce.py && uv run python verify.py   # the same notices again: the graph is unchanged
```

The graph is in Neo4j's browser at <http://localhost:7474> (`neo4j` / `flow-local-password`):

```cypher
MATCH (c:Cart)-[:CHECKED_OUT]->(k:Checkout) RETURN c, k LIMIT 50
```

Throughput, with nothing else running:

```bash
docker compose down && docker compose up -d kafka neo4j
uv run python bench.py
```

## The image

```bash
docker build -f samples/checkout-graph/Dockerfile -t sample-checkout-graph .   # from the repository root
```

## On a kind cluster beside ankka

ankka's shopping cart deployed with `ANKKA_KAFKA_BOOTSTRAP_SERVERS` (see `samples/checkout-feed`),
ankka-flow installed with `just up`, then a development Neo4j and the sink's Secret:

```bash
just neo4j-up                                                     # from the repository root
kind load docker-image --name ankka sample-checkout-graph
flow generate blueprint.conf --descriptors flow --conf k8s/in-cluster.conf \
  --image mapper=sample-checkout-graph:latest -n shop | kubectl apply -f -
kubectl -n shop get aflow checkouts-graph -w                      # Ready
kubectl -n neo4j exec neo4j-0 -- cypher-shell -u neo4j -p flow-local-password \
  'MATCH (c:Cart)-[:CHECKED_OUT]->(k:Checkout) RETURN c.cartId, k.checkedOutAt'
```

`graph` needs no image: its pod has only the sidecar, which runs the stage, and the operator mounts
the `neo4j-local` Secret into it.

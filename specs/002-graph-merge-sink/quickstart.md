# Quickstart: proving the graph merge sink works

**Feature**: [spec.md](./spec.md) | **Plan**: [plan.md](./plan.md) | **Contracts**: [contracts/](./contracts/)

Six tiers, cheapest first; each gates the next. Tiers 1 needs only a JDK; 2 and 3 need Docker; 4
needs Python 3.12 and `uv`; 5 runs the k3s suite; 6 needs a kind cluster with ankka in it.

## Tier 1 — the pure pieces (seconds, no Docker)

```bash
sbt 'protocol/testOnly *BuiltinsSuite'           # the built-in's canonical JSON equals the committed fixture and validates
sbt blueprint/test                                # builtin/ lookup, UnknownBuiltin, no shadowing
sbt 'cli/testOnly *VerifySuite *GenerateSuite'   # verified without a descriptor file; no image needed; an image refused; builtin: true
sbt 'crd/testOnly *CrdSchemaSuite'               # builtin in the schema; image no longer required; round trip
sbt 'operator/testOnly *RenderingSuite *StreamletFilesSuite'
cd sdks/python && uv run python scripts/proto.py && git diff --exit-code proto/   # the fixture copy is current
```

Expected: `graph = builtin/neo4j-merge-sink` verifies against a producer of `ankka.graph-delta.v1`
and is refused, naming both ports, against any other contract (SC-003); `generate` writes `builtin:
true` and `image: ""`; rendering yields one container with the `neo4j` mount, no
`FLOW_PROCESS_ADDRESS`, a `stage.neo4j` block without the password, and refusals for a missing
Secret, a Secret without `password`, and an image on a built-in (FR-019–FR-023).

## Tier 2 — the stage against Neo4j (a minute)

```bash
sbt 'sidecar/testOnly *Neo4jMergeSuite'
```

A `neo4j:5.26-community` container from `-Dflow.neo4j.image`. Expected: every row of the state
table in data-model.md; labels replaced; properties removed when absent; placeholders created and
replaced; ties and lower versions stale; a batch folded to one delta per id; each malformed shape
in contracts/graph-delta.md fails the batch naming the offset; numbers and arrays round-trip; a
wrong password fails to open with a message that does not contain it; a user without `CREATE
CONSTRAINT` gets `ConstraintNotCreated` and the stage opens anyway (US1, FR-007–FR-011, FR-015,
FR-016).

## Tier 3 — the sidecar in stage mode, end to end (a few minutes)

```bash
sbt 'sidecar/testOnly *Neo4jSinkKafkaSuite'
sbt mutationCheck                                 # still fails with the commit moved first
```

Kafka and Neo4j containers, the sidecar through `SidecarRun` with a `stage` block. Expected: not
ready until Neo4j answers, ready once every inlet is subscribed; committed offsets never exceed
what the graph holds; Neo4j paused mid-run → not ready within the readiness period, lag grows, the
stall warning carries the Neo4j error, nothing committed; unpaused → drained, every delta once;
killed between transaction and commit → redelivered and found stale, graph unchanged; the group
reset to the start → an identical graph; a `descriptor.json` that is not the built-in → exit 1
(SC-001, SC-002, SC-005, FR-012–FR-014, FR-026).

## Tier 4 — the sample on a laptop (Python)

```bash
sbt sidecar/docker:publishLocal
cd samples/checkout-graph
uv sync && uv run pytest -q && uv run descriptor --check
docker compose up -d                              # kafka, neo4j, the mapper's sidecar, the sink's sidecar
uv run python -m checkout_graph.main &            # the mapper on 127.0.0.1:9010
uv run python produce.py                          # twenty checkout notices over five carts, as ankka would write them
uv run python verify.py                           # queries Neo4j: five Cart nodes, twenty Checkout nodes, twenty CHECKED_OUT edges
uv run python produce.py && uv run python verify.py   # the same again: unchanged
uv run python bench.py                            # SC-007: deltas/s per partition, recorded in research.md
```

Expected: the mapper's `Harness` tests pass; the descriptor matches the SDK's output; the graph is
identical after a second production of the same notices; the bench sustains ≥ 1,000 deltas/s per
partition (US4, SC-006, SC-007).

## Tier 5 — the operator on k3s (the long suite)

```bash
sbt 'operator/testOnly *FlowClusterSuite'          # or `sbt operator/test`; -Dflow.cluster.tests=off skips it
```

Expected: the existing scenarios, plus one with a Neo4j Deployment in the cluster, the sample's
mapper image and the sink: the sink pod has exactly one container, the Secret is mounted at
`/etc/flow/neo4j`, the pipeline is `Ready`, a notice produced to the input topic becomes a cart, a
checkout and an edge in the graph, and deleting the Secret and re-applying the resource yields
`Refused` (SC-004).

## Tier 6 — beside ankka on kind (manual)

With ankka's shopping cart deployed with `ANKKA_KAFKA_BOOTSTRAP_SERVERS` (as for `checkout-feed`)
and ankka-flow installed (`just up`):

```bash
just neo4j-up                                     # kustomization/overlays/neo4j: a dev Neo4j and the Secret neo4j-local in shop
docker build -f samples/checkout-graph/Dockerfile -t sample-checkout-graph . && kind load docker-image --name ankka sample-checkout-graph
flow generate samples/checkout-graph/blueprint.conf --descriptors samples/checkout-graph/flow \
  --conf samples/checkout-graph/k8s/in-cluster.conf --image mapper=sample-checkout-graph:latest -n shop | kubectl apply -f -
kubectl -n shop get aflow checkouts-graph -w      # Ready
curl … /carts/cart-1/items … && curl … /carts/cart-1/checkout
kubectl -n neo4j exec neo4j-0 -- cypher-shell -u neo4j -p … 'MATCH (c:Cart)-[:CHECKED_OUT]->(k:Checkout) RETURN c.cartId, k.checkedOutAt'
```

Expected: the cart and its checkout in the graph within seconds of the checkout; `kubectl get
events` on the resource shows `TopicCreated` for `graph-deltas` and nothing for `cart-checkouts`;
the sink pod's `/metrics` shows `deltas_written_total` rising and `deltas_stale_total` at zero; a
`flow reset` of the pipeline after scaling both streamlets to zero, then scaling up, leaves the
graph identical (US4, FR-026, FR-029).

## The reviewer's checklist

- [x] `protocol/fixtures/builtin/neo4j-merge-sink.json` and its Python copy are byte-identical; CI's diff passes.
- [x] The sidecar image starts with the driver on the classpath and `evicted` shows no Netty conflict.
- [x] No log line, event note or refusal message contains a password (grep the suites' captured output).
- [x] The process path is untouched: `ConversationSuite`, `ConformanceSuite`, `RestartKafkaSuite` pass unchanged.
- [x] `CLAUDE.md`'s decoding rule is narrowed, and the skills say the same.
- [x] `docs check` passes with the three new pages in the nav and in a skill each; `limitations.md` no longer says "no built-in stages".

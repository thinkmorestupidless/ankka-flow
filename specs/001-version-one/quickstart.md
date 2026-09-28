# Quickstart: proving version one works

**Feature**: [spec.md](./spec.md) | **Plan**: [plan.md](./plan.md) | **Contracts**: [contracts/](./contracts/)

Seven tiers, cheapest first; each gates the next. Tiers 1 and 2 need only a JDK; 3 and 4 need
Docker; 5 needs Python 3.12 and `uv`; 6 needs a kind cluster; 7 needs ankka running in it.

## Tier 1 — the protocol and the pure pieces (seconds, no Docker)

```bash
sbt protocol/compile                          # ScalaPB output warning-free under -source:3.3
sbt 'protocol/testOnly *DescriptorJsonSuite'  # every fixture parses and re-writes byte for byte
sbt blueprint/test                            # carried BlueprintSpec, BlueprintParserSpec, UnmanagedTopicSpec as munit; the new problems
sbt 'cli/testOnly *VerifySuite *GenerateSuite'
sbt 'crd/testOnly *CrdSchemaSuite'
sbt 'operator/testOnly *RenderingSuite *TopicResolutionSuite *ResetRequestSuite'
```

Expected: the cart blueprint verifies; each blueprint under `cli/src/test/resources/blueprints/`
is refused with exactly the message in `contracts/cli.md`, all problems in one pass (S2.1–2.4,
SC-002); `generate` emits a resource with every descriptor, image, binding and topic (S2.5);
rendering yields two containers with the sidecar's env, mounts, probes and ports and a process
container with none (S3.2); no sidecar image → a refusal and no actions (S3.6); a changed config
changes the hash annotation and an unchanged one does not (FR-024a).

## Tier 2 — the conversation, both ends in one JVM (seconds)

```bash
sbt 'sidecar/testOnly *ConversationSuite *SupervisorSuite'
sbt 'sidecar/testOnly *ConformanceSuite'        # Scala reference, in-process
```

Expected: `ConversationSuite` proves every rule in `contracts/protocol.md` against the
`ProcessDouble`: emits buffered until ack, one in flight per partition, interleaving across
partitions, each violation fails the stream, revoked batches drop their acks, a 5 MiB record
fails naming its offset. `SupervisorSuite` proves discovery refusals (`99.0`, a different
streamlet, a duplicate port → `ReportError` then exit code 1, S1.6), not-ready until the double
answers (S1.5), tear-down and reconnect with a new conversation id after a `Fail`, and the stall
warning after the threshold. Every `contracts/conformance.md` case passes; `violation.*` and
`version.*` run against the double.

## Tier 3 — real Kafka (about two minutes, Docker)

```bash
sbt 'sidecar/testOnly *InletGraphKafkaSuite *RecordKafkaSuite *SinkCommittingAfterKafkaSuite *ConsumerLagKafkaSuite *RestartKafkaSuite'
sbt 'operator/testOnly *ConsumerGroupResetSuite'
```

Expected (testcontainers `apache/kafka:3.9.1`):

- `RecordKafkaSuite` (carried): keys, header order with a binary header, one partition per key in
  offset order, over many partitions (S1.1, S1.4).
- `SinkCommittingAfterKafkaSuite` (carried): a produce failure at record 12 commits nothing at or
  past 12; a second run writes and commits all 20 (S1.2, S1.3).
- `InletGraphKafkaSuite`: fifty records over ten keys through the double to two outlets; the
  double killed after twenty and restarted; every record on exactly its outlet, per key in order,
  headers intact (S1's independent test with the double standing in for Python). Then two consumers
  in one group: a partition revoked mid-batch commits nothing and the other consumer reads it.
- `RestartKafkaSuite`: the double stopped for 5 s while the graph runs → `ready` removed within
  2 s, in-flight batches voided, graph rebuilt, records resume from the last commit with no loss.
- `ConsumerLagKafkaSuite` (carried): `records-lag` appears under `client_id =
  <pipeline>.<streamlet>.<inlet>` through the exporter's rules (S4.4).
- `ConsumerGroupResetSuite` (carried): a group moved to earliest; a group with members refused
  with Kafka's message; a group that never committed reports its partitions at the start.
- SC-006: `sbt 'sidecar/testOnly *SinkCommittingAfterKafkaSuite' -Dflow.mutation=commit-first`
  runs the suite with the commit moved before the produce and **must fail**; the build's
  `mutationCheck` alias asserts the failure.

## Tier 4 — the operator on k3s (five to ten minutes, Docker)

```bash
sbt docker:publishLocal                       # ankka-flow-sidecar, ankka-flow-operator, sample-cart-router (Python image)
sbt 'operator/testOnly *FlowClusterSuite'     # skipped with -Dflow.cluster.tests=off
```

Expected: Kafka as a one-node StatefulSet; the cluster secret; the cart resource applied →
managed topics created with the declared partitions, the unmanaged one untouched (S3.1); pods
`Ready` with two containers; the pipeline `Ready` (S3.3); a test producer writes to the unmanaged
topic and both outlet topics fill (SC-003's automated form, research R16); `replicas: 3` → three
pods share the inlet's partitions (S3.5); a second apply with a changed parameter rolls only the
router (S3.7); `replicas: 0`, then the reset annotation → groups at earliest, one `ResetOffsets`
event per group, the done marker; `replicas: 1` → every record delivered again (S4.1, S4.3,
SC-004); a reset requested with pods running → `ResetRefused` and no marker (S4.2); a pre-existing
topic with other partitions → `TopicDiffers` and no change; the operator restarted → no second
reset.

## Tier 5 — the Python SDK and the laptop loop (a minute; Python 3.12, uv, Docker)

```bash
cd sdks/python && uv sync && uv run python scripts/proto.py && uv run mypy && uv run pytest -q
uv run conformance                                       # Python reference against the suite (S5, SC-005)
diff -r ../../protocol/src/main/protobuf proto/src/main/protobuf && diff -r ../../protocol/fixtures proto/fixtures   # FR-027

cd ../../samples/cart-router
uv sync && uv run descriptor && diff flow/descriptor.json ../../protocol/fixtures/descriptors/cart-router.json
docker compose up -d                                     # Kafka + sidecar; sidecar waits for the process
uv run python -m cart_router.main &                      # the router on 127.0.0.1:9010
uv run python produce.py                                 # fifty CloudEvents over ten carts to localhost:9094
kill %1; sleep 1; uv run python -m cart_router.main &    # the kill-and-restart of S1
uv run python verify.py                                  # every event on its outlet, per key in order, headers intact
uv run python bench.py                                   # SC-008: records/s per partition, added median latency
```

Expected: fixtures byte-equal (S5.2); every conformance case passes and, with
`ANKKA_FLOW_BREAK=ack-first`, exactly `run.emits-precede-ack` fails; `verify.py` exits 0 (SC-001);
`bench.py` prints a table and exits 0 when ≥ 1,000 records/s per partition and < 10 ms added
median latency, and the numbers are pasted into `research.md`'s *Measurements* section with the
machine named.

## Tier 6 — installed on a kind cluster (manual)

Needs kind, kubectl and just. `flow` is not installed by anything: `just cli` builds it and prints
the line that puts it on your PATH.

```bash
just cli                                                 # builds flow; run the export line it prints
export PATH="$PWD/cli/target/universal/stage/bin:$PATH"
just up                                                  # kind cluster (default name: ankka), CRD, operator, dev Kafka, cluster secret, images loaded
kubectl create namespace shop
kubectl -n kafka exec kafka-0 -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic shop.cart-events.v1 --partitions 3     # the unmanaged input, owned by "someone else"
cd samples/cart-router
flow generate blueprint.conf --descriptors flow --conf k8s/in-cluster.conf \
  --image router=sample-cart-router:latest -n shop | kubectl apply -f -
kubectl -n shop wait aflow/cart --for=jsonpath='{.status.phase}'=Ready --timeout=240s
for i in $(seq 0 49); do echo "cart-$((i % 10)):{\"id\":$i,\"total\":$(( (i*37) % 200 ))}"; done |
  kubectl -n kafka exec -i kafka-0 -- /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 \
    --topic shop.cart-events.v1 --property parse.key=true --property key.separator=:
kubectl -n kafka exec kafka-0 -- /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic cart.valid-carts --from-beginning --timeout-ms 5000 | wc -l   # and cart.review-carts: 50 between them
kubectl -n shop get events --field-selector involvedObject.kind=AnkkaFlow
kubectl -n shop port-forward deploy/flow-cart-router 2050 & sleep 2; curl -s localhost:2050/metrics | grep records_lag
cd ../.. && just down                                    # removes the cluster
```

`k8s/in-cluster.conf` points the unmanaged input at the in-cluster Kafka; the blueprint's
`kafka:9092` is the compose network's name.

## Tier 7 — beside a running ankka service (manual)

With ankka deployed in the same kind cluster (`just up` in `../ankka`) and an ankka consumer
producing cart events to a topic (research item 6 settles which sample): declare that topic
unmanaged in `blueprint.conf` with `cluster = ankka` pointing at ankka's Kafka, apply, and watch
the router's outlet topics fill without the operator touching the ankka topic (S3's independent
test, SC-003).

## The reviewer's checklist

- [ ] Every carried file keeps its Lightbend header and its test is present as munit (SC-007).
- [ ] `grep -r "pekko-grpc\|pekko-http" build.sbt project/` finds nothing.
- [ ] The sidecar image has no HTTP server; `ss -ltn` in the container shows only 2050.
- [ ] The process container in the rendered pod has no ports, probes, mounts or secrets (S3.2).
- [ ] No resource, blueprint or descriptor names the sidecar image (FR-020).
- [ ] `flow verify` runs with no network and no JVM other than its own (FR-006).
- [ ] The fixtures match from Python and from Scala; CI diffs the protocol copy (FR-027).
- [ ] `notes/design/version-one.md` says grpc-java, JSON only, files for probes, `FLOW_` variables.

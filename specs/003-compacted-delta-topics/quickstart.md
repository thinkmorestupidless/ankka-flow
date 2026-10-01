# Quickstart: proving compacted delta topics work

**Feature**: [spec.md](./spec.md) | **Plan**: [plan.md](./plan.md) | **Contracts**: [contracts/](./contracts/)

Six tiers, cheapest first. Tier 1 needs a JDK and Python; 2 and 3 need Docker; 4 the sample's
compose file; 5 the k3s suite; 6 the kind cluster with ankka in it.

## Tier 1 — the pure pieces (seconds)

```bash
sbt 'sidecar/testOnly *DeltasSuite'              # keys.json; read(): delta, marker, the two key refusals
sbt 'blueprint/testOnly *DeltaTopicsSuite'       # every row of the decision table
sbt 'cli/testOnly *VerifySuite *GenerateSuite *CliResetSuite'
sbt -Dflow.cluster.tests=off 'operator/testOnly *RenderingSuite'
sbt 'sidecar/testOnly *PrometheusRulesSuite'
cd sdks/python && uv run python scripts/proto.py && uv run mypy && uv run pytest -q
cd samples/checkout-graph && uv run descriptor --check && uv run pytest -q
```

Expected: every row of `keys.json` holds in Scala and in Python; a managed delta topic is written
`cleanup.policy: compact` with its note, a blueprint's own policy is kept with the right note, and a
topic with no delta port is untouched (SC-006); `TopicNotCompacted` is rendered for an existing
topic that is not compacted and `cleanup.policy` is not repeated under `TopicSettingsIgnored`; a
reset of one streamlet is accepted while another runs; the helper takes no key and refuses what the
sink would (SC-004).

## Tier 2 — the stage against Neo4j

```bash
sbt 'sidecar/testOnly *Neo4jMergeSuite'
```

Expected: a wrongly keyed delta and a keyless one each fail the batch with the messages of
contracts/element-keys.md and write nothing; a node and an edge with one id are both written; a
record with no value is passed over, counted as a marker, and a batch of markers alone is
acknowledged (SC-003, SC-005).

## Tier 3 — compaction actually run (a few minutes)

```bash
sbt 'sidecar/testOnly *CompactionKafkaSuite *Neo4jSinkKafkaSuite'
sbt mutationCheck
```

Expected: a thousand elements written ten times compact to under two thousand records; the graph
rebuilt from the compacted topic into an emptied database equals the snapshot of the original,
element for element; a tombstone then a delete marker, compacted, leaves that element out of the
next rebuild and everything else equal; the sink never stalls on a marker; committed offsets still
never run ahead of the graph (SC-001, SC-002, SC-005).

## Tier 4 — the sample on a laptop

```bash
sbt sidecar/docker:publishLocal
cd samples/checkout-graph
uv run python produce.py && docker compose up -d && uv run python -m checkout_graph.main &
uv run python verify.py
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 \
  --topic checkouts-graph.graph-deltas --from-beginning --property print.key=true --max-messages 3
```

Expected: `verify.py` passes; the three records' keys read `node:cart:…`, `node:checkout:…`,
`edge:checked-out:…`.

## Tier 5 — the operator on k3s

```bash
sbt 'operator/testOnly *FlowClusterSuite'
```

Expected: the existing scenarios pass unchanged (a delta published by the suite now carries its
element key).

## Tier 6 — migration and rebuild on kind (manual)

The cluster still runs the `checkouts-graph` pipeline of 0.2.0, whose delta topic is not compacted
and whose records are keyed the earlier way.

```bash
just deploy                                         # this build's operator and sidecar
kubectl -n shop get events --field-selector involvedObject.kind=AnkkaFlow | grep TopicNotCompacted
# the migration of contracts/element-keys.md: scale to zero, delete the topic, new mapper image,
# flow reset, scale up; then
kubectl -n kafka exec kafka-0 -- /opt/kafka/bin/kafka-configs.sh --bootstrap-server localhost:9092 \
  --describe --topic checkouts-graph.graph-deltas | grep cleanup.policy=compact
# the rebuild of contracts/rebuild.md against an emptied Neo4j
```

Expected: before the migration, the sink refuses the first old record naming the expected key and
the resource carries `TopicNotCompacted`; after it, the topic is compacted, the graph is as it was,
and the rebuild restores it with the mapper stopped (SC-007).

## The reviewer's checklist

- [ ] `keys.json` and its Python copy are byte-identical; both suites read it.
- [ ] No test names a Kafka or Neo4j image by a literal tag.
- [ ] The built-in descriptor's fixture is unchanged (`git diff --stat protocol/fixtures/builtin` is empty).
- [ ] The sink's throughput is not worse than 1,782 deltas/s per partition by more than noise (`bench.py`).
- [ ] The release notes say that a writer built for 0.2.0 is refused until it keys its deltas.
- [ ] `docs check` passes with `deploy/rebuild-a-graph.md` in the nav and in a skill.

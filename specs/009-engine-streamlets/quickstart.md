# Quickstart: proving Engine Streamlets

Three tiers. Tier 1 and 2 are the build's checks; tier 3 is by hand, once, and recorded in
`research.md`. Shapes are in `contracts/` and `data-model.md`.

## Prerequisites

Docker; JDK 21 (the build) with `--release 17` for the Flink modules; `uv`; for tier 3 also
`kind`, `kubectl`, `helm` and a checkout of ankka at `v0.18.0` or later.

## Tier 1: no cluster (minutes)

```bash
sbt descriptor/test blueprint/test crd/test          # the model, the fixtures, verification
sbt pekkoSdk/test                                    # the harness, commit after write on a real Kafka
sbt mutationCheck                                    # the commit-after-write suite fails with the commit moved first
sbt flinkSdk/test flinkTestkit/test                  # the descriptor bytes, the harness on a local environment
sbt cartCleaner/test cartCounter/test                # the sample's own tests, which the docs include
sbt cartCleaner/descriptorCheck cartCounter/descriptorCheck
sbt cli/test                                         # verify, generate, local, the ankka topics, mcp
sbt -Dflow.cluster.tests=off operator/test           # rendering for both engines, reset, conditions
just features                                        # 0 findings
just docs                                            # no problems
```

Expected: every suite green; `mutationCheck` reports the mutated run failed.

The laptop walkthrough (`features/pipelines/laptop.feature`):

```bash
docker compose up -d kafka
sbt cli/stage
cli/target/universal/stage/bin/flow local samples/cart-pipeline/blueprint.conf \
  --descriptors samples/cart-pipeline/flow --bootstrap-servers localhost:9092 -o /tmp/carts
FLOW_CONFIG_DIR=/tmp/carts/cleaner sbt cartCleaner/run     # terminal 1
FLOW_CONFIG_DIR=/tmp/carts/counter sbt cartCounter/run     # terminal 2
samples/cart-pipeline/produce.sh localhost:9092 50          # 50 cart events, 5 malformed
samples/cart-pipeline/verify.sh localhost:9092              # counts per cart per minute match
```

Expected: `verify.sh` exits 0; the counter logs once that its state is kept only while it runs;
stopped and started again it starts with no state and reads from its committed offsets.

## Tier 2: the cluster suite (k3s in Docker, about fifteen minutes)

```bash
sbt operator/test                                    # FlowClusterSuite; -Dflow.flink.tests=off skips Flink
```

It builds and imports the operator and the two sample images, applies the committed Flink
operator manifest and MinIO, and runs, one case per scenario:

| Case | Scenario |
|---|---|
| the pipeline is Ready; the cleaner's pod has one container; the counter is a FlinkDeployment | `engines.feature`: reads and writes, one blueprint |
| 50 events in, counts out, malformed dropped | `flink.feature`: windows by the record's time |
| three cleaner replicas share the group | spec edge case |
| the counter's pods deleted mid-window; counts equal events, no window twice | `flink.feature`: fails and continues |
| a second image of the counter with a field added to its state; the open window carries over | `flink.feature`: keeps its state |
| a third image whose state class is retyped; `StateUnreadable`, the state directory untouched | `flink.feature`: cannot read the state |
| reset refused while running; stopped, reset, epoch 2, everything read again | `flink.feature`, `engines.feature`: reset |
| status holds a lag per inlet for both streamlets | `engines.feature`: lag |
| a resource applied before the Flink CRD or `flink-state` exists | `flink.feature`: refused |
| a version one resource | refused, saying a descriptor must name an engine |
| two SASL users on an ACL broker, one without the grant: `TopicNotGranted`, then Ready | `ankka.feature`: grant |

## Tier 3: beside ankka, by hand

1. ankka's local platform on kind with the `broker-external` component and the shopping cart in
   project `checkout`; `ankka projects topics set cart-checkouts --contract checkout-notice.v1
   --schema …` and `clean-checkouts` likewise.
2. `ankka organizations machines register <org> pipelines`; the secret into a Kafka cluster
   Secret `kafka-cluster-ankka` with `machine: <org>/pipelines`.
3. `ankka projects topics list -p checkout -o json > checkout-topics.json`.
4. `flow verify samples/cart-pipeline/ankka/blueprint.conf --descriptors … --ankka-topics
   checkout=checkout-topics.json`: verified. Change a contract in the blueprint's descriptor
   directory: refused, naming both.
5. Deploy without a grant: `TopicNotGranted` within a minute, naming the topic, the right and
   the command. `ankka projects grants make machine:<org>/pipelines topic cart-checkouts consume
   -p checkout` and `… clean-checkouts produce …`: Ready.
6. Check out a cart: an ankka consumer of `clean-checkouts` in the project reads the cleaned
   record. `ankka projects grants list` shows the machine reading.
7. Revoke the grant: the condition returns within a minute; grant again: it clears.

Record the outcome under `## Tier 3` in `research.md`.

## Success criteria, where each is shown

| | Shown by |
|---|---|
| SC-001 | the cleaner rewritten as a Flink streamlet in a scratch branch: `git diff --stat` touches only `samples/cart-pipeline/cleaner` and its image name (by hand, recorded) |
| SC-002 | `VerifySuite`'s pairings outline; tier 3 step 4 |
| SC-003 | tier 2's kill cases, run for ten minutes with `-Dflow.cluster.soak=on` |
| SC-004 | tier 2's refused and grant cases, timed |
| SC-005 | `git grep -il sidecar -- . ':!specs' ':!notes' ':!NOTICE'` prints nothing |
| SC-006 | the engines page's scenario in `engine-docs.feature`, read by a person |

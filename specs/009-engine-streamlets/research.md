# Research: Engine Streamlets

Decisions for `plan.md`, each with why and what lost. Facts about Flink were read from its
documentation, repositories and Maven Central on 2026-11-22; facts about ankka from its tag
`v0.18.0`. "V*n*" items at the end are things to prove before the code that depends on them.

## R1. Versions

**Decision**: Apache Flink **2.2.1**, `flink-connector-kafka` **5.0.0-2.2** (kafka-clients 4.2.0),
Flink Kubernetes Operator **1.16.1** (`flink.apache.org/v1beta1`, `flinkVersion: v2_2`), image
`flink:2.2.1-java17`. Flink streamlets compile with `--release 17`. Pekko Streams streamlets stay
on the repository's line: Pekko 1.7.1, pekko-connectors-kafka 1.2.0, kafka-clients 3.9.2.

**Rationale**: Flink 2.3.0 is the newest release, and its own documentation says "There is no
connector (yet) available for Flink version 2.3". 2.2 is one of the two lines the community
patches. Java 17 is Flink's default and recommended version; Java 21 is marked experimental.
The two engines never share a classpath, so two kafka-clients versions cost nothing.

**Alternatives**: Flink 2.3.0 with an untested connector (lost: unconfirmed). Flink 1.20 LTS
(lost: Java 11 line, config keys already deprecated in 2.x, no Java 21 image).

**Licence (principle 1)**: Flink, its operator and its Kafka connector are Apache-2.0. Flink's RPC
runs on Apache Pekko 1.4.0 (module `flink-rpc-akka`, "Pekko-based RPC implementation"), not Akka.
The full transitive tree is checked in V9.

## R2. Modules

**Decision**:

| Module | Path | Language | Published as | Depends on |
|---|---|---|---|---|
| `descriptor` | `descriptor/` (was `protocol/`) | Scala 3 LTS | `ankka-flow-descriptor_3` | typesafe-config only |
| `pekkoSdk` | `sdks/pekko-streams/` | Scala 3 LTS | `ankka-flow-pekko-streams_3` | `descriptor`, pekko-stream, pekko-connectors-kafka |
| `flinkSdk` | `sdks/flink/` | Java 17 | `ankka-flow-flink` | Flink (provided), flink-connector-kafka, jackson-databind |
| `flinkTestkit` | `sdks/flink-testkit/` | Java 17 | `ankka-flow-flink-testkit` | `flinkSdk`, flink-clients |
| `blueprint`, `crd`, `operator`, `cli` | as now | Scala 3.9 | not published | `descriptor` in place of `protocol` |
| `cartCleaner` | `samples/cart-pipeline/cleaner/` | Scala | image `sample-cart-cleaner` | `pekkoSdk` |
| `cartCounter` | `samples/cart-pipeline/counter/` | Java | image `sample-cart-counter` | `flinkSdk`, `flinkTestkit` (test) |

Removed: `sidecar/`, `sdks/scala/`, `sdks/python/`, `samples/cart-router`,
`samples/cart-router-scala`, `samples/checkout-feed`, `samples/checkout-graph`, the protobuf and
ScalaPB, gRPC, the JMX agent, the Neo4j driver and its testcontainer.

**Rationale**: with no protocol the module that holds the descriptor is named for what it holds.
The Flink SDK is Java with no Scala library on its classpath, so it cannot depend on `descriptor`;
it writes the descriptor itself and both SDKs are held to one set of fixture bytes (R3), which is
how version one kept two SDKs honest. The testkit is separate because it puts `flink-clients` on
the compile classpath, which a job's image must not carry.

**Alternatives**: one SDK module with both engines (lost: a Pekko streamlet's image would carry
Flink). Flink SDK in Scala (lost: clarified, Java).

## R3. The descriptor

**Decision**: a hand-written model in `descriptor`, JSON in the canonical form `DESCRIPTOR.md`
already defines (sorted keys, two-space indent, trailing newline, snake_case, defaults omitted):

```json
{
  "descriptor_version": "2.0",
  "sdk": {"name": "ankka-flow-pekko-streams", "version": "0.6.0"},
  "streamlet": {
    "name": "cleaner",
    "engine": "pekko-streams",
    "inlets": [{"name": "in", "contract": {"name": "cart-events.v1", "fingerprint": "sha256:…"}}],
    "outlets": [{"name": "out", "contract": {"name": "clean-carts.v1"}}],
    "config_parameters": [{"key": "max-total", "type": "INTEGER", "default_value": "10000"}]
  }
}
```

A Flink descriptor adds `"flink": {"entry_class": "…", "version": "2.2"}` inside `streamlet`.
`engine` is `pekko-streams` or `flink`. A descriptor without `engine`, or with
`protocol_version` (version one's), is refused: "a descriptor must name an engine". A
`descriptor_version` whose major is not 2 is refused. The CRD's `protocolVersion` becomes
`descriptorVersion`.

Fixtures move to `descriptor/fixtures/descriptors/` and are rewritten for the six declarations
with an engine each; `pekkoSdk` and `flinkSdk` each have a suite that declares the fixtures in
their own API and compares bytes.

**Alternatives**: keep protobuf as the model (lost: nothing speaks the protocol; ScalaPB and gRPC
stay in the build for one message).

## R4. The contract

**Decision**: `Contract(name, fingerprint: Option[String])`. The name follows ankka's rule
`[a-z0-9][a-z0-9._-]{0,98}[a-z0-9]`. The fingerprint is `sha256:` and the lowercase hex SHA-256
of the schema document rendered under RFC 8785, exactly as ankka's `Contract.fingerprint`
computes it; the document alone is hashed, not the name. An SDK takes the schema as a classpath
resource and writes the fingerprint; the document is not in the descriptor. `format` and version
one's `Fingerprint.scala` (Base64 of the name's hash, carried from Cloudflow) are removed, and
`NOTICE` loses that line.

Two ports connect when names are equal and, where both state a fingerprint, the fingerprints are
equal. The canonicaliser is ported from ankka's `Contract.Canonical` into `descriptor`
(`Canonical.scala`) and into `flinkSdk` (`Canonical.java`); both are held to ankka's
`protocol/fixtures/contracts/` copied to `descriptor/fixtures/contracts/`.

**Alternatives**: depend on `ankka-core` for the function (lost: ankka-flow depends on no ankka
artefact, and the Java SDK could not use it).

## R5. What a streamlet is given at run time

**Decision**: one file, `streamlet.properties`, Java properties, in the directory
`FLOW_CONFIG_DIR` names (`/etc/flow/config` in a pod). Both SDKs read it with the JDK alone.
`contracts/streamlet-settings.md` is the format. The operator writes it into the Secret
`flow-<pipeline>-<streamlet>` it already renders; `flow local` writes the same file for a laptop
(R15). It replaces `streamlet.conf` and `descriptor.json` in that Secret.

**Rationale**: the Flink SDK has no HOCON library and should not gain one. The settings hold
broker credentials, which is why they stay in a Secret.

## R6. Kafka credentials without a sidecar

**Decision**: a streamlet's Secret holds the connection settings of the topics that streamlet's
own ports are on and no other topic's, mounted read-only (mode 0440) into the streamlet's
containers. The rule "Kafka credentials reach only the sidecar" in `CLAUDE.md` becomes "a
streamlet holds the credentials of its own topics only". The limitations page says a streamlet's
code can read them.

**Alternatives**: a credential per streamlet issued by the operator (lost: the operator would
have to create broker users on brokers it does not own).

## R7. The Pekko Streams SDK

**Decision**: a streamlet extends `PekkoStreamlet`, declares ports and parameters as values, and
gives one flow per inlet:

```scala
object Cleaner extends PekkoStreamlet("cleaner"):
  val in       = inlet[CartEvent]("in", Contract("cart-events.v1", schema = "cart-events.v1.json"), Codec.json)
  val out      = outlet[CleanCart]("out", Contract("clean-carts.v1"), Codec.json)
  val maxTotal = intParameter("max-total", default = 10000)

  def logic(ctx: Context) = from(in) { _.filter(_.value.total <= ctx(maxTotal)).map(r => out(r.key, clean(r.value))) }
```

`from(inlet)(f)` takes a function over `FlowWithContext[Record[A], Offset, …]` returning elements
that are zero or more `Emit`s (`out(key, value)`, `Seq(…)` or `Nil`). The SDK owns the source
(`Consumer.committableSource`), the producer (`Producer.flexiFlow` with the offset as pass
through) and the committer, so an offset is committed only after every record made from it is
confirmed; `FlowWithContext` offers no operator that reorders or drops the context. Dropping an
element commits nothing for it and a later commit covers it. A failure anywhere fails the stream;
the runner restarts it with backoff from the last commit, indefinitely. No `groupedWithin`,
`statefulMap`, timer or store is offered through the context API (FR-008): `FlowWithContext`
does not have them.

The runner (`PekkoStreamlets.run(streamlet, args)`): `--descriptor <file>` writes the descriptor
and exits; otherwise reads the settings, starts one stream per inlet, and keeps
`/tmp/flow/ready` (present while every inlet has partitions assigned or the group is balanced
without it) and `/tmp/flow/alive` (touched every 5 s), the file probes version one's sidecar used.

Version one's `CommitAfterWrite.scala` and its `SinkCommittingAfterKafkaSuite` (carried from
Cloudflow) move from `sidecar` to `sdks/pekko-streams` with their headers; `mutationCheck` points
at the moved suite. `RecordKafkaSuite` moves likewise. `ConsumerLagKafkaSuite`,
`PrometheusRulesSuite` and `prometheus.yaml` are removed with the metrics they tested, and
`NOTICE` is updated.

**Harness**: `PekkoHarness(streamlet, parameters).run(in -> records)` runs the same flows over
in-memory sources and returns the records per outlet and the offsets that would be committed, in
order. No broker.

**Alternatives**: hand the author a raw `Source` and `Sink` (lost: commit after write would be
the author's to get right, which is the one promise the engine makes).

## R8. The Flink SDK

**Decision**: Java. A streamlet extends `FlinkStreamlet`:

```java
public final class CartCounter extends FlinkStreamlet {
  final Inlet<CartEvent> in = inlet("in", Contract.of("clean-carts.v1"), Codec.json(CartEvent.class));
  final Outlet<CartCount> out = outlet("out", Contract.of("cart-counts.v1"), Codec.json(CartCount.class));
  final Parameter<Duration> window = durationParameter("window", "1m");

  public CartCounter() { super("cart-counter"); }

  @Override public void build(FlinkContext ctx) {
    ctx.write(out, ctx.read(in, WatermarkStrategy.<Record<CartEvent>>forBoundedOutOfOrderness(…))
        .keyBy(Record::key).window(TumblingEventTimeWindows.of(ctx.get(window))).aggregate(…));
  }
  public static void main(String[] args) { FlinkStreamlets.run(new CartCounter(), args); }
}
```

`ctx.read` returns Flink's own `DataStream<Record<A>>` from a `KafkaSource`; `ctx.write` attaches
a `KafkaSink`. Everything between is Flink's API, untouched (FR-011). A record's timestamp is the
Kafka record's unless the author's `WatermarkStrategy` assigns another. `Record<A>` is a Java
record of key, value, headers and timestamp; state types are the author's and follow Flink's
POJO rules, which is what lets state survive a changed class (fields added or removed, never
retyped or renamed).

- Source: group id from the settings, `OffsetsInitializer.committedOffsets(EARLIEST)`,
  `commit.offsets.on.checkpoint=true`. On restore Flink uses the offsets in its state.
- Sink: `DeliveryGuarantee.EXACTLY_ONCE` with the transactional id prefix from the settings when
  the outlet's `delivery` is `exactly-once`; otherwise `AT_LEAST_ONCE`.
- `FlinkStreamlets.run`: `--descriptor <file>` writes the descriptor; otherwise builds and
  executes. With no cluster (`FLOW_LOCAL=true` in the settings) it uses Flink's local environment
  and logs once that state is kept only while the process runs.

**Harness** (`ankka-flow-flink-testkit`): `FlinkHarness.run(streamlet, parameters, input)` builds
the same job on a local environment with bounded sources carrying each record's timestamp and
collecting sinks, and returns the records per outlet. No broker, no cluster.

**Image**: `FROM flink:2.2.1-java17`, the streamlet's jar at
`/opt/flink/usrlib/streamlet.jar` and its non-provided dependencies beside it (Flink puts every
jar in `usrlib` on the job's classpath, V3), so no assembly plugin is needed.

**Tests are Java**: the sample's test is a documentation sample, so `sbt-jupiter-interface` is
added to run JUnit 5 from sbt (V10).

## R9. How the operator runs a Pekko Streams streamlet

**Decision**: as version one ran a process, without the sidecar: a Secret and a Deployment
`flow-<pipeline>-<streamlet>`, one container named `streamlet` from the streamlet's image, env
`FLOW_CONFIG_DIR`, the Secret mounted, the two file probes, no service account token, replicas
from the resource. Each replica joins the inlet's group, so a partition is read by one replica at
a time. `FLOW_SIDECAR_IMAGE` and its refusal are removed.

## R10. How the operator runs a Flink streamlet

**Decision**: one `FlinkDeployment` `flow-<pipeline>-<streamlet>` in application mode per Flink
streamlet, rendered by `Rendering` as an action like any other and applied by `Fabric8Executor`
as a generic resource (no new dependency: fabric8's `GenericKubernetesResource`).

```yaml
spec:
  image: <the streamlet's image>
  flinkVersion: v2_2                       # from the descriptor's flink.version
  serviceAccount: <FLOW_FLINK_SERVICE_ACCOUNT, default flink>
  flinkConfiguration:
    execution.checkpointing.interval: 10s  # deploy-time: flink.checkpoint-interval
    execution.checkpointing.mode: EXACTLY_ONCE
    execution.checkpointing.dir: <state>/<namespace>/<pipeline>/<streamlet>/<epoch>/checkpoints
    execution.checkpointing.savepoint-dir: …/<epoch>/savepoints
    high-availability.type: kubernetes
    high-availability.storageDir: …/<epoch>/ha
    restart-strategy.type: exponential-delay
    # then the state store's own flink-config keys
  jobManager: {resources: …}               # deploy-time, defaults 1 cpu / 1024m
  taskManager: {resources: …}
  podTemplate: the settings Secret mounted, FLOW_CONFIG_DIR, the state store's env Secret as envFrom
  job:
    jarURI: local:///opt/flink/usrlib/streamlet.jar
    entryClass: <descriptor flink.entry_class>
    parallelism: <deploy-time, default 1>
    upgradeMode: last-state
    state: running | suspended             # suspended when replicas = 0
```

`last-state` is what makes a new image start from the state the previous one left, including
when the previous job was unhealthy; it needs checkpointing and Kubernetes high availability,
which is why the service account must be one the Flink operator's chart made (it carries the
ConfigMap rights). The operator observes `status.lifecycleState`, `status.jobStatus.state`,
`status.jobManagerDeploymentStatus` and `status.error` and maps them: `STABLE`/`RUNNING` is
running; `FAILED`, or `ERROR`, is failed with `status.error`'s message as the streamlet's detail.

**Deploy-time configuration** (`flow.streamlets.<name>`): `parallelism`, `replicas` (only `0`,
to stop, or absent), `flink { checkpoint-interval, job-manager { cpu, memory }, task-manager {
cpu, memory } }`. `replicas` above 1 on a Flink streamlet and `parallelism` or `flink` on a Pekko
Streams streamlet are refused by `flow`.

**Alternatives**: a session cluster shared by a pipeline's Flink streamlets (lost: one image per
streamlet is the model, and a session cluster takes jars, not images). Our operator creating
Flink's pods itself (lost: that is the Flink operator's whole job).

## R11. The Flink operator and the state store as prerequisites (FR-015)

**Decision**: when a resource holds a Flink streamlet the operator checks, each reconcile:

1. the CRD `flinkdeployments.flink.apache.org` exists, else condition `FlinkOperatorMissing`;
2. a Secret `flink-state` exists in the clusters namespace with a `dir` key, else condition
   `StateStoreMissing`.

Either refuses the whole resource: phase `Failed`, an event, and no streamlet of any engine is
started. `flink-state` holds:

| Key | Meaning |
|---|---|
| `dir` | required; a URI Flink can write, such as `s3://flow-state/prod` |
| `flink-config` | optional; Flink configuration as properties text, merged into every `flinkConfiguration` (`s3.endpoint`, `s3.path-style-access`) |
| `env` | optional; properties text, copied by the operator into a Secret `flow-<pipeline>-flink-state` in the pipeline's namespace and given to Flink's pods as environment (`AWS_ACCESS_KEY_ID`, `ENABLE_BUILT_IN_PLUGINS`) |

It follows the `kafka-cluster-<name>` Secrets: one place in the operator's namespace, never in a
resource. Credentials go as environment, not into `flinkConfiguration`, because a
`FlinkDeployment`'s spec is readable by anyone who can read the resource.

`docs/deploy/install.md` installs the Flink operator with Helm before ankka-flow's, with
`webhook.create=false` offered for clusters without cert-manager, and its chart's
`watchNamespaces` naming the namespaces pipelines run in.

## R12. Reset and state (FR-020)

**Decision**: reset keeps version one's shape: `flow reset` writes the annotation; the operator
refuses while the streamlet runs (Pekko: replicas above 0; Flink: job not suspended) and leaves
the request pending; once stopped it resets each inlet's group to earliest
(`ConsumerGroupReset.toEarliest`, unchanged) and marks the request done.

For a Flink streamlet the operator also (a) deletes the `FlinkDeployment`, which removes its
high-availability metadata, and (b) advances the streamlet's **state epoch**, a number in the
annotation `flow.ankka.thinkmorestupidless.com/state-epochs` (`{"counter": 2}`). The epoch is
part of every state path and of the transactional id prefix, so the job that starts next finds
no state. The previous epoch's files stay in the state store until the store's own lifecycle
rule or a person removes them; the reset page and the limitations page say so.

**Alternatives**: the operator deleting the files (lost: it would need a client and write
credentials for every filesystem Flink supports). A Kubernetes Job running Flink's image to
delete them (lost: a second moving part for something a bucket rule does).

## R13. Delivery (FR-006, FR-007, FR-013)

**Decision**:

- Pekko Streams: at least once, commit after write, never skip (R7).
- Flink, outlet on a managed topic: `exactly-once`. The operator writes `delivery=exactly-once`
  and `transactional-id-prefix=flow.<namespace>.<pipeline>.<streamlet>.<outlet>.e<epoch>` for the
  outlet, and `transaction.timeout.ms=900000` (the broker's default maximum).
- Flink, outlet on an unmanaged or ankka topic: `at-least-once`. ankka renders no
  transactional-id rule for a machine, so a transaction would be refused.
- Every inlet on a managed topic, of either engine, reads with `isolation.level=read_committed`,
  so a reader downstream of a Flink streamlet sees only committed records, one checkpoint
  interval behind. The delivery page says so.

## R14. Lag (FR-019, FR-028)

**Decision**: the operator computes lag and writes it into the resource's status. Each observe
pass (every `FLOW_OPERATOR_OBSERVE_SECONDS`, default 30) it asks each inlet's cluster for the
group's committed offsets and the topic's end offsets with the `Admin` it already holds, and
writes `status.streamlets[].inlets[] = {name, topic, group, lag}` where `lag` is the sum over
partitions, absent when the broker could not be asked. `flow mcp`'s `pipeline_lag` reads the
status; `Lag.parse`, the port-forward and `LagSuite`'s metrics text go. For Flink the group's
offsets move on each checkpoint, so its lag trails by the checkpoint interval.

**Rationale**: with no sidecar there is no one metrics endpoint to scrape, the CLI has no broker
credential and should not get one, and the operator already talks to every cluster.

**Consequence**: Prometheus metrics from a streamlet are out of this feature's scope (added to
the spec's Out of Scope). Flink has its own reporters, configurable through `flink-config`.

## R15. A laptop (FR-029)

**Decision**: a new command, `flow local <blueprint.conf> --descriptors DIR --bootstrap-servers
HOST:PORT [--conf FILE]* -o DIR`, verifies the blueprint and writes
`DIR/<streamlet>/streamlet.properties` for every streamlet: every topic on the one broker given,
plaintext, managed topics named as a cluster would name them, `local=true`. A streamlet is then
run with `FLOW_CONFIG_DIR=DIR/<streamlet>` from its build (`sbt cartCleaner/run`,
`sbt cartCounter/run`). The root `docker-compose.yml` provides Kafka; the topics are created by
the broker's auto-creation there. `flow local` refuses a blueprint with an ankka topic unless
`--ankka-bootstrap-servers` is also given (ankka's local platform exposes its broker in
plaintext).

**Alternatives**: hand-written settings files in the sample (lost: every author would copy and
edit them, and they would drift from what the operator writes).

## R16. ankka topics (FR-022 to FR-025)

**Decision**:

- **Blueprint**: `blueprint.topics.<id> { ankka = "<project>/<name>", consumers = […], producers
  = […] }`. `flow` sets the Kafka name `<project>.<name>` (ankka's `BrokerNames.topic`; there is
  no organization in it), `managed = false`, and the cluster `ankka` unless `cluster` or
  `flow.ankka.cluster` in a `--conf` file names another. `ankka` together with `managed = true`,
  `topic.name` or `bootstrap.servers` is refused. An ankka topic may have producers, which an
  ordinary unmanaged topic may not.
- **The machine**: a Kafka cluster Secret with one more key, `machine = <organization>/<name>`.
  Its `bootstrap.servers` is ankka's external listener and its `connection-config` is the
  properties ankka documents for a machine (`SASL_SSL`, `OAUTHBEARER`, the token endpoint, the
  client id `machine:<organization>/<name>` and its secret). This is "the credential the
  deploy-time configuration names": the configuration names the cluster. A group on such a
  cluster is `ankka.machine.<organization>.<name>.<pipeline>.<streamlet>.<inlet>`, inside the
  prefix ankka allows the machine. The operator never calls ankka's control plane itself; the
  Kafka client does, for its token.
- **Declarations**: `flow verify --ankka-topics <project>=<file>` (repeatable; `generate` and
  `local` take it too), the file being the output of `ankka projects topics list -p <project> -o
  json`: an array of `{name, partitions, contract: {name, fingerprint}?, …}`; unknown fields are
  ignored. Checks: the topic is declared (else refused, listing the declared names); the topic
  declares a contract (else refused, saying what to declare in ankka); the port's contract name
  equals it; where the port states a fingerprint, it equals the topic's. Without the option for
  a project the blueprint names: verified otherwise, with the note "ankka topics of project
  '<p>' were not checked: pass --ankka-topics <p>=<file>".
- **Grants**: each observe pass the operator, as the machine, describes the pipeline's ankka
  topics with `includeAuthorizedOperations`. A topic the broker refuses to describe, or whose
  operations lack `READ` for an inlet or `WRITE` for an outlet, is a condition `TopicNotGranted`
  ("machine affiliates/network holds no grant to read topic checkout.cart-checkouts: an owner of
  project checkout runs `ankka projects grants make machine:affiliates/network topic
  cart-checkouts consume -p checkout`") and an event; the phase is `Degraded`. An authentication
  failure (`SaslAuthenticationException`, or the token endpoint answering `invalid_client`) is the
  condition `MachineCredentialRefused`. Both clear on the first pass that succeeds. Streamlets
  are started regardless and retry: a Pekko Streams stream restarts with backoff, a Flink job by
  its restart strategy.

**Rationale**: checking from the operator is the same for every engine, needs nothing from a
streamlet, and meets "within one minute" by the observe interval. Reading the refusal out of a
Flink job's exceptions would be Flink-specific and late.

**Limits to document**: ankka answers a client id's token requests at most twelve times a
minute and caps connections per address; many replicas starting at once may be told to slow
down, and retry. Fields ankka encrypts stay envelopes: decrypting through the keyring is not in
this feature.

## R17. Status and conditions (FR-018)

**Decision**: `AnkkaFlowStatus` gains `conditions: List[Condition(type, status, reason, message,
lastTransitionTime)]`; `StreamletStatus` gains `engine` and `inlets` (R14). Condition types:
`Ready`, `FlinkOperatorMissing`, `StateStoreMissing`, `TopicNotGranted`,
`MachineCredentialRefused`, `StateUnreadable`. Phases stay `Pending | Ready | Degraded | Failed`.
`Ready` only when every streamlet is running: a Deployment rolled out, or a `FlinkDeployment`
`STABLE` with its job `RUNNING`. The API version stays `v1alpha1`; the shape changes without a
conversion, since nothing depends on version one. A resource whose streamlet's descriptor names
no engine is refused with the event `Refused` and phase `Failed`, and nothing is started for it;
this is what an old pipeline looks like to the new operator.

**State that cannot be read (FR-014)**: a `FlinkDeployment` that reaches `FAILED` or `ERROR`
after its image changed, with `status.error` naming a restore failure (`StateMigrationException`,
`Failed to rollback to checkpoint/savepoint`, `Cannot map checkpoint/savepoint state`,
`InvalidClassException`), gets the condition `StateUnreadable` and the streamlet's detail says:
deploy the previous image, or reset. The operator changes nothing in the state store. V6 proves
which of Flink's failure paths this takes and pins the matched text to what it shows.

## R18. Removal (FR-021) and what it touches

**Decision**: one phase late in the build, after the sample pipeline runs in the cluster suite,
removes: `sidecar/`, `sdks/scala/`, `sdks/python/`, the four samples, `protocol/src/main/protobuf`
and its README, `Builtins.scala`, `BuiltinStages.scala`, `DeltaTopics.scala`, `Init.scala`,
`Scaffold.scala`, `cli/src/main/templates/`, the `init` command, `TemplateSuite`, `InitSuite`,
`LagSuite`; the build's `sdk`, `cartRouterScala`, `sidecar`, `templateArtifacts`, `initTemplates`,
`sampleImage`, `sdkConformance`; the CI job `sdk-python` and the release jobs `sdk-python` and
the sample images; the docs pages the map marks V1; the skills `ankka-flow-protocol`,
`ankka-flow-python`, `ankka-flow-scala` (replaced by `ankka-flow-pekko-streams` and
`ankka-flow-flink`); `kustomization/overlays/neo4j`. The release publishes the three library
artefacts under the Scala SDK job's existing Central Portal steps, the operator image, the two
sample images, the plugin and the archives.

Doing it late keeps every suite green between tasks; the first phases add beside what exists.

## R19. The cluster suite

**Decision**: `FlowClusterSuite` runs the sample pipeline on k3s with: the Flink operator applied
from a manifest rendered once from its Helm chart with the webhook off and committed at
`operator/src/test/resources/flink-operator.yaml` (`just flink-operator-manifest` regenerates
it); MinIO as the state store; the images `flink-kubernetes-operator`, `minio`, the two samples
and the operator imported into k3s. A second switch, `-Dflow.flink.tests=off`, skips the Flink
cases, and a third, `-Dflow.cluster.soak=on`, runs the kill cases for ten minutes under load
(SC-003); both are added to `forwardedTestSwitches`. The ankka cases (story 4) run against a Kafka
with an ACL authorizer and two SASL users standing in for a machine with and without a grant
(V8); the full run beside a real ankka installation is quickstart tier 3, by hand.

## Verify first

- **V1** `flink-connector-kafka:5.0.0-2.2` resolves, and a job built on it reads and writes the
  suite's `apache/kafka:3.9.1` with kafka-clients 4.2.0.
- **V2** kafka-clients 4.2.0 with `OAUTHBEARER` client credentials: the login handler's class
  name, and whether the JVM needs
  `-Dorg.apache.kafka.sasl.oauthbearer.allowed.urls=<token endpoint>`; if so the SDKs set it
  from the settings before the first client is made, and the operator does for its own `Admin`.
- **V3** every jar in `/opt/flink/usrlib` is on an application-mode job's classpath, so the
  image needs no fat jar.
- **V4** the Flink operator's chart renders with `webhook.create=false` and runs on k3s; the
  service account it creates in a watched namespace is named `flink`.
- **V5** Flink's S3 plugin reads `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` from the
  environment against MinIO with `s3.endpoint` and `s3.path-style-access` in the configuration;
  if not, the keys go in `flink-config` and the page says who can read them.
- **V6** what a `FlinkDeployment` reports when a new image cannot restore the state (R17).
- **V7** deleting a suspended `FlinkDeployment` removes its high-availability ConfigMaps, so a
  recreated one at a new epoch starts empty.
- **V8** `describeTopics` with `includeAuthorizedOperations` returns `READ` and `WRITE` per the
  caller's ACLs on Kafka 3.9, and a topic with no ACL at all for the caller answers
  `TopicAuthorizationException`.
- **V9** `sbt cartCounter/dependencyTree` and Flink 2.2.1's distribution NOTICE hold no BSL
  licence and no `com.typesafe.akka`.
- **V10** `sbt-jupiter-interface`'s current release runs JUnit 5 under sbt 1.13.1.
- **V11** a Scala 3 LTS `pekkoSdk` compiles the `from(inlet)` shape of R7 with
  `FlowWithContext`; if the type gets in an author's way, the fallback is a function
  `Record[A] => Seq[Emit]` plus `async` for a call, decided in research before the sample.

## Design finished

2026-11-22: analyze reported nothing above LOW.

Two rounds. The first found six things, all applied, none changing a decision or a scenario:
four task cross-references pointing at the wrong task; the operator's refusal of an unknown
engine missing from T016 (FR-002); the scenario "an outlet writes an ankka topic by its project
and name" proven only by hand, now also in T047; SC-002's "every pairing of engines" covered for
one engine only in T044; the spec calling the state store's Secret "the operator's settings".
The second found one LOW item, left: the spec's Status line still says Draft.

Coverage: 29 functional requirements and 6 success criteria, each with at least one task; 31
added or changed scenarios, each named by a task; 68 tasks, each mapped to a requirement, a
scenario or a verify-first item except T068 (the family brain, outside this repository).

## Verified

- **V10** (T001): `sbt-jupiter-interface` 0.20.0, the current release, loads under sbt 1.13.1 and
  runs JUnit Jupiter 6.1.3 (the version its `jupiter-interface` is built against, so `V.junit` is
  that).
- **V9** (T003): no BSL licence and no `com.typesafe.akka` on a Flink streamlet's path.
  `sbt cartCounter/dependencyTree`, with a placeholder class using `StreamExecutionEnvironment` and
  `KafkaSource`, resolves 55 modules; none is under `com.typesafe.akka`, and no jar's
  `META-INF/LICENSE*` or `NOTICE*` names the Business Source License. The one module with "akka" in
  its name, `flink-rpc-akka-loader:2.2.1`, embeds `flink-rpc-akka.jar`, whose 6,365 classes under
  `org/apache/pekko/` are Apache Pekko **1.1.2** (`pekko-actor_2.12`, `-remote`, `-stream`, …,
  Apache-2.0, as its NOTICE lists), and which holds no class under `akka/`. R1 said Pekko 1.4.0;
  the embedded jar is 1.1.2, which changes nothing: it is Flink's own classloader's, never on a
  streamlet's. In the image `flink:2.2.1-java17` (JDK 17.0.21), `grep -il "business source"` over
  `/opt/flink/NOTICE` and `/opt/flink/licenses/` finds nothing; the NOTICE names Akka only as the
  origin of Pekko's code.
- **V1, V3** (T004): a ten-line job (`KafkaSource` → `map` → `KafkaSink`, at least once) compiled
  with `--release 17` against the Flink SDK module's compile classpath, put in an image
  `FROM flink:2.2.1-java17` as `/opt/flink/usrlib/streamlet.jar` beside the twelve jars of
  `cartCounter`'s runtime classpath (Flink's own modules being provided: the connector
  `5.0.0-2.2`, `kafka-clients-4.2.0`, Jackson, zstd, lz4, snappy, slf4j-api, commons-lang3), run
  as `standalone-job --job-classname v1.Copy` with one task manager against `apache/kafka:3.9.1`,
  read `a b c` from `in` and wrote `A B C` to `out`. No fat jar: every jar in `usrlib` is on the
  job's classpath. `flink-connector-base`, which the connector declares provided, is inside
  `flink-dist-2.2.1.jar` (108 classes under `org/apache/flink/connector/base/`), so the SDK keeps
  it `Provided`. No fallback to sbt-assembly is needed.
- **V2** (T005): `OAUTHBEARER` with client credentials against a stub token endpoint (the JDK's
  `HttpServer`, answering a JWT), an `Admin` created with `SASL_PLAINTEXT`, the endpoint in
  `sasl.oauthbearer.token.endpoint.url` and `clientId`/`clientSecret` in the JAAS line. The login
  runs when the client is constructed, so a refusal is immediate.
  - **The login callback handler** is
    `org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler` in both 3.9.2
    and 4.2.0 (3.9.2 still carries the deprecated `…oauthbearer.secured.` alias).
  - **kafka-clients 3.9.2** (Pekko Streams, the operator): logs in with no system property. It needs
    `jackson-databind` on the classpath (without it: `ClassNotFoundException:
    com.fasterxml.jackson.databind.ObjectMapper`); kafka-clients declares no dependency on it.
  - **kafka-clients 4.2.0** (Flink): refuses the endpoint unless
    `-Dorg.apache.kafka.sasl.oauthbearer.allowed.urls=<endpoint>` is set ("The URL cannot be
    accessed due to restrictions"), and needs `org.bitbucket.b_c:jose4j` (without it:
    `ClassNotFoundException: org.jose4j.keys.resolvers.VerificationKeyResolver`) as well as
    `jackson-databind`.
  - **Consequences**: the Pekko Streams SDK depends on `jackson-databind`; the Flink SDK depends on
    `jose4j` and `jackson-databind` (both reach the image's `usrlib`). The Flink SDK sets the
    allowed-URLs property from the token endpoints its settings name before it builds a source or
    sink, which covers the job manager (where the source's enumerator makes its `Admin`) and a
    laptop. A task manager is a different JVM that never runs the main, so the operator also writes
    `env.java.opts.taskmanager: -Dorg.apache.kafka.sasl.oauthbearer.allowed.urls=<endpoints>` into a
    `FlinkDeployment`'s `flinkConfiguration` when one of the streamlet's topics is on a cluster whose
    connection settings name a token endpoint (T032). The operator's own `Admin` is on 3.9.2 and
    needs nothing.
- **V4, V5, V7** (T006), on a scratch kind cluster (kind 0.x, Kubernetes on arm64; the suite's k3s
  runs the same manifest):
  - **V4**: the operator's chart 1.16.1 (from `archive.apache.org/dist/flink/`) renders with
    `--set webhook.create=false --set watchNamespaces={shop} --include-crds` into 14 objects: four
    CRDs, the operator's Deployment, ConfigMap, two ServiceAccounts, three Roles and three
    RoleBindings. Its image is `ghcr.io/apache/flink-kubernetes-operator:1c895a3` (the chart names a
    commit tag, recorded as `V.flinkOperatorImage`). Applied with two Namespaces in front, it rolls
    out with no cert-manager. The job service account it makes in the watched namespace is
    **`flink`**, so `FLOW_FLINK_SERVICE_ACCOUNT` defaults to that. The manifest is committed at
    `operator/src/test/resources/flink-operator.yaml`; `just flink-operator-manifest` runs
    `kustomization/flink-operator-manifest.sh`, which regenerates it.
  - **V5**: Flink's `StateMachineExample` as a `FlinkDeployment` on `flink:2.2.1-java17`,
    `flinkVersion: v2_2`, `upgradeMode: last-state`, Kubernetes high availability, the checkpoint,
    savepoint and HA directories under `s3://flow-state/…`, `s3.endpoint` and
    `s3.path-style-access: "true"` in `flinkConfiguration`, and `AWS_ACCESS_KEY_ID`,
    `AWS_SECRET_ACCESS_KEY` and `ENABLE_BUILT_IN_PLUGINS=flink-s3-fs-presto-2.2.1.jar` given to
    `flink-main-container` as `envFrom` a Secret: `STABLE`/`RUNNING`/`READY` in about 40 s, and
    checkpoints completed into MinIO (`checkpoints/<job>/chk-35`, `ha/…` listed by the client).
    The environment works, so R11's `env` stands and no key goes into `flink-config`.
  - **V7**: suspended (`job.state: suspended`), the deployment is `SUSPENDED` with the job
    `FINISHED`, and its HA ConfigMaps (`ha-cluster-config-map`, `ha-<job>-config-map`) stay, which
    is what lets `last-state` resume. Deleting the `FlinkDeployment` then removes every ConfigMap it
    made: only `kube-root-ca.crt` is left. A deployment recreated at a new epoch finds no HA
    metadata.
  - **MinIO's images**: `minio/minio` on Docker Hub and `quay.io/minio/minio` no longer answer
    anonymously. The suite and the local overlay use Chainguard's build of MinIO
    (`cgr.dev/chainguard/minio`, RELEASE.2026-11-16) and its client `cgr.dev/chainguard/minio-client`
    for the bucket Job, both pinned by digest in `V.minioImage` and `V.minioClientImage`. Chainguard
    publishes only `latest` anonymously; a digest it stops serving is a bump of those two values.
  - **kind and Docker's containerd store**: `kind load docker-image` fails with `ctr: content digest
    … not found` for multi-platform images; `docker save --platform linux/<arch> <image> | kind load
    image-archive` loads them. `deploy-local.sh` loads that way.

**V11** (2026-11-22): holds. `pekkoSdk` on Scala 3.3.8 compiles `from(inlet)(flow: Stage[A] =>
Stage.Out)` with `Stage[A] = FlowWithContext[Record[A], Offset, Record[A], Offset, NotUsed]` and
`Stage.Out = FlowWithContext[Nothing, Offset, Emit | Seq[Emit] | Option[Emit], Offset, Any]`:
`FlowWithContext` is contravariant in its input and context and covariant in its output, so an
author's `.map(r => out(…))`, `.mapConcat(…)` to `Seq[Emit]` or `.map(…)` to `Option[Emit]` is a
`Stage.Out` without a type annotation, warning-free under `-Wunused:all`. The fallback is not
needed. One thing for T020: an element `mapConcat` splits into several carries the same offset
on each, so the runner must not commit that offset until the last of them is confirmed.

**Laptop walkthrough, Pekko Streams row** (T025, 2026-11-22): `docker compose up -d kafka`
(Kafka on `localhost:9092`, topics created on first use); `flow local samples/cart-pipeline/blueprint.conf
--descriptors samples/cart-pipeline/flow --bootstrap-servers localhost:9092 -o /tmp/carts` wrote
`/tmp/carts/cleaner/streamlet.properties`; `FLOW_CONFIG_DIR=/tmp/carts/cleaner sbt cartCleaner/run`
had `/tmp/flow/ready` within seconds; `produce.sh localhost:9092 50` then `verify.sh localhost:9092 50`
exited 0 ("45 clean cart events, each keyed by its cart": the five malformed dropped). Stopped
with SIGTERM, the runner drained: `ready` removed, and `carts.cleaner.in` at lag 0 on all three
partitions, the last record of each committed. The counter's row waits for T041.

**Laptop walkthrough, Flink row** (T030, 2026-11-22): the blueprint gained the counter on
`clean-carts` and the topic `cart-counts`; `flow local` wrote both streamlets' settings; the cleaner
ran from its staged script and the counter as `java -cp <its Compile classpath> cart.counter.CartCounter`
(what `sbt cartCounter/run` runs; two sbt runs do not share a build). The counter logged once
"local=true: running on Flink's local environment; state is kept only while this process runs".
`produce.sh localhost:9092 50` then `verify.sh localhost:9092 50` exited 0: 45 clean events, and
36 counts on `carts.cart-counts`, one per cart (nine of them) and closed minute (four), each of one
event; the fifth minute stays open, as no event passes its end. Stopped and run again: the group
`carts.counter.in` had committed 45 of 45, and in 45 seconds the new run wrote nothing (it read
from the committed offsets). `produce.sh` again with the same start wrote 36 more counts of 1:
with its state the run would have dropped those events as late, so it started with none.
Found on the way, and fixed in the sample and the SDK:
- **A window's record timestamp is the window's time.** Counts for events of 2026-01-01 were
  deleted by the broker's seven-day retention minutes after they were written, which looked like a
  consumer that read nothing. `produce.sh` now starts at the current minute (a fixed start is its
  third argument); the delivery page should say that a Flink outlet's timestamps are event time.
- **The local environment checkpoints** every 10 s in memory (`FlinkStreamlets`), or a laptop run
  never commits an offset and a restarted command reads its topic again from the beginning.
- **`flink-clients` is provided** to the counter's `run` (the local executor), with an slf4j 1.7
  binding at `warn`, so the notice is seen; neither reaches the image.

## Found during implementation

**The Pekko Streams runner commits on the next offset** (2026-11-22, asked at T019). A stage's
`mapConcat` can make several elements of one record, each carrying its offset; committing each
offset on its first element written (`CommitWhen.OffsetFirstObserved`) would lose the rest on a
crash between them. The runner commits with `CommitWhen.NextOffsetObserved`: an offset once the
partition's next offset is seen, and the last one when the stream stops. The promise of C:sdks
holds as written. The cost, accepted: on a partition gone quiet its last record stays uncommitted
(lag 1 in the status) and is read again after a restart, which at least once allows. Rejected: a
stream per record (the author's `mapAsync` loses its concurrency across records), and committing
on the first write (breaks the `mapConcat` promise, a contract change).

How it is built (T020): not with `CommitWhen.NextOffsetObserved`, whose committer flow left a
drained stop's last offset uncommitted in `RunnerKafkaSuite`, but in
`CommitAfterWrite.flowCommittingAfter`: after the ordered write, a `statefulMap` holds each
partition's latest confirmed offset, releases it when a later one of that partition is confirmed,
and releases every held offset when the stream completes (never on failure). The committer then
commits what it is handed (`OffsetFirstObserved`). The consumer's stop timeout is zero, so a failed
run leaves its group at once and its restart is assigned promptly; a stop is
`drainAndShutdown`.

**V6: what a new image that cannot read the state shows** (2026-11-22, T038, on k3s with the
Flink Kubernetes Operator 1.16.1 and Flink 2.2.1). The counter's window state is a POJO `Tally`;
the suite's third image retypes its field `long count` to `int count`. Deployed with
`upgradeMode: last-state` over a running job, the Flink operator suspends the old deployment and
submits the new one, which recovers from the high-availability checkpoint and fails in
`WindowOperator`'s restore. It then restarts under `exponential-delay` without end: the
`FlinkDeployment` stays `lifecycleState: DEPLOYED`, `jobManagerDeploymentStatus: READY`, the job
cycles `CREATED`, `RUNNING`, `RESTARTING`, and `status.error` stays empty. R17's assumption, a
`FAILED` deployment with the cause in `status.error`, does not hold. What does show it: a
`Warning` event `JobException` on the `FlinkDeployment`, repeated each restart, whose message is
the top of the trace only (581 characters): `java.lang.Exception: Exception while creating
StreamOperatorStateContext.` with the first frames; the causes (`Could not restore keyed state
backend for WindowOperator…`, `Failed when trying to restore heap backend`) are only in the job
manager's log. Decided, within R17's "pin the matched text to what V6 shows": the operator lists
the namespace's `JobException` events (RBAC: `list` on core `events`), keeps the latest per
`FlinkDeployment` uid that is no older than the current job's `jobStatus.startTime`, and a Flink
streamlet whose image is not the last stable one (`lastStableSpec`) and whose latest such event
names a restore failure is failed with `StateUnreadable`. `Exception while creating
StreamOperatorStateContext` joins R17's texts. The same exception with the image that last ran is a
failure without the condition; a different exception after a new image is still rolling out. The
contract is unchanged: the condition, its phase `Degraded` and how it clears. The checkpoint
directory is untouched: nothing in the operator writes to the store.

**The last stable image is under `spec`** (2026-11-22, T038). `status.reconciliationStatus.lastStableSpec`
is JSON text of the form `{"spec":{…,"image":…},"resource_metadata":{…}}`, not the bare spec. Read
at the top level, the image was never found, so no new image counted as changed and a job that
could not restore its state stayed `rolling out` while its `JobException` event repeated each
restart (twenty times in twelve minutes on k3s). The unit fixture had the bare shape and passed;
it now has the operator's. The Flink operator keeps that event as one core event, its `count` and
`lastTimestamp` moving on each restart; `eventTime` and `series` stay empty.

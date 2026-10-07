# Limitations

> What ankka-flow does not do — delivery guarantees, contract formats, SDKs, ingress, brokers and tooling — stated plainly so a design does not depend on it.

Source: https://flow.ankka.cloud/reference/limitations/
What ankka-flow does not do, stated plainly so a design does not depend on it.

- **At least once, never exactly once.** A record whose emits were written but whose offsets were not
  yet committed is delivered again after a failure or a rebalance. Streamlet logic must tolerate
  repeats. [Delivery and failure](../concepts/delivery.md) explains when they happen.
- **JSON contracts only.** Avro and Protobuf contracts, schema registries and compatibility rules
  between schema versions are not supported, and nothing checks a record against its schema. A new
  contract version is a new schema name.
- **Two SDKs.** [Scala](scala-sdk.md) and [Python](python-sdk.md). Any other language can implement
  the streamlet protocol directly and prove itself with the descriptor fixtures and the conformance
  suite; see [Adding a language SDK](../contributing/language-sdks.md).
- **One built-in stage.** The [Neo4j merge sink](neo4j-merge-sink.md) is the only streamlet that runs
  inside the sidecar, and there is no way to add a stage from outside the sidecar image; every other
  streamlet's logic runs in its own process.
- **Neo4j 5.26 or later, written only.** The merge sink needs dynamic labels in `MERGE`, which Neo4j
  5.26 introduced; it refuses to open against an older server. No other graph database is supported,
  a pipeline cannot supply its own Cypher, and the sink never reads from the graph.
- **Graph deltas state, never change.** A delta carries an element's whole state with a version from
  one source entity; there are no increments, no merging of properties from several sources, and
  tombstones mark elements rather than removing them. See [Graph deltas](graph-deltas.md).
- **The platform writes no delete markers.** A tombstone stays in a delta topic as its element's last
  record; removing it from the topic is its writer's to do, and nothing removes a tombstoned element
  from the graph database.
- **A rebuilt graph is the live graph.** A graph rebuilt from its delta topic has every element that
  is not marked deleted exactly as it was; tombstoned elements whose records have left the topic are
  not in it. See [Rebuild a graph from its delta topic](../deploy/rebuild-a-graph.md).
- **No per-partition state in the process.** A streamlet sees batches of whichever partitions are
  assigned to its pod, and the assignment changes on every rebalance. State that must survive belongs
  in a topic or an external store.
- **No dead-letter topic and no skipping.** A batch that fails every time stalls its partition,
  indefinitely, by design. The stall is visible as lag, a metric and a `PartitionStalled` event;
  skipping a record is the streamlet's own decision.
- **No HTTP or gRPC ingress into a pipeline.** Records enter a pipeline through a Kafka topic.
- **Kafka only.** No other broker, and no single topic spread over several Kafka clusters.
- **Existing topics are never changed, their cleanup policy included.** The operator creates a managed topic once; a different
  partition count, replication or topic configuration on an existing topic is reported, not applied.
- **No user interface and no hosted control plane.** A pipeline is a Kubernetes resource, operated
  with `kubectl` and the `flow` CLI.
- **Four platforms for the native CLI.** `flow` ships as a native binary for macOS (Apple silicon
  and Intel) and Linux (x64 and arm64), through Homebrew or a release archive. Anywhere else it is
  built from source and runs on a JVM. There is no Windows build and no apt, dnf or winget package.
- **Records under 4 MiB.** A record larger than the protocol's per-record limit, just under 4 MiB
  including its key and headers, fails the stream and stalls its partition.

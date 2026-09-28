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
- **One SDK.** Python. Any other language can implement the streamlet protocol directly and prove
  itself with the descriptor fixtures and the conformance suite; see
  [Adding a language SDK](../contributing/language-sdks.md).
- **No stages built into the sidecar.** Every streamlet's logic runs in its own process; the sidecar
  runs none itself.
- **No per-partition state in the process.** A streamlet sees batches of whichever partitions are
  assigned to its pod, and the assignment changes on every rebalance. State that must survive belongs
  in a topic or an external store.
- **No dead-letter topic and no skipping.** A batch that fails every time stalls its partition,
  indefinitely, by design. The stall is visible as lag, a metric and a `PartitionStalled` event;
  skipping a record is the streamlet's own decision.
- **No HTTP or gRPC ingress into a pipeline.** Records enter a pipeline through a Kafka topic.
- **Kafka only.** No other broker, and no single topic spread over several Kafka clusters.
- **Existing topics are never changed.** The operator creates a managed topic once; a different
  partition count, replication or topic configuration on an existing topic is reported, not applied.
- **No user interface and no hosted control plane.** A pipeline is a Kubernetes resource, operated
  with `kubectl` and the `flow` CLI.
- **A JVM CLI.** `flow` needs a JVM and is built from source; there is no native binary or package.
- **Records under 4 MiB.** A record larger than the protocol's per-record limit, just under 4 MiB
  including its key and headers, fails the stream and stalls its partition.

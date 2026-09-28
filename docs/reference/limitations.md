# Limitations

What version one does not do, stated plainly so a design does not depend on it.

- **At least once, never exactly once.** A record whose emits were written but whose offsets were
  not yet committed is delivered again after a failure. Streamlet logic must tolerate repeats.
- **JSON contracts only.** Avro and Protobuf contracts, schema registries and schema evolution are
  not supported. A new contract version is a new schema name.
- **One SDK.** Python. A Scala or TypeScript SDK follows; any language can implement the protocol
  and prove itself with the conformance suite.
- **No stages built into the sidecar.** The sidecar's batch processor is written so a stage can run
  inside it, but version one ships none.
- **No per-partition state in the process.** A streamlet sees batches of any partition assigned to
  its pod, and the assignment changes on every rebalance.
- **No HTTP or gRPC ingress into a pipeline.** Records enter a pipeline through a Kafka topic.
- **One broker type.** Kafka. No UI, no hosted control plane, no multi-cluster Kafka within one topic.
- **A JVM CLI.** `flow` needs a JVM; a native binary is a later feature.
- **A failing batch stalls its partition, indefinitely.** By design: nothing is skipped or
  dead-lettered. The stall is visible as lag, a metric and a warning event.

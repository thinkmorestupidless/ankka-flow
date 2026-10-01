package com.thinkmorestupidless.ankka.flow.protocol

import ankka.flow.v1.discovery.*

/**
 * The streamlet descriptors the platform ships rather than an SDK writes: stages the sidecar runs
 * itself, with no process container.
 *
 * A blueprint names one as `builtin/<name>`. The CLI verifies it like any descriptor, the operator
 * renders it as a pod with only the sidecar, and the sidecar refuses a deployed descriptor that is
 * not its own copy of the same built-in. The canonical JSON of each is committed under
 * `protocol/fixtures/builtin/` and checked byte for byte, so a change here is a visible change.
 */
object Builtins:

  /** How a blueprint names a built-in descriptor: `builtin/neo4j-merge-sink`. */
  val Prefix = "builtin/"

  /** The graph delta contract the merge sink reads. */
  val GraphDeltaSchema = "ankka.graph-delta.v1"

  /** Merges graph deltas into Neo4j, one transaction per batch (feature 002). */
  val neo4jMergeSink: Spec =
    Spec(
      ProtocolVersion.Current.toString,
      Some(SdkInfo("ankka-flow-sidecar", "0.0.0")),
      Some(
        StreamletDescriptor(
          name = "neo4j-merge-sink",
          description = "Merges graph deltas into Neo4j in one transaction per batch.",
          inlets = Seq(
            Port(
              "in",
              Some(
                Contract(
                  Fingerprint.Format,
                  GraphDeltaSchema,
                  Fingerprint.fingerprint(GraphDeltaSchema)
                )
              )
            )
          ),
          outlets = Seq.empty,
          configParameters = Seq(
            ConfigParameter(
              "secret",
              "The Secret in the pipeline's namespace holding uri, username, password and optionally database.",
              ConfigType.STRING,
              ""
            ),
            ConfigParameter(
              "transaction-timeout",
              "How long one batch's transaction may take before it fails and the batch is retried.",
              ConfigType.DURATION,
              "30s"
            )
          )
        )
      )
    )

  val all: Vector[Spec] = Vector(neo4jMergeSink)

  def byName(name: String): Option[Spec] = all.find(_.getStreamlet.name == name)

  /** Every built-in's name, for messages that list what exists. */
  def names: Vector[String] = all.map(_.getStreamlet.name)

package com.thinkmorestupidless.ankka.flow.operator

import java.nio.charset.StandardCharsets.UTF_8
import java.security.MessageDigest

import com.thinkmorestupidless.ankka.flow.crd.{FlowSerialization, StreamletSpec}
import com.thinkmorestupidless.ankka.flow.descriptor.{
  Delivery,
  Descriptor,
  DescriptorJson,
  Engine,
  InletSettings,
  Json,
  OutletSettings,
  Settings as FlowSettings
}

/**
 * A streamlet's `streamlet.properties` (contracts/streamlet-settings.md): its parameters, and the
 * topic, group, client id and resolved connection of each of its own ports, and no other's (R6).
 * The hash covers the file and the descriptor, and is the pod template's config-hash annotation, so
 * a change to either rolls the streamlet and nothing else does.
 */
final case class StreamletFiles(properties: String, descriptorJson: String):
  def hash: String =
    MessageDigest
      .getInstance("SHA-256")
      .digest((descriptorJson + "\n---\n" + properties).getBytes(UTF_8))
      .map(b => f"$b%02x")
      .mkString

object StreamletFiles:

  /** The key of the file in the streamlet's Secret. */
  val Key: String = FlowSettings.FileName

  /** A Flink outlet's transaction timeout: the broker's default maximum (R13). */
  val TransactionTimeoutMs = "900000"

  /**
   * `flow.<namespace>.<pipeline>.<streamlet>.<outlet>.e<epoch>`: unique to the job's state (R13).
   */
  def transactionalIdPrefix(
      namespace: String,
      pipeline: String,
      streamlet: String,
      outlet: String,
      epoch: Int
  ): String = s"flow.$namespace.$pipeline.$streamlet.$outlet.e$epoch"

  def render(
      pipeline: String,
      namespace: String,
      s: StreamletSpec,
      topics: Map[String, ResolvedTopic],
      epoch: Int = 1
  ): Either[String, StreamletFiles] =
    for
      streamlet <- Json
        .parse(FlowSerialization.mapper().writeValueAsString(s.descriptor))
        .flatMap(DescriptorJson.streamletFromJson)
        .left
        .map(e => s"streamlet '${s.name}': its descriptor does not parse: $e")
      parameters <- sequence(streamlet.parameters.map { p =>
        s.config
          .get(p.key)
          .map(text)
          .orElse(p.default)
          .map(p.key -> _)
          .toRight(s"streamlet '${s.name}': parameter '${p.key}' has no default and no value")
      })
      inlets  <- ports(s.name, s.inlets, topics)
      outlets <- ports(s.name, s.outlets, topics)
    yield
      val settings = FlowSettings(
        pipeline = pipeline,
        streamlet = s.name,
        namespace = namespace,
        local = false,
        parameters = parameters.toMap,
        inlets = inlets.map { (port, t) =>
          // Records a transaction aborted are never read: a managed topic may have a Flink writer.
          val committed =
            if t.managed then Map("isolation.level" -> "read_committed") else Map.empty
          InletSettings(
            port,
            t.name,
            Names.group(pipeline, s.name, port, t.groupPrefix),
            Names.clientId(pipeline, s.name, port),
            t.connectionConfig ++ t.consumerConfig ++
              Map("bootstrap.servers" -> t.bootstrapServers) ++ committed
          )
        },
        outlets = outlets.map { (port, t) =>
          val kafka =
            t.connectionConfig ++ t.producerConfig ++ Map("bootstrap.servers" -> t.bootstrapServers)
          // A Flink streamlet writes a managed topic in transactions; any other topic, at least
          // once, as ankka renders no transactional id rule for a machine (R13).
          if streamlet.engine == Engine.Flink && t.managed then
            OutletSettings(
              port,
              t.name,
              Names.clientId(pipeline, s.name, port),
              Delivery.ExactlyOnce,
              Some(transactionalIdPrefix(namespace, pipeline, s.name, port, epoch)),
              kafka ++ Map("transaction.timeout.ms" -> TransactionTimeoutMs)
            )
          else
            OutletSettings(
              port,
              t.name,
              Names.clientId(pipeline, s.name, port),
              Delivery.AtLeastOnce,
              None,
              kafka
            )
        }
      )
      StreamletFiles(
        FlowSettings.write(settings),
        DescriptorJson.write(Descriptor(Descriptor.Version, None, streamlet))
      )

  /** A parameter's value as the file carries it: the text of its type. */
  private def text(v: com.fasterxml.jackson.databind.JsonNode): String =
    if v.isTextual then v.asText else v.toString

  private def sequence[A](es: Vector[Either[String, A]]): Either[String, Vector[A]] =
    es.collectFirst { case Left(e) => e }.toLeft(es.collect { case Right(a) => a })

  private def ports(
      streamlet: String,
      bindings: Map[String, String],
      topics: Map[String, ResolvedTopic]
  ): Either[String, Vector[(String, ResolvedTopic)]] =
    sequence(bindings.toVector.sortBy(_._1).map { (port, id) =>
      topics
        .get(id)
        .map(port -> _)
        .toRight(
          s"streamlet '$streamlet': port '$port' names topic '$id', which the resource does not declare"
        )
    })

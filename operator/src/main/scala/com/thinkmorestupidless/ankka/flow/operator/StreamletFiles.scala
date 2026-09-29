package com.thinkmorestupidless.ankka.flow.operator

import java.nio.charset.StandardCharsets.UTF_8
import java.security.MessageDigest

import com.thinkmorestupidless.ankka.flow.crd.{FlowSerialization, StreamletSpec}
import com.thinkmorestupidless.ankka.flow.protocol.{DescriptorJson, Json}

/**
 * The two files the sidecar reads (contracts/sidecar.md): the deployed descriptor, and
 * `streamlet.conf` with every port's topic, group, client id and resolved connection. Their hash is
 * the pod template's config-hash annotation, so a change to either rolls the streamlet and nothing
 * else does (FR-024a). For a built-in streamlet the hash also covers the version of the Secret its
 * stage reads, so a rotated credential rolls it; the Secret's values are never in either file.
 */
final case class StreamletFiles(
    descriptorJson: String,
    streamletConf: String,
    secretVersion: Option[String] = None
):
  def hash: String =
    MessageDigest
      .getInstance("SHA-256")
      .digest(
        (descriptorJson + "\n---\n" + streamletConf + secretVersion.fold("")(v =>
          s"\n---\nsecret:$v"
        ))
          .getBytes(UTF_8)
      )
      .map(b => f"$b%02x")
      .mkString

object StreamletFiles:

  def render(
      pipeline: String,
      s: StreamletSpec,
      topics: Map[String, ResolvedTopic],
      secretVersion: Option[String] = None
  ): Either[String, StreamletFiles] =
    for
      streamlet <- Json
        .parse(FlowSerialization.mapper().writeValueAsString(s.descriptor))
        .flatMap(DescriptorJson.streamletFromJson)
        .left
        .map(e => s"streamlet '${s.name}': its descriptor does not parse: $e")
      spec = ankka.flow.v1.discovery.Spec(
        protocolVersion =
          com.thinkmorestupidless.ankka.flow.protocol.ProtocolVersion.Current.toString,
        sdk = None,
        streamlet = Some(streamlet)
      )
      inlets  <- ports(s.name, s.inlets, topics)
      outlets <- ports(s.name, s.outlets, topics)
    yield
      val config = Json.compact(
        Json.Obj(
          s.config.toVector
            .sortBy(_._1)
            .map((k, v) => k -> Json.parse(v.toString).getOrElse(Json.Str(v.asText)))
        )
      )
      val in = inlets.map { (port, t) =>
        val id = Names.clientId(pipeline, s.name, port)
        s"""    ${q(port)} {
           |      topic = ${q(t.name)}
           |      group = ${q(Names.group(pipeline, s.name, port))}
           |      client-id = ${q(id)}
           |      bootstrap.servers = ${q(t.bootstrapServers)}
           |      connection-config ${block(t.connectionConfig)}
           |      consumer-config ${block(t.consumerConfig)}
           |      batch { max-records = ${t.batch.maxRecords}, max-bytes = ${t.batch.maxBytes} }
           |    }""".stripMargin
      }
      val out = outlets.map { (port, t) =>
        s"""    ${q(port)} {
           |      topic = ${q(t.name)}
           |      client-id = ${q(Names.clientId(pipeline, s.name, port))}
           |      bootstrap.servers = ${q(t.bootstrapServers)}
           |      connection-config ${block(t.connectionConfig)}
           |      producer-config ${block(t.producerConfig)}
           |    }""".stripMargin
      }
      // A built-in streamlet's stage reads its credentials from the mounted Secret, never from here.
      val stage =
        if !s.builtin then ""
        else s"""
             |  stage {
             |    name = ${q(streamlet.name)}
             |    neo4j { credentials-dir = ${q(BuiltinStages.CredentialsDir)} }
             |  }""".stripMargin
      val conf =
        s"""# Rendered by the ankka-flow operator for ${q(s.name)} of pipeline ${q(
            pipeline
          )}. Do not edit.
           |flow {
           |  pipeline = ${q(pipeline)}
           |  streamlet = ${q(s.name)}
           |  config = $config$stage
           |  inlets {
           |${in.mkString("\n")}
           |  }
           |  outlets {
           |${out.mkString("\n")}
           |  }
           |}
           |""".stripMargin
      StreamletFiles(DescriptorJson.write(spec), conf, secretVersion)

  private def ports(
      streamlet: String,
      bindings: Map[String, String],
      topics: Map[String, ResolvedTopic]
  ): Either[String, Vector[(String, ResolvedTopic)]] =
    val resolved = bindings.toVector.sortBy(_._1).map { (port, id) =>
      topics
        .get(id)
        .map(port -> _)
        .toRight(
          s"streamlet '$streamlet': port '$port' names topic '$id', which the resource does not declare"
        )
    }
    resolved.collectFirst { case Left(e) => e }.toLeft(resolved.collect { case Right(p) => p })

  /** A string as a HOCON-safe quoted value (JSON string syntax is valid HOCON). */
  private def q(s: String): String = Json.compact(Json.Str(s))

  /** Kafka properties as a HOCON block of quoted keys, so dots stay part of the key. */
  private def block(m: Map[String, String]): String =
    if m.isEmpty then "{}"
    else m.toVector.sortBy(_._1).map((k, v) => s"${q(k)} = ${q(v)}").mkString("{ ", ", ", " }")

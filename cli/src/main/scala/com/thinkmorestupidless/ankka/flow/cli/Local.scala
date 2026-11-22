package com.thinkmorestupidless.ankka.flow.cli

import java.nio.charset.StandardCharsets.ISO_8859_1
import java.nio.file.{Files, Path}

import com.thinkmorestupidless.ankka.flow.blueprint.{TopicSettings, VerifiedTopic}
import com.thinkmorestupidless.ankka.flow.crd.ResetRequest
import com.thinkmorestupidless.ankka.flow.descriptor.{
  Delivery,
  InletSettings,
  Json,
  OutletSettings,
  Settings
}

/**
 * `flow local`: verify as `flow verify` does, then write each streamlet's `streamlet.properties`
 * for a laptop: `local=true`, every topic on one broker in plaintext, every outlet at least once,
 * topic names and groups as a cluster would have them. Touches no cluster and no broker.
 */
object Local:

  final case class Request(
      in: Verify.Inputs,
      bootstrapServers: String,
      out: Path,
      ankkaBootstrapServers: Option[String]
  )

  /** The namespace a laptop's settings name, for logs and client ids. */
  val Namespace = "local"

  /** Every streamlet's settings, by its name in the blueprint. */
  def settings(r: Request): Either[Vector[String], Vector[Settings]] =
    Verify.run(r.in).map { v =>
      val pipeline =
        v.blueprint.name.getOrElse(r.in.blueprint.getFileName.toString.takeWhile(_ != '.'))
      def ports(streamlet: String, outlet: Boolean): Vector[(String, VerifiedTopic)] =
        v.blueprint.topics.flatMap(t =>
          t.connections
            .filter(p => p.streamlet.name == streamlet && p.isOutlet == outlet)
            .map(_.portName -> t)
        )
      def topic(t: VerifiedTopic) = TopicSettings.fromConfig(v.overrides.topicConfig(t))
      v.blueprint.streamlets.map { s =>
        val broker = Map("bootstrap.servers" -> r.bootstrapServers)
        Settings(
          pipeline = pipeline,
          streamlet = s.name,
          namespace = Namespace,
          local = true,
          parameters = v.parameters(s.name).map((k, j) => k -> text(j)).toMap,
          inlets = ports(s.name, outlet = false).map { (port, t) =>
            val group = ResetRequest.groupId(pipeline, s.name, port)
            InletSettings(
              port,
              t.kafkaName(pipeline),
              group,
              group,
              topic(t).consumerConfig ++ broker
            )
          },
          outlets = ports(s.name, outlet = true).map { (port, t) =>
            OutletSettings(
              port,
              t.kafkaName(pipeline),
              s"$pipeline.${s.name}.$port",
              Delivery.AtLeastOnce,
              None,
              topic(t).producerConfig ++ broker
            )
          }
        )
      }
    }

  /** Writes the files; the paths written, or every problem with nothing written. */
  def run(r: Request): Either[Vector[String], Vector[Path]] =
    settings(r).map(_.map { s =>
      val dir = r.out.resolve(s.streamlet)
      Files.createDirectories(dir)
      val file = dir.resolve(Settings.FileName)
      Files.write(file, Settings.write(s).getBytes(ISO_8859_1))
      file
    })

  /** A parameter's value as the file carries it: the text of its type. */
  private def text(j: Json): String = j match
    case Json.Str(s)  => s
    case Json.Num(n)  => n.bigDecimal.toPlainString
    case Json.Bool(b) => b.toString
    case other        => Json.compact(other)

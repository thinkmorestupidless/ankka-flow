package com.thinkmorestupidless.ankka.flow.operator

import java.io.StringReader
import java.util.Properties

import scala.jdk.CollectionConverters.*
import scala.util.Try

/**
 * Where Flink streamlets keep their state (research R11): the Secret `flink-state` in the
 * operator's clusters namespace. `dir` is a URI Flink can write; `flink-config` is merged into
 * every `FlinkDeployment`'s configuration; `env` goes to Flink's pods as environment, through a
 * Secret in the pipeline's namespace, never into a spec anyone who reads the resource can read.
 */
final case class StateStore(
    dir: String,
    flinkConfig: Map[String, String] = Map.empty,
    env: Map[String, String] = Map.empty
):
  /** The three directories of one streamlet's state at one epoch (R10, R12). */
  def paths(namespace: String, pipeline: String, streamlet: String, epoch: Int): StateStore.Paths =
    val base = s"${dir.stripSuffix("/")}/$namespace/$pipeline/$streamlet/$epoch"
    StateStore.Paths(s"$base/checkpoints", s"$base/savepoints", s"$base/ha")

object StateStore:
  val SecretName = "flink-state"

  final case class Paths(checkpoints: String, savepoints: String, ha: String)

  /** The Secret in the pipeline's namespace that holds `env`: `flow-<pipeline>-flink-state`. */
  def envSecretName(pipeline: String): String = s"flow-$pipeline-$SecretName"

  /**
   * From the Secret's decoded data; none when it has no `dir`, which the operator reports as
   * `StateStoreMissing`. Properties text that does not parse is a problem named by its key.
   */
  def fromData(data: Map[String, String]): Either[String, Option[StateStore]] =
    def props(key: String): Either[String, Map[String, String]] =
      data.get(key).filter(_.trim.nonEmpty) match
        case None => Right(Map.empty)
        case Some(text) =>
          Try {
            val p = new Properties()
            p.load(new StringReader(text))
            p.asScala.toMap.map((k, v) => k.trim -> v.trim)
          }.toEither.left.map(e => s"$SecretName: $key is not properties text: ${e.getMessage}")
    data.get("dir").map(_.trim).filter(_.nonEmpty) match
      case None => Right(None)
      case Some(dir) =>
        for
          config <- props("flink-config")
          env    <- props("env")
        yield Some(StateStore(dir, config, env))

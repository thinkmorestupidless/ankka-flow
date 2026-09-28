package com.thinkmorestupidless.ankka.flow.sidecar

import java.nio.file.{Path, Paths}

import scala.concurrent.duration.*
import scala.util.Try

import com.typesafe.config.ConfigFactory

/**
 * The sidecar's environment (contracts/sidecar.md). System property, then variable, then default.
 */
final case class Settings(
    processHost: String,
    processPort: Int,
    configDir: Path,
    stateDir: Path,
    metricsPort: Int,
    discoveryTimeout: FiniteDuration,
    stallWarningAfter: FiniteDuration,
    reconnectMaxBackoff: FiniteDuration,
    pod: Option[Settings.Pod]
):
  def processAddress: String = s"$processHost:$processPort"

object Settings:

  /** Where the Kubernetes sink posts events: the pod's own name and namespace (downward API). */
  final case class Pod(name: String, namespace: String, apiHost: String, apiPort: Int)

  val DefaultProcessAddress = "127.0.0.1:9010"

  def load(env: Map[String, String] = sys.env): Either[String, Settings] =
    def get(key: String): Option[String] =
      sys.props.get(key.toLowerCase.replace('_', '.')).orElse(env.get(key)).filter(_.nonEmpty)
    def duration(key: String, default: FiniteDuration): Either[String, FiniteDuration] =
      get(key) match
        case None => Right(default)
        case Some(v) =>
          Try(
            ConfigFactory.parseString(s"v = \"$v\"").getDuration("v").toMillis.millis
          ).toEither.left
            .map(_ => s"$key: '$v' is not a duration")
    def int(key: String, default: Int): Either[String, Int] =
      get(key).fold(Right(default))(v => v.toIntOption.toRight(s"$key: '$v' is not a number"))

    val address = get("FLOW_PROCESS_ADDRESS").getOrElse(DefaultProcessAddress)
    for
      hp <- address.lastIndexOf(':') match
        case -1 => Left(s"FLOW_PROCESS_ADDRESS: '$address' is not host:port")
        case i =>
          address
            .substring(i + 1)
            .toIntOption
            .map(address.substring(0, i) -> _)
            .toRight(s"FLOW_PROCESS_ADDRESS: '$address' is not host:port")
      metrics   <- int("FLOW_METRICS_PORT", 2050)
      discovery <- duration("FLOW_DISCOVERY_TIMEOUT", 60.seconds)
      stall     <- duration("FLOW_STALL_WARNING_AFTER", 5.minutes)
      backoff   <- duration("FLOW_RECONNECT_MAX_BACKOFF", 30.seconds)
    yield Settings(
      processHost = hp._1,
      processPort = hp._2,
      configDir = Paths.get(get("FLOW_CONFIG_DIR").getOrElse("/etc/flow/config")),
      stateDir = Paths.get(get("FLOW_STATE_DIR").getOrElse("/tmp/flow")),
      metricsPort = metrics,
      discoveryTimeout = discovery,
      stallWarningAfter = stall,
      reconnectMaxBackoff = backoff,
      pod =
        for
          host <- env.get("KUBERNETES_SERVICE_HOST")
          name <- env.get("FLOW_POD_NAME")
          ns   <- env.get("FLOW_POD_NAMESPACE")
        yield Pod(
          name,
          ns,
          host,
          env.get("KUBERNETES_SERVICE_PORT").flatMap(_.toIntOption).getOrElse(443)
        )
    )

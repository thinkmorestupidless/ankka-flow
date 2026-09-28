package com.thinkmorestupidless.ankka.flow.operator

import scala.concurrent.duration.*

/**
 * The operator's configuration (contracts/resource-and-operator.md): system property, then
 * environment variable, then default, as ankka's.
 */
final case class Settings(
    sidecarImage: Option[String],
    clustersNamespace: String,
    resyncInterval: FiniteDuration,
    retryMinBackoff: FiniteDuration,
    retryMaxBackoff: FiniteDuration,
    maxConcurrentReconciles: Int,
    reportingInstance: String
):
  def backoffFor(attempt: Int): FiniteDuration =
    val grown = retryMinBackoff * math.pow(2, (attempt - 1).min(20)).toLong
    grown.min(retryMaxBackoff)

object Settings:

  def load(
      env: Map[String, String] = sys.env,
      props: Map[String, String] = sys.props.toMap
  ): Settings =
    def get(envKey: String): Option[String] =
      val prop = "flow.operator." + envKey
        .stripPrefix("FLOW_")
        .stripPrefix("OPERATOR_")
        .toLowerCase
        .replace('_', '-')
      props.get(prop).orElse(env.get(envKey)).filter(_.nonEmpty)
    def seconds(key: String, default: Int) =
      get(key).flatMap(_.toIntOption).getOrElse(default).seconds
    Settings(
      sidecarImage = get("FLOW_SIDECAR_IMAGE"),
      clustersNamespace = get("FLOW_KAFKA_CLUSTERS_NAMESPACE")
        .orElse(env.get("FLOW_OPERATOR_NAMESPACE"))
        .getOrElse("ankka-flow"),
      resyncInterval = seconds("FLOW_OPERATOR_RESYNC_SECONDS", 300),
      retryMinBackoff = seconds("FLOW_OPERATOR_RETRY_MIN_BACKOFF_SECONDS", 1),
      retryMaxBackoff = seconds("FLOW_OPERATOR_RETRY_MAX_BACKOFF_SECONDS", 300),
      maxConcurrentReconciles =
        get("FLOW_OPERATOR_MAX_CONCURRENT_RECONCILES").flatMap(_.toIntOption).getOrElse(4),
      reportingInstance = env.getOrElse("HOSTNAME", "ankka-flow-operator")
    )

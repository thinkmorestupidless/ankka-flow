package com.thinkmorestupidless.ankka.flow.cli.mcp

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}

import scala.jdk.CollectionConverters.*
import scala.util.Using

import com.thinkmorestupidless.ankka.flow.cli.KubernetesReset
import com.thinkmorestupidless.ankka.flow.crd.{AnkkaFlow, FlowSerialization}
import io.fabric8.kubernetes.api.model.Pod
import io.fabric8.kubernetes.client.{Config, KubernetesClient, KubernetesClientBuilder}

/**
 * The named cluster, reached as `kubectl --context <context> -n <namespace>` reaches it, one client
 * per call. Nothing here reads the kubeconfig's current context.
 */
private[cli] final class Cluster(
    val named: NamedCluster,
    configure: NamedCluster => Config = Cluster.configure
):

  private val Pipeline  = "flow.ankka.thinkmorestupidless.com/pipeline"
  private val Streamlet = "flow.ankka.thinkmorestupidless.com/streamlet"

  def newClient(): KubernetesClient =
    new KubernetesClientBuilder()
      .withConfig(configure(named))
      .withKubernetesSerialization(FlowSerialization())
      .build()

  private def withClient[A](f: KubernetesClient => A): A =
    Using.resource(newClient())(f)

  private def flows(client: KubernetesClient) =
    client.resources(classOf[AnkkaFlow]).inNamespace(named.namespace)

  private def pipelineOrFail(client: KubernetesClient, name: String): AnkkaFlow =
    Option(flows(client).withName(name).get())
      .getOrElse(
        throw IllegalArgumentException(s"no pipeline '$name' in namespace '${named.namespace}'")
      )

  def listPipelines(): String =
    withClient { client =>
      val items = flows(client).list().getItems.asScala.toVector.sortBy(_.getMetadata.getName)
      if items.isEmpty then s"no pipelines in namespace '${named.namespace}'"
      else
        items
          .map { f =>
            val status = Option(f.getStatus)
            val ready = status
              .map(_.streamlets.map(s => s"${s.name} ${s.ready}/${s.desired}").mkString(", "))
              .getOrElse("")
            s"${f.getMetadata.getName}  ${status.map(_.phase).filter(_.nonEmpty).getOrElse("Pending")}  ${status.map(_.detail).getOrElse("")}  $ready".trim
          }
          .mkString("\n")
    }

  def describePipeline(name: String): String =
    withClient { client =>
      val flow   = pipelineOrFail(client, name)
      val status = Option(flow.getStatus)
      val spec   = flow.getSpec
      val streamlets = spec.streamlets
        .map(s => s"  ${s.name}: image ${s.image}, replicas ${s.replicas}")
        .mkString("\n")
      val statusText = status.fold("  (no status yet)") { st =>
        val ss = st.streamlets
          .map(s =>
            s"    ${s.name}: ${s.ready}/${s.desired}${
                if s.detail.nonEmpty then s" — ${s.detail}" else ""
              }"
          )
          .mkString("\n")
        val ts = st.topics
          .map(t =>
            s"    ${t.id}: ${
                if t.exists then "exists" else "missing"
              }${if t.detail.nonEmpty then s" — ${t.detail}" else ""}"
          )
          .mkString("\n")
        s"  phase: ${st.phase}${
            if st.detail.nonEmpty then s"\n  detail: ${st.detail}" else ""
          }\n  streamlets:\n$ss\n  topics:\n$ts"
      }
      // The namespace's events, filtered here: a field selector on involvedObject is not something
      // every API server (or the suite's double) honours, and a namespace's events are few.
      val all = client.v1.events.inNamespace(named.namespace).list().getItems.asScala.toVector
      val onFlow = all.filter(e =>
        Option(e.getInvolvedObject).exists(o => o.getKind == "AnkkaFlow" && o.getName == name)
      )
      val stalled = all.filter(e =>
        e.getReason == "PartitionStalled" &&
          Option(e.getInvolvedObject)
            .flatMap(o => Option(o.getName))
            .exists(_.startsWith(s"flow-${spec.pipeline}-"))
      )
      val events = (onFlow ++ stalled)
        .sortBy(e =>
          Option(e.getLastTimestamp).orElse(Option(e.getEventTime).map(_.getTime)).getOrElse("")
        )
        .takeRight(20)
        .map(e =>
          s"  ${Option(e.getLastTimestamp).getOrElse("")}  ${e.getType}  ${e.getReason}  ${e.getMessage}"
        )
      s"pipeline $name (${spec.pipeline}) in ${named.namespace}\nstreamlets:\n$streamlets\nstatus:\n$statusText\nevents:\n${
          if events.isEmpty then "  (none)" else events.mkString("\n")
        }"
    }

  private def pods(
      client: KubernetesClient,
      flow: AnkkaFlow,
      streamlet: Option[String]
  ): Vector[Pod] =
    val selector =
      client.pods.inNamespace(named.namespace).withLabel(Pipeline, flow.getSpec.pipeline)
    streamlet
      .fold(selector)(s => selector.withLabel(Streamlet, s))
      .list()
      .getItems
      .asScala
      .toVector
      .sortBy(_.getMetadata.getName)

  def logs(name: String, streamlet: String, container: String, lines: Int): String =
    if !Set("process", "sidecar")(container) then
      throw IllegalArgumentException(s"container '$container' is not process or sidecar")
    withClient { client =>
      val flow = pipelineOrFail(client, name)
      if !flow.getSpec.streamlets.exists(_.name == streamlet) then
        throw IllegalArgumentException(s"pipeline '$name' has no streamlet '$streamlet'")
      val found = pods(client, flow, Some(streamlet))
      if found.isEmpty then s"no pod of streamlet '$streamlet' in pipeline '$name' yet"
      else
        found
          .map { pod =>
            val log = client.pods
              .inNamespace(named.namespace)
              .withName(pod.getMetadata.getName)
              .inContainer(container)
              .tailingLines(lines)
              .getLog
            s"--- ${pod.getMetadata.getName} [$container]\n$log"
          }
          .mkString("\n")
    }

  def lag(name: String): String =
    withClient { client =>
      val flow  = pipelineOrFail(client, name)
      val found = pods(client, flow, None)
      if found.isEmpty then s"no pods of pipeline '$name' yet"
      else
        val rows = found.flatMap { pod =>
          val streamlet =
            Option(pod.getMetadata.getLabels).flatMap(l => Option(l.get(Streamlet))).getOrElse("?")
          Using.resource(
            client.pods
              .inNamespace(named.namespace)
              .withName(pod.getMetadata.getName)
              .portForward(2050)
          ) { forward =>
            val metrics = Cluster.get(s"http://127.0.0.1:${forward.getLocalPort}/metrics")
            Lag
              .parse(metrics)
              .map(l =>
                s"$streamlet  ${pod.getMetadata.getName}  ${l.clientId}  ${l.topic}  ${l.partition}  ${l.lag}"
              )
          }
        }
        if rows.isEmpty then "no inlet lag reported yet"
        else ("streamlet  pod  client_id  topic  partition  lag" +: rows).mkString("\n")
    }

  /** Creates the pipeline or updates it in place, as `kubectl apply` does. */
  def apply(yaml: String): String =
    withClient { client =>
      val resource = FlowSerialization.fromYaml(yaml)
      val applied  = flows(client).resource(resource).createOr(_.update())
      s"applied ${applied.getMetadata.getName} in ${named.namespace}"
    }

  def reset(name: String, streamlets: List[String]): ToolResult =
    new KubernetesReset(() => newClient()).request(name, streamlets, Some(named.namespace)) match
      case Left(problems) => ToolResult(problems.mkString("\n"), isError = true)
      case Right(id)      => ToolResult(s"reset requested for '$name': $id")

private[cli] object Cluster:

  /** `kubectl --context <context>`: the kubeconfig's named context, never its current one. */
  def configure(named: NamedCluster): Config =
    val config = Config.autoConfigure(named.context)
    if config.getCurrentContext == null || config.getCurrentContext.getName != named.context then
      throw IllegalArgumentException(
        s"the kubeconfig has no context '${named.context}' (named in flow.toml)"
      )
    config.setNamespace(named.namespace)
    config

  private lazy val http =
    HttpClient.newBuilder().connectTimeout(java.time.Duration.ofSeconds(5)).build()

  def get(url: String): String =
    val response = http.send(
      HttpRequest
        .newBuilder(URI.create(url))
        .timeout(java.time.Duration.ofSeconds(10))
        .GET()
        .build(),
      HttpResponse.BodyHandlers.ofString()
    )
    if response.statusCode != 200 then
      throw IllegalArgumentException(s"$url answered ${response.statusCode}")
    response.body

/** One `records_lag` sample of a sidecar's metrics. */
final case class LagSample(clientId: String, topic: String, partition: String, lag: Double)

object Lag:
  private val Sample =
    """kafka_consumer_consumer_fetch_manager_metrics_records_lag\{([^}]*)\}\s+([0-9.eE+-]+|NaN)""".r
  private val Label = """(\w+)="([^"]*)"""".r

  /** The lag samples of a `/metrics` text, in the order written. */
  def parse(metrics: String): Vector[LagSample] =
    metrics.linesIterator.collect { case Sample(labels, value) =>
      val l = Label.findAllMatchIn(labels).map(m => m.group(1) -> m.group(2)).toMap
      LagSample(
        l.getOrElse("client_id", ""),
        l.getOrElse("topic", ""),
        l.getOrElse("partition", ""),
        value.toDoubleOption.getOrElse(Double.NaN)
      )
    }.toVector

package com.thinkmorestupidless.ankka.flow.cli.mcp

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
        .map(s => s"  ${s.name}: engine ${s.engine}, image ${s.image}, replicas ${s.replicas}")
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
        val cs = st.conditions
          .map(c =>
            s"    ${c.`type`}: ${c.status}${if c.message.nonEmpty then s" — ${c.message}" else ""}"
          )
          .mkString("\n")
        s"  phase: ${st.phase}${
            if st.detail.nonEmpty then s"\n  detail: ${st.detail}" else ""
          }\n  conditions:\n${
            if cs.isEmpty then "    (none)" else cs
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

  /**
   * The last lines of every container of every pod carrying the streamlet's labels: a Pekko Streams
   * streamlet's pods, or a Flink streamlet's job manager and task managers.
   */
  def logs(name: String, streamlet: String, lines: Int): String =
    withClient { client =>
      val flow = pipelineOrFail(client, name)
      if !flow.getSpec.streamlets.exists(_.name == streamlet) then
        throw IllegalArgumentException(s"pipeline '$name' has no streamlet '$streamlet'")
      val found = pods(client, flow, Some(streamlet))
      if found.isEmpty then s"no pod of streamlet '$streamlet' in pipeline '$name' yet"
      else
        found
          .flatMap { pod =>
            val containers =
              Option(pod.getSpec).toVector.flatMap(_.getContainers.asScala.map(_.getName))
            containers.map { container =>
              val log = client.pods
                .inNamespace(named.namespace)
                .withName(pod.getMetadata.getName)
                .inContainer(container)
                .tailingLines(lines)
                .getLog
              s"--- ${pod.getMetadata.getName} [$container]\n$log"
            }
          }
          .mkString("\n")
    }

  /**
   * Each streamlet's inlets with their topic, group and lag, as the operator last wrote them on the
   * status: nothing is port-forwarded or asked of Kafka here.
   */
  def lag(name: String): String =
    withClient { client =>
      val flow     = pipelineOrFail(client, name)
      val reported = Option(flow.getStatus).toList.flatMap(_.streamlets).map(s => s.name -> s).toMap
      val rows = flow.getSpec.streamlets.flatMap { s =>
        val inlets = reported.get(s.name).map(_.inlets).getOrElse(Nil).map(i => i.name -> i).toMap
        s.inlets.toList.sortBy(_._1).map { (inlet, topicId) =>
          inlets.get(inlet) match
            case Some(i) =>
              s"${s.name}  ${s.engine}  $inlet  ${i.topic}  ${i.group}  ${i.lag.fold("unknown")(_.toString)}"
            case None => s"${s.name}  ${s.engine}  $inlet  $topicId  -  unknown"
        }
      }
      if rows.isEmpty then s"pipeline '$name' has no inlets"
      else ("streamlet  engine  inlet  topic  group  lag" +: rows).mkString("\n")
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

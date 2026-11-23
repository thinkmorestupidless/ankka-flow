package com.thinkmorestupidless.ankka.flow.operator

import java.time.{Clock, Instant}

import scala.concurrent.duration.*

import com.thinkmorestupidless.ankka.flow.crd.AnkkaFlow
import io.fabric8.kubernetes.client.KubernetesClient
import org.slf4j.LoggerFactory

/** Read, observe, render, execute. Level-triggered: the same reconcile runs whatever changed. */
final class PipelineReconciler(
    client: KubernetesClient,
    settings: Settings,
    k8s: Fabric8Executor,
    kafka: KafkaExecutor,
    clock: Clock = Clock.systemUTC()
) extends Reconciler:

  private val log = LoggerFactory.getLogger(classOf[PipelineReconciler])

  /**
   * How soon to look again while something only time will change, such as pods terminating before a
   * reset.
   */
  val WaitingRequeue: FiniteDuration = 5.seconds

  def reconcile(ref: PipelineRef): Option[FiniteDuration] =
    Option(client.resources(classOf[AnkkaFlow]).inNamespace(ref.namespace).withName(ref.name).get())
      .flatMap { resource =>
        val base = k8s.observe(resource)
        val resolved =
          resource.getSpec.topics.flatMap(t => TopicResolution.resolve(t, base.clusters).toOption)
        val byId     = resolved.map(t => t.id -> t).toMap
        val pipeline = resource.getSpec.pipeline
        val lag = (for
          s              <- resource.getSpec.streamlets
          (inlet, topic) <- s.inlets
          t              <- byId.get(topic)
          group = Names.group(pipeline, s.name, inlet, t.groupPrefix)
          n <- kafka.lag(group, t)
        yield group -> n).toMap
        // What each topic on a machine's cluster needs: Read for an inlet, Write for an outlet.
        val needs = resource.getSpec.streamlets
          .flatMap(s =>
            s.inlets.values.map(_ -> Grant.Right.Read) ++
              s.outlets.values.map(_ -> Grant.Right.Write)
          )
          .groupMap(_._1)(_._2)
        val grants = (for
          t <- resolved if t.machine.isDefined
          n <- needs.get(t.id)
        yield t.id -> kafka.grants(t, n.toSet)).toMap
        val observed =
          base.copy(
            topics = resolved.map(t => t.name -> kafka.describe(t)).toMap,
            lag = lag,
            grants = grants
          )
        val rendered = Rendering.render(resource, settings, observed, Instant.now(clock).toString)
        rendered.actions.foreach { action =>
          log.debug("{}: {}", ref, action.describe)
          action match
            case Action.EnsureTopic(t) => kafka.ensure(t)
            case Action.DeleteTopic(t) => kafka.delete(t)
            case Action.ResetGroup(target) =>
              val outcome = kafka.reset(target) match
                case Right(n) =>
                  Action.RecordEvent(
                    "ResetOffsets",
                    Events.Normal,
                    s"${target.groupId}: $n partition(s) of '${target.topic.name}' to earliest"
                  )
                case Left(reason) =>
                  // Kafka's refusal of a group with members is a warning, never a pipeline error (S4.2).
                  Action.RecordEvent(
                    "ResetOffsetsFailed",
                    Events.Warning,
                    s"${target.groupId}: $reason"
                  )
              k8s.execute(resource, outcome)
            case other => k8s.execute(resource, other)
        }
        // A reset waiting for pods to go is woken by nothing the informers see: look again soon.
        Option.when(rendered.actions.exists {
          case Action.RecordEvent("ResetRefused", _, note) => note.contains(" waits: ")
          case _                                           => false
        })(WaitingRequeue)
      }

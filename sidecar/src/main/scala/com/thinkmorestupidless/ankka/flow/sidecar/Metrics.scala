package com.thinkmorestupidless.ankka.flow.sidecar

import java.lang.management.ManagementFactory
import javax.management.{ObjectName, StandardMBean}

import scala.collection.mutable
import scala.util.Try

/** The sidecar's per-partition bean, read by the JMX exporter agent (research R7). */
trait PartitionMetricsMBean:
  def getInFlight: Int
  def getStalledSeconds: Long

/**
 * One bean per inlet partition the sidecar has run, under
 * `ankka.flow:type=sidecar,inlet=<inlet>,partition=<p>`, exported as `ankka_flow_sidecar_in_flight`
 * and `ankka_flow_sidecar_stalled_seconds`.
 */
final class Metrics(stalls: Stalls):

  private val server     = ManagementFactory.getPlatformMBeanServer
  private val registered = mutable.Set.empty[(String, Int)]
  private val stageSeen  = mutable.Set.empty[(String, Int)]

  /** A built-in stage's counters; its beans appear for partitions it has processed. */
  val stage: StageMetrics = new StageMetrics

  /** Registers beans for partitions seen since the last call. Called every second. */
  def refresh(): Unit = synchronized {
    stalls.seen.filterNot(registered).foreach { (inlet, partition) =>
      val bean = new PartitionMetricsMBean:
        def getInFlight: Int        = stalls.inFlight(inlet, partition)
        def getStalledSeconds: Long = stalls.stalledSeconds(inlet, partition)
      val name = Metrics.name(inlet, partition)
      Try(server.registerMBean(new StandardMBean(bean, classOf[PartitionMetricsMBean]), name))
      registered += inlet -> partition
    }
    stage.seen.filterNot(stageSeen).foreach { (inlet, partition) =>
      val name = Metrics.name(inlet, partition, "stage")
      Try(
        server.registerMBean(
          new StandardMBean(stage.of(inlet, partition), classOf[StageMetricsMBean]),
          name
        )
      )
      stageSeen += inlet -> partition
    }
  }

object Metrics:
  def name(inlet: String, partition: Int, kind: String = "sidecar"): ObjectName =
    new ObjectName(
      s"ankka.flow:type=$kind,inlet=${ObjectName.quote(inlet).drop(1).dropRight(1)},partition=$partition"
    )

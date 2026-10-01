package com.thinkmorestupidless.ankka.flow.sidecar

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.LongAdder

import scala.jdk.CollectionConverters.*

/** A built-in stage's per-partition bean, read by the JMX exporter agent (research R11). */
trait StageMetricsMBean:
  def getDeltasWritten: Long
  def getDeltasStale: Long
  def getBatchesFailed: Long
  def getDeleteMarkers: Long

/**
 * What a built-in stage did, per inlet partition: deltas applied, deltas found stale (folded away
 * in a batch or filtered by the version guard), and batches that failed. Exported as
 * `ankka_flow_stage_deltas_written_total`, `…_deltas_stale_total`, `…_batches_failed_total` and
 * `…_delete_markers_total` (records with no value, passed over).
 */
final class StageMetrics extends Neo4jMergeStage.Record:

  final class Counters extends StageMetricsMBean:
    val written                = new LongAdder
    val stale                  = new LongAdder
    val failed                 = new LongAdder
    val markers                = new LongAdder
    def getDeltasWritten: Long = written.sum
    def getDeltasStale: Long   = stale.sum
    def getBatchesFailed: Long = failed.sum
    def getDeleteMarkers: Long = markers.sum

  private val counters = new ConcurrentHashMap[(String, Int), Counters]()

  def of(inlet: String, partition: Int): Counters =
    counters.computeIfAbsent(inlet -> partition, _ => new Counters)

  def seen: Set[(String, Int)] = counters.keySet.asScala.toSet

  def applied(inlet: String, partition: Int, written: Int, stale: Int, markers: Int): Unit =
    val c = of(inlet, partition)
    c.written.add(written.toLong)
    c.stale.add(stale.toLong)
    c.markers.add(markers.toLong)

  def failed(inlet: String, partition: Int): Unit = of(inlet, partition).failed.increment()

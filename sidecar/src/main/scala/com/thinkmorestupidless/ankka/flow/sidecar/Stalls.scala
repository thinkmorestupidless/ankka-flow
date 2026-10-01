package com.thinkmorestupidless.ankka.flow.sidecar

import java.time.Clock
import java.util.concurrent.ConcurrentHashMap

import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

/**
 * Per (inlet, partition): when the oldest batch not yet committed was first sent. Survives the
 * sidecar's reconnects, so a batch that fails every time shows a growing stall. A stall past the
 * threshold is warned once; it clears when a batch of that partition commits (FR-012a).
 */
final class Stalls(threshold: FiniteDuration, events: EventSink, clock: Clock = Clock.systemUTC()):

  private final case class Entry(since: Long, warned: Boolean)

  private val entries = new ConcurrentHashMap[(String, Int), Entry]()
  private val known   = ConcurrentHashMap.newKeySet[(String, Int)]()

  @volatile var lastError: String = "none yet"

  def sent(inlet: String, partition: Int): Unit =
    known.add((inlet, partition))
    entries.putIfAbsent((inlet, partition), Entry(clock.millis, warned = false)): Unit

  /**
   * A partition assigned to this sidecar: its metrics exist from now, at zero, before any batch.
   */
  def assigned(inlet: String, partition: Int): Unit = known.add((inlet, partition)): Unit

  /** 1 while a batch of the partition is sent and not yet committed. */
  def inFlight(inlet: String, partition: Int): Int =
    if entries.containsKey((inlet, partition)) then 1 else 0

  /** Every partition this sidecar has sent a batch of, for the metrics. */
  def seen: Vector[(String, Int)] = known.asScala.toVector.sorted

  def committed(inlet: String, partition: Int): Unit =
    entries.remove((inlet, partition)): Unit

  def forget(inlet: String, partition: Int): Unit =
    entries.remove((inlet, partition)): Unit

  /** Forgets every stall of `inlet` except those of `partitions`: what an assignment moved away. */
  def retain(inlet: String, partitions: Set[Int]): Unit =
    entries.keySet.asScala.toVector
      .filter((i, p) => i == inlet && !partitions.contains(p))
      .foreach(entries.remove(_))

  def stalledSeconds(inlet: String, partition: Int): Long =
    Option(entries.get((inlet, partition))).fold(0L)(e => (clock.millis - e.since) / 1000)

  def partitions: Vector[(String, Int)] = entries.keySet.asScala.toVector.sorted

  /** Called every second: warns once for each partition that has crossed the threshold. */
  def check(describe: (String, Int) => String): Unit =
    val now = clock.millis
    entries.asScala.foreach { case (key @ (inlet, partition), e) =>
      if !e.warned && now - e.since >= threshold.toMillis then
        if entries.replace(key, e, e.copy(warned = true)) then
          events.warning(
            "PartitionStalled",
            s"${describe(inlet, partition)}: inlet '$inlet' partition $partition has not committed for ${(now - e.since) / 1000}s; last error: $lastError"
          )
    }

package com.thinkmorestupidless.ankka.flow.operator

import java.util.concurrent.{
  ConcurrentHashMap,
  Executors,
  LinkedBlockingQueue,
  ScheduledExecutorService,
  TimeUnit
}
import java.util.concurrent.atomic.AtomicBoolean

import scala.concurrent.duration.*
import scala.util.control.NonFatal

import org.slf4j.LoggerFactory

final case class PipelineRef(namespace: String, name: String):
  override def toString: String = s"$namespace/$name"

trait Reconciler:
  /** Reconciles once; `Some(delay)` asks to run again after it even if nothing changes. */
  def reconcile(ref: PipelineRef): Option[FiniteDuration]

/**
 * ankka's work queue: a ref is queued at most once, one reconcile per ref at a time, a change that
 * arrives mid-reconcile runs it again after, and a failure backs off per ref.
 */
final class WorkQueue(settings: Settings, reconcile: PipelineRef => Option[FiniteDuration]):
  import WorkQueue.*

  private val log      = LoggerFactory.getLogger(classOf[WorkQueue])
  private val pending  = new LinkedBlockingQueue[PipelineRef]()
  private val queued   = ConcurrentHashMap.newKeySet[PipelineRef]()
  private val inFlight = ConcurrentHashMap.newKeySet[PipelineRef]()
  private val dirty    = ConcurrentHashMap.newKeySet[PipelineRef]()
  private val attempts = new ConcurrentHashMap[PipelineRef, Integer]()
  private val running  = new AtomicBoolean(false)
  // What each worker is reconciling and since when: the watchdog's view.
  private val started = new ConcurrentHashMap[PipelineRef, (Thread, Long)]()

  private val scheduler: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor {
    r =>
      val t = new Thread(r, "ankka-flow-operator-scheduler")
      t.setDaemon(true)
      t
  }
  private val workers = Executors.newVirtualThreadPerTaskExecutor()

  def enqueue(ref: PipelineRef): Unit = synchronized {
    if inFlight.contains(ref) then dirty.add(ref): Unit
    else if queued.add(ref) then pending.put(ref)
  }

  def enqueueAfter(ref: PipelineRef, delay: FiniteDuration): Unit =
    scheduler.schedule((() => enqueue(ref)): Runnable, delay.toMillis, TimeUnit.MILLISECONDS): Unit

  /** Runs `pass` every `interval` until stopped; a failing pass is logged and the next one runs. */
  def every(interval: FiniteDuration)(pass: () => Unit): Unit =
    val task: Runnable = () =>
      try pass()
      catch case NonFatal(e) => log.warn("observe pass failed", e)
    scheduler.scheduleWithFixedDelay(
      task,
      interval.toMillis,
      interval.toMillis,
      TimeUnit.MILLISECONDS
    ): Unit

  def start(): Unit =
    if running.compareAndSet(false, true) then
      (1 to settings.maxConcurrentReconciles).foreach(_ => workers.submit((() => loop()): Runnable))
      every(WatchdogInterval)(() => watchdog())

  /**
   * A reconcile that runs long holds its pipeline: every change to it waits behind it. Each one
   * past `SlowReconcile` is logged with where its thread is, once per interval, so a hang names
   * itself.
   */
  private def watchdog(): Unit =
    val now = System.nanoTime()
    started.forEach { (ref, entry) =>
      val (thread, since) = entry
      val running         = (now - since).nanos
      if running > SlowReconcile then
        val where = thread.getStackTrace.take(40).map(f => s"\n    at $f").mkString
        log.warn(s"reconcile of $ref has run for ${running.toSeconds}s; its thread:$where")
    }

  def stop(): Unit =
    if running.compareAndSet(true, false) then
      scheduler.shutdownNow(): Unit
      workers.shutdownNow(): Unit

  private def loop(): Unit =
    while running.get() do
      try
        val ref = pending.take()
        synchronized {
          queued.remove(ref)
          inFlight.add(ref): Unit
        }
        started.put(ref, (Thread.currentThread(), System.nanoTime()))
        try
          val again = reconcile(ref)
          attempts.remove(ref)
          again.foreach(enqueueAfter(ref, _))
        catch
          case NonFatal(failure) =>
            val attempt = attempts.merge(ref, 1, (a, b) => a + b).intValue
            val delay   = settings.backoffFor(attempt)
            log.warn(s"reconcile of $ref failed (attempt $attempt); retrying in $delay", failure)
            enqueueAfter(ref, delay)
        finally
          started.remove(ref)
          synchronized {
            inFlight.remove(ref)
            if dirty.remove(ref) then enqueue(ref)
          }
      catch case _: InterruptedException => Thread.currentThread().interrupt()

object WorkQueue:
  private val WatchdogInterval: FiniteDuration = 30.seconds
  private val SlowReconcile: FiniteDuration    = 2.minutes

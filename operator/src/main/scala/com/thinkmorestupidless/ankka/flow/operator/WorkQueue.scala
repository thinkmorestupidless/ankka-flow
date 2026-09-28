package com.thinkmorestupidless.ankka.flow.operator

import java.util.concurrent.{
  ConcurrentHashMap,
  Executors,
  LinkedBlockingQueue,
  ScheduledExecutorService,
  TimeUnit
}
import java.util.concurrent.atomic.AtomicBoolean

import scala.concurrent.duration.FiniteDuration
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

  private val log      = LoggerFactory.getLogger(classOf[WorkQueue])
  private val pending  = new LinkedBlockingQueue[PipelineRef]()
  private val queued   = ConcurrentHashMap.newKeySet[PipelineRef]()
  private val inFlight = ConcurrentHashMap.newKeySet[PipelineRef]()
  private val dirty    = ConcurrentHashMap.newKeySet[PipelineRef]()
  private val attempts = new ConcurrentHashMap[PipelineRef, Integer]()
  private val running  = new AtomicBoolean(false)

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

  def start(): Unit =
    if running.compareAndSet(false, true) then
      (1 to settings.maxConcurrentReconciles).foreach(_ => workers.submit((() => loop()): Runnable))

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
          synchronized {
            inFlight.remove(ref)
            if dirty.remove(ref) then enqueue(ref)
          }
      catch case _: InterruptedException => Thread.currentThread().interrupt()

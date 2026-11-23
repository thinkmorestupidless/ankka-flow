package com.thinkmorestupidless.ankka.flow.pekko

import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.atomic.AtomicReference

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*
import scala.util.control.NonFatal

import com.thinkmorestupidless.ankka.flow.descriptor.{
  Delivery,
  InletSettings,
  OutletSettings,
  Settings
}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.{Callback, Producer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}
import org.apache.pekko.Done
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.kafka.{CommitterSettings, ConsumerSettings, ProducerSettings, Subscriptions}
import org.apache.pekko.kafka.ConsumerMessage.CommittableOffset
import org.apache.pekko.kafka.scaladsl.{Consumer, PartitionAssignmentHandler}
import org.apache.pekko.kafka.RestrictedConsumer
import org.apache.pekko.stream.{KillSwitches, Materializer, RestartSettings, UniqueKillSwitch}
import org.apache.pekko.stream.scaladsl.{Keep, RestartSource, Sink}

/**
 * Runs a streamlet from its settings: one stream per inlet, each its topic's committable source,
 * the inlet's stage, and every emit written to its outlet's topic and confirmed before the offset
 * it came from is committed. A failure anywhere restarts that inlet's stream, with backoff, from
 * the last commit, indefinitely. Offsets are committed on the next offset observed (or on a drained
 * stop), so a record made into several emits is committed after the last of them.
 */
object Runner:

  /** How the runner restarts, commits and touches `alive`; the defaults are the pod's. */
  final case class Options(
      restart: RestartSettings,
      committer: CommitterSettings => CommitterSettings,
      aliveEvery: FiniteDuration
  )

  object Options:
    val default: Options = Options(RestartSettings(1.second, 30.seconds, 0.2), identity, 5.seconds)

  /** Writes in flight per inlet, their results kept in order. */
  private val InFlight = 100

  /** The consumer's properties: the file's, its client id, and what the guarantee needs. */
  def consumerProperties(inlet: InletSettings): Map[String, String] =
    Map("auto.offset.reset" -> "earliest") ++ inlet.kafka ++ Map(
      "client.id"          -> inlet.clientId,
      "enable.auto.commit" -> "false"
    )

  /** The producer's properties: the file's, its client id, idempotent and acknowledged by all. */
  def producerProperties(outlet: OutletSettings): Map[String, String] =
    outlet.kafka ++ Map(
      "client.id"          -> outlet.clientId,
      "enable.idempotence" -> "true",
      "acks"               -> "all"
    )

  /**
   * Every declared port and parameter is given, nothing else is, and every outlet is at least once.
   */
  def check(streamlet: PekkoStreamlet, settings: Settings): Either[String, Unit] =
    val inlets  = streamlet.inlets.map(_.name)
    val outlets = streamlet.outlets.map(_.name)
    val keys    = streamlet.parameters.map(_.key)
    val problems =
      inlets
        .filterNot(p => settings.inlet(p).isDefined)
        .map(p => Settings.missing(s"inlet.$p.topic")) ++
        outlets
          .filterNot(p => settings.outlet(p).isDefined)
          .map(p => Settings.missing(s"outlet.$p.topic")) ++
        keys.filterNot(settings.parameters.contains).map(k => Settings.missing(s"parameter.$k")) ++
        settings.inlets
          .map(_.port)
          .filterNot(inlets.contains)
          .map(p =>
            s"${Settings.FileName} gives inlet '$p', which ${streamlet.name} does not declare"
          ) ++
        settings.outlets
          .map(_.port)
          .filterNot(outlets.contains)
          .map(p =>
            s"${Settings.FileName} gives outlet '$p', which ${streamlet.name} does not declare"
          ) ++
        settings.parameters.keys.toVector.sorted
          .filterNot(keys.contains)
          .map(k =>
            s"${Settings.FileName} gives parameter '$k', which ${streamlet.name} does not declare"
          ) ++
        settings.outlets.collect {
          case o if o.delivery != Delivery.AtLeastOnce =>
            s"outlet '${o.port}' is ${o.delivery.wire}; a Pekko Streams streamlet delivers at least once"
        }
    problems.headOption.toLeft(())

  /** The streams of one run; `stop` drains them, committing what was written. */
  final class Running private[pekko] (
      streams: Vector[InletStream],
      producers: Vector[Producer[Array[Byte], Array[Byte]]],
      probes: Probes,
      alive: org.apache.pekko.actor.Cancellable
  )(using ExecutionContext):

    /** Completes when every inlet's stream has ended. */
    val done: Future[Done] = Future.sequence(streams.map(_.done)).map(_ => Done)

    def stop(): Future[Done] =
      streams.foreach(_.stop())
      done.transform { result =>
        alive.cancel()
        probes.notReady()
        producers.foreach(p => scala.util.Try(p.close(java.time.Duration.ofSeconds(10))))
        result.map(_ => Done)
      }

  /** One inlet's restartable stream and the consumer control of its current run. */
  private[pekko] final class InletStream(
      val done: Future[Done],
      control: AtomicReference[Consumer.Control | Null],
      killSwitch: UniqueKillSwitch
  )(using ExecutionContext):
    def stop(): Unit =
      Option(control.get) match
        // The source completes; what is in flight is written and committed, then the consumer goes.
        case Some(c) => c.drainAndShutdown(done).onComplete(_ => ())
        case None    => killSwitch.shutdown()

  def start(
      streamlet: PekkoStreamlet,
      settings: Settings,
      system: ActorSystem[?],
      probes: Probes,
      options: Options = Options.default
  ): Running =
    check(streamlet, settings).left.foreach(e => throw new IllegalArgumentException(e))
    given ExecutionContext = system.executionContext
    given Materializer     = Materializer(system)
    val classic            = system.classicSystem

    val ctx   = Context.resolve(streamlet.parameters, settings.parameters, system)
    val logic = streamlet.logic(ctx)
    val parts = streamlet.inlets.map { i =>
      i -> logic
        .part(i)
        .getOrElse(
          throw new IllegalArgumentException(
            s"streamlet '${streamlet.name}' gives no stage for inlet '${i.name}'"
          )
        )
    }

    val producers = streamlet.outlets.map { o =>
      val s = settings.outlet(o.name).get
      o.name -> (
        s.topic,
        ProducerSettings(classic, new ByteArraySerializer, new ByteArraySerializer)
          .withProperties(producerProperties(s))
          .createKafkaProducer()
      )
    }.toMap

    def send(emits: Seq[Emit]): Future[Any] =
      Future
        .sequence(emits.map { e =>
          val (topic, producer) = producers.getOrElse(
            e.outlet.name,
            throw new IllegalStateException(
              s"an emit for outlet '${e.outlet.name}', which ${streamlet.name} does not declare"
            )
          )
          val r = e.record
          val record = new ProducerRecord[Array[Byte], Array[Byte]](
            topic,
            null,
            r.key.map(_.getBytes(UTF_8)).orNull,
            e.bytes,
            r.headers.map((k, v) => new RecordHeader(k, v): Header).asJava
          )
          val promise = Promise[RecordMetadata]()
          producer.send(
            record,
            new Callback:
              def onCompletion(m: RecordMetadata, e: Exception): Unit =
                if e == null then promise.success(m) else promise.failure(e)
          )
          promise.future
        })

    val readiness = new Readiness(parts.map(_._1.name), probes)

    // CommitAfterWrite holds each offset until the next of its partition is confirmed.
    val committer =
      options.committer(CommitAfterWrite.defaultCommitterSettings(CommitterSettings(classic)))

    val streams = parts.map { (inlet, part) =>
      val s       = settings.inlet(inlet.name).get
      val control = new AtomicReference[Consumer.Control | Null](null)
      val consumer = ConsumerSettings(classic, new ByteArrayDeserializer, new ByteArrayDeserializer)
        .withProperties(consumerProperties(s))
        .withGroupId(s.group)
        // A failed run's consumer leaves the group at once, so its restart is assigned promptly;
        // a stop drains first, so nothing written is left uncommitted by it.
        .withStopTimeout(Duration.Zero)
      val subscription =
        Subscriptions.topics(s.topic).withPartitionAssignmentHandler(readiness.handler(inlet.name))
      val flow = part.asInstanceOf[Logic.Part[Any]].flow
      val (kill, done) = RestartSource
        .onFailuresWithBackoff(options.restart) { () =>
          Consumer
            .committableSource(consumer, subscription)
            .mapMaterializedValue(c => control.set(c))
            .map(m =>
              decode(inlet.asInstanceOf[Inlet[Any]], m.record) -> offset(m.committableOffset)
            )
            .via(flow.asFlow)
            .map((emits, o) => emits -> o.committable.asInstanceOf[CommittableOffset])
            .via(CommitAfterWrite.flowCommittingAfter[Seq[Emit]](send, committer, InFlight))
        }
        .viaMat(KillSwitches.single)(Keep.right)
        .toMat(Sink.ignore)(Keep.both)
        .run()
      new InletStream(done, control, kill)
    }

    val alive =
      system.scheduler.scheduleAtFixedRate(Duration.Zero, options.aliveEvery)(() => probes.alive())
    new Running(streams, producers.values.map(_._2).toVector, probes, alive)

  private def decode[A](inlet: Inlet[A], r: ConsumerRecord[Array[Byte], Array[Byte]]): Record[A] =
    Record(
      Option(r.key).map(new String(_, UTF_8)),
      inlet.codec.decode(r.value),
      r.headers.toArray.toSeq.map(h => h.key -> h.value)
    )

  private def offset(c: CommittableOffset): Offset =
    new Offset(c.partitionOffset.key.partition, c.partitionOffset.offset, c)

  /** `ready` while every inlet's consumer holds its assignment. */
  private final class Readiness(inlets: Vector[String], probes: Probes):
    private val assigned = new java.util.concurrent.ConcurrentHashMap[String, Boolean]()

    private def update(): Unit =
      if inlets.forall(i => assigned.getOrDefault(i, false)) then probes.ready()
      else probes.notReady()

    def handler(inlet: String): PartitionAssignmentHandler = new PartitionAssignmentHandler:
      def onAssign(assigned: Set[TopicPartition], consumer: RestrictedConsumer): Unit =
        mark(true)
      def onRevoke(revoked: Set[TopicPartition], consumer: RestrictedConsumer): Unit = ()
      def onLost(lost: Set[TopicPartition], consumer: RestrictedConsumer): Unit      = mark(false)
      def onStop(current: Set[TopicPartition], consumer: RestrictedConsumer): Unit   = mark(false)
      private def mark(on: Boolean): Unit =
        try
          Readiness.this.assigned.put(inlet, on)
          update()
        catch case NonFatal(_) => ()

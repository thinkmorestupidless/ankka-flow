package com.thinkmorestupidless.ankka.flow.sidecar

import java.time.Duration as JDuration

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.jdk.CollectionConverters.*

import org.apache.kafka.clients.producer.{Callback, Producer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.header.internals.{RecordHeader, RecordHeaders}
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.kafka.ProducerSettings

/**
 * One producer per outlet, each with its own connection, so an outlet on other brokers needs
 * nothing special. Records keep the key and headers the process gave; a keyless record is left to
 * Kafka's default partitioner (research R8).
 *
 * A plain Kafka `Producer`, called on the caller's thread: `send` calls made in order reach a
 * partition in order. Pekko's `SendProducer` hops each send through a Future callback, which lets
 * two sends of one batch race; the carried RecordKafkaSuite caught exactly that.
 */
final class Producers(outlets: Map[String, OutletConfig])(using system: ActorSystem):

  private val producers: Map[String, (OutletConfig, Producer[Array[Byte], Array[Byte]])] =
    outlets.map { (name, o) =>
      val settings = ProducerSettings(system, new ByteArraySerializer, new ByteArraySerializer)
        .withBootstrapServers(o.bootstrapServers)
        // linger.ms 1 unless the outlet says otherwise: a batch's emits are sent together anyway,
        // and a longer linger is latency on every record of a quiet stream (SC-008).
        .withProperties(
          Map(
            "linger.ms" -> "1"
          ) ++ o.connectionConfig ++ o.producerConfig + ("client.id" -> o.clientId)
        )
      name -> (o, settings.createKafkaProducer())
    }

  /** Sends every record in order and completes when the broker has confirmed all of them. */
  def sendAll(records: Vector[EmittedRecord])(using
      ExecutionContext
  ): Future[Vector[RecordMetadata]] =
    // Every send is issued, in order, before any is awaited.
    Future.sequence(records.map(send))

  def send(r: EmittedRecord): Future[RecordMetadata] =
    producers.get(r.outlet) match
      case None => Future.failed(new StreamFailed(s"no producer for outlet '${r.outlet}'"))
      case Some((o, p)) =>
        val headers = new RecordHeaders(
          r.record.headers
            .map(h =>
              new RecordHeader(h.key, h.value.toByteArray): org.apache.kafka.common.header.Header
            )
            .asJava
        )
        val key     = r.record.key.map(_.toByteArray).orNull
        val promise = Promise[RecordMetadata]()
        try
          p.send(
            new ProducerRecord[Array[Byte], Array[Byte]](
              o.topic,
              null,
              key,
              r.record.value.toByteArray,
              headers
            ),
            new Callback:
              def onCompletion(metadata: RecordMetadata, exception: Exception): Unit =
                if exception == null then promise.success(metadata)
                else
                  promise.failure(
                    new StreamFailed(
                      s"producing to '${o.topic}' failed: ${exception.getMessage}",
                      exception
                    )
                  )
          ): Unit
        catch
          case e: Exception =>
            promise.tryFailure(
              new StreamFailed(s"producing to '${o.topic}' failed: ${e.getMessage}", e)
            ): Unit
        promise.future

  def close(): Future[Unit] =
    given ExecutionContext = system.dispatchers.lookup("pekko.actor.default-blocking-io-dispatcher")
    Future(producers.values.foreach(_._2.close(JDuration.ofSeconds(5))))

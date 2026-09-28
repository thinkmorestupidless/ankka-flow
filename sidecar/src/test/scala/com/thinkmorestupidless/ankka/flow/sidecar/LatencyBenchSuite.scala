package com.thinkmorestupidless.ankka.flow.sidecar

import java.time.Duration as JDuration
import java.util.{Properties, UUID}

import scala.collection.mutable
import scala.jdk.CollectionConverters.*

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}

/**
 * The sidecar's own contribution to latency, with no Docker hop and no Python: a steady 200
 * records/s through the whole sidecar to the double and back, measured send to outlet receive,
 * against a plain consumer of the input. Measures; asserts nothing. `-Dflow.benchmarks=on`.
 */
class LatencyBenchSuite extends KafkaSuite:

  override def munitIgnore: Boolean = !sys.props.get("flow.benchmarks").contains("on")

  test("bench.sidecar-latency") {
    val in     = createTopic(uniqueTopic("lat-in"), 3)
    val out    = createTopic(uniqueTopic("lat-out"), 3)
    val double = new ProcessDouble(TestSpecs.fixture("minimal"), ProcessDouble.keyed())
    val port   = double.start()
    val sidecar = new SidecarRun(
      TestSpecs.fixture("minimal"),
      SidecarRun.conf("b", "s", bootstrap, Seq("in" -> in), Seq("out" -> out)),
      port
    )
    eventually()(assert(sidecar.ready))
    val samples = Map(
      "input"  -> mutable.ArrayBuffer.empty[Double],
      "outlet" -> mutable.ArrayBuffer.empty[Double]
    )
    @volatile var running = true
    def reader(name: String, topic: String) = new Thread(() => {
      val p = new Properties()
      p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap)
      p.put(ConsumerConfig.GROUP_ID_CONFIG, s"lat-${UUID.randomUUID()}")
      p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
      p.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "5")
      val c = new KafkaConsumer(p, new ByteArrayDeserializer, new ByteArrayDeserializer)
      c.subscribe(java.util.List.of(topic))
      while running do
        c.poll(JDuration.ofMillis(20)).asScala.foreach { r =>
          val now = System.nanoTime / 1e6
          samples(name).synchronized(samples(name) += now - new String(r.value).toDouble)
        }
      c.close()
    })
    val readers = Seq(reader("input", in), reader("outlet", out))
    readers.foreach(_.start())
    Thread.sleep(3000)
    val p = new Properties()
    p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap)
    p.put(ProducerConfig.LINGER_MS_CONFIG, "0")
    val producer = new KafkaProducer(p, new ByteArraySerializer, new ByteArraySerializer)
    val start    = System.nanoTime
    (0 until 2000).foreach { i =>
      val due = start + i * 5_000_000L
      while System.nanoTime < due do Thread.onSpinWait()
      producer.send(
        new ProducerRecord[Array[Byte], Array[Byte]](
          in,
          s"k$i".getBytes,
          (System.nanoTime / 1e6).toString.getBytes
        )
      )
    }
    producer.close()
    Thread.sleep(3000)
    running = false
    readers.foreach(_.join())
    sidecar.stop(): Unit
    double.close()
    samples.foreach { (name, xs) =>
      val sorted       = xs.sorted
      def q(f: Double) = sorted((f * (sorted.size - 1)).toInt)
      println(
        f"$name%-7s n=${sorted.size}%5d p10=${q(0.1)}%6.1f p50=${q(0.5)}%6.1f p90=${q(0.9)}%6.1f p99=${q(0.99)}%6.1f ms"
      )
    }
  }

package com.thinkmorestupidless.ankka.flow.blueprint

import java.util.concurrent.atomic.AtomicInteger

import ankka.flow.v1.discovery.{
  ConfigParameter,
  ConfigType,
  Contract,
  Port,
  StreamletDescriptor as Proto
}
import com.thinkmorestupidless.ankka.flow.protocol.Fingerprint
import com.typesafe.config.ConfigFactory

/**
 * Builds descriptors and blueprints for the suites, in the spirit of Cloudflow's BlueprintBuilder
 * and StreamletDescriptorBuilder: define descriptors, use them as named streamlets, connect ports
 * through topics, and verify.
 */
object Builders:

  private val counter = new AtomicInteger(0)

  def json(schemaName: String): Contract =
    Contract(Fingerprint.Format, schemaName, Fingerprint.fingerprint(schemaName))

  def descriptor(
      name: String = s"streamlet-${counter.incrementAndGet()}",
      inlets: Seq[(String, Contract)] = Nil,
      outlets: Seq[(String, Contract)] = Nil,
      parameters: Seq[ConfigParameter] = Nil
  ): StreamletDescriptor =
    StreamletDescriptor(
      Proto(
        name = name,
        inlets = inlets.map((n, c) => Port(n, Some(c))),
        outlets = outlets.map((n, c) => Port(n, Some(c))),
        configParameters = parameters
      )
    )

  def ingress(schema: String = "foo.v1", outlet: String = "out") =
    descriptor(outlets = Seq(outlet -> json(schema)))
  def processor(in: String = "foo.v1", out: String = "foo.v1", inlet: String = "in") =
    descriptor(inlets = Seq(inlet -> json(in)), outlets = Seq("out" -> json(out)))
  def egress(schema: String = "foo.v1") = descriptor(inlets = Seq("in" -> json(schema)))
  def merge(in0: String, in1: String, out: String) =
    descriptor(
      inlets = Seq("in-0" -> json(in0), "in-1" -> json(in1)),
      outlets = Seq("out" -> json(out))
    )

  def param(key: String, t: ConfigType = ConfigType.STRING, default: String = "") =
    ConfigParameter(key, "", t, default)

  /** A blueprint under construction: `define`, `use`, `connect`, then `problems` or `verified`. */
  final case class Draft(
      descriptors: Vector[StreamletDescriptor] = Vector.empty,
      uses: Vector[(String, String)] = Vector.empty,
      topics: Vector[Topic] = Vector.empty
  ):
    def define(ds: StreamletDescriptor*): Draft          = copy(descriptors = descriptors ++ ds)
    def use(name: String, d: StreamletDescriptor): Draft = copy(uses = uses :+ (name -> d.name))
    def connect(topic: String, ports: String*): Draft    = connectTopic(Topic(topic), ports*)
    def connectTopic(topic: Topic, ports: String*): Draft =
      val (outs, ins) = ports.partition { p =>
        val (s, port) = p.splitAt(p.lastIndexOf('.'))
        uses
          .find(_._1 == s)
          .flatMap(u => descriptors.find(_.name == u._2))
          .exists(_.outlets.exists(_.name == port.drop(1)))
      }
      copy(topics =
        topics :+ topic.copy(
          producers = topic.producers ++ outs,
          consumers = topic.consumers ++ ins
        )
      )
    def blueprint: Blueprint =
      Blueprint(
        streamlets = uses.map((n, d) => StreamletRef(n, d)),
        topics = topics,
        streamletDescriptors = descriptors
      ).verify
    def problems: Vector[BlueprintProblem] = blueprint.problems

  def unmanaged(id: String, conf: String): Topic =
    Topic(id, kafkaConfig = ConfigFactory.parseString(conf))

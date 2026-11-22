package com.thinkmorestupidless.ankka.flow.blueprint

import java.util.concurrent.atomic.AtomicInteger

import com.thinkmorestupidless.ankka.flow.descriptor.{
  Contract,
  Descriptor,
  Engine,
  Parameter,
  ParameterType,
  Port,
  SdkInfo,
  StreamletDescriptor as Declared
}
import com.typesafe.config.ConfigFactory

/**
 * Builds descriptors and blueprints for the suites, in the spirit of Cloudflow's BlueprintBuilder
 * and StreamletDescriptorBuilder: define descriptors, use them as named streamlets, connect ports
 * through topics, and verify.
 */
object Builders:

  private val counter = new AtomicInteger(0)

  /** A contract by name alone. */
  def json(name: String): Contract = Contract(name, None)

  /** A contract with a fingerprint: 64 hex digits made from `seed`. */
  def fingerprinted(name: String, seed: Char): Contract =
    Contract(name, Some("sha256:" + seed.toString * 64))

  def descriptor(
      name: String = s"streamlet-${counter.incrementAndGet()}",
      inlets: Seq[(String, Contract)] = Nil,
      outlets: Seq[(String, Contract)] = Nil,
      parameters: Seq[Parameter] = Nil,
      engine: Engine = Engine.PekkoStreams
  ): StreamletDescriptor =
    StreamletDescriptor(
      Descriptor(
        Descriptor.Version,
        Some(SdkInfo("test", "0.0.0")),
        Declared(
          name,
          "",
          engine,
          inlets.map((n, c) => Port(n, c)).toVector,
          outlets.map((n, c) => Port(n, c)).toVector,
          parameters.toVector,
          None
        )
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

  def param(key: String, t: ParameterType = ParameterType.String, default: String = "") =
    Parameter(key, "", t, Option.when(default.nonEmpty)(default))

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

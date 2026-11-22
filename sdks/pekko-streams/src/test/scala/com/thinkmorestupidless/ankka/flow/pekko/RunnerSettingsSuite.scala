package com.thinkmorestupidless.ankka.flow.pekko

import java.nio.charset.StandardCharsets.ISO_8859_1
import java.nio.file.Files

import com.thinkmorestupidless.ankka.flow.descriptor.{
  Delivery,
  InletSettings,
  OutletSettings,
  Settings
}

/** What the runner takes from `streamlet.properties`, and what it refuses, before any broker. */
class RunnerSettingsSuite extends munit.FunSuite:

  final class Relay extends PekkoStreamlet("relay"):
    val in                         = inlet("in", Contract("x.v1"), Codec.string)
    val out                        = outlet("out", Contract("x.v1"), Codec.string)
    val label                      = stringParameter("label", default = "x")
    def logic(ctx: Context): Logic = from(in)(_.map(r => out(r)))

  private val relay = new Relay

  private val settings = Settings(
    pipeline = "p",
    streamlet = "relay",
    namespace = "n",
    local = false,
    parameters = Map("label" -> "y"),
    inlets = Vector(
      InletSettings("in", "p.a", "p.relay.in", "p.relay.in", Map("bootstrap.servers" -> "k:9092"))
    ),
    outlets = Vector(
      OutletSettings(
        "out",
        "p.b",
        "p.relay.out",
        Delivery.AtLeastOnce,
        None,
        Map("bootstrap.servers" -> "k:9092", "linger.ms" -> "5")
      )
    )
  )

  test("the consumer gets the file's properties, its client id, and only what the guarantee needs") {
    assertEquals(
      Runner.consumerProperties(settings.inlets.head),
      Map(
        "bootstrap.servers"  -> "k:9092",
        "client.id"          -> "p.relay.in",
        "enable.auto.commit" -> "false",
        "auto.offset.reset"  -> "earliest"
      )
    )
  }

  test("auto.offset.reset is the file's when it gives one; enable.auto.commit is never") {
    val i = settings.inlets.head
      .copy(kafka = Map("auto.offset.reset" -> "latest", "enable.auto.commit" -> "true"))
    val p = Runner.consumerProperties(i)
    assertEquals(p("auto.offset.reset"), "latest")
    assertEquals(p("enable.auto.commit"), "false")
  }

  test("the producer is idempotent and waits for every replica, over the file's properties") {
    assertEquals(
      Runner.producerProperties(settings.outlets.head),
      Map(
        "bootstrap.servers"  -> "k:9092",
        "linger.ms"          -> "5",
        "client.id"          -> "p.relay.out",
        "enable.idempotence" -> "true",
        "acks"               -> "all"
      )
    )
  }

  test("a file that gives every declared port and parameter is accepted") {
    assertEquals(Runner.check(relay, settings), Right(()))
  }

  test("a declared port or parameter the file does not give is refused naming its key") {
    assertEquals(
      Runner.check(relay, settings.copy(inlets = Vector.empty)),
      Left(Settings.missing("inlet.in.topic"))
    )
    assertEquals(
      Runner.check(relay, settings.copy(outlets = Vector.empty)),
      Left(Settings.missing("outlet.out.topic"))
    )
    assertEquals(
      Runner.check(relay, settings.copy(parameters = Map.empty)),
      Left(Settings.missing("parameter.label"))
    )
  }

  test("a port or parameter the streamlet does not declare is refused") {
    val Left(port) = Runner.check(
      relay,
      settings.copy(inlets = settings.inlets :+ settings.inlets.head.copy(port = "other"))
    ): @unchecked
    assert(port.contains("inlet 'other'"), port)
    val Left(param) =
      Runner.check(relay, settings.copy(parameters = Map("label" -> "y", "z" -> "1"))): @unchecked
    assert(param.contains("parameter 'z'"), param)
  }

  test("an exactly-once outlet is refused: a Pekko Streams streamlet delivers at least once") {
    val eo = settings.outlets.head.copy(
      delivery = Delivery.ExactlyOnce,
      transactionalIdPrefix = Some("t")
    )
    val Left(e) = Runner.check(relay, settings.copy(outlets = Vector(eo))): @unchecked
    assert(e.contains("exactly-once") && e.contains("'out'"), e)
  }

  test("the settings come from the directory FLOW_CONFIG_DIR names, and its absence is named") {
    val dir = Files.createTempDirectory("config")
    Files.write(dir.resolve(Settings.FileName), Settings.write(settings).getBytes(ISO_8859_1))
    assertEquals(
      PekkoStreamlets.settings(Map(Settings.DirectoryVariable -> dir.toString)),
      Right(settings)
    )
    val Left(e) = PekkoStreamlets.settings(Map.empty): @unchecked
    assert(e.contains("FLOW_CONFIG_DIR"), e)
  }

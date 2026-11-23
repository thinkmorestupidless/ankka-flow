package com.thinkmorestupidless.ankka.flow.descriptor

import java.nio.charset.StandardCharsets
import java.nio.file.Files

class SettingsSuite extends munit.FunSuite:

  /** The example of the settings contract (specs/009-engine-streamlets/contracts). */
  private val example =
    """pipeline=carts
      |streamlet=counter
      |namespace=shop
      |local=false
      |
      |parameter.window=1m
      |
      |inlet.in.topic=carts.clean-carts
      |inlet.in.group=carts.counter.in
      |inlet.in.client-id=carts.counter.in
      |inlet.in.kafka.bootstrap.servers=kafka.kafka.svc:9092
      |inlet.in.kafka.isolation.level=read_committed
      |
      |outlet.out.topic=carts.cart-counts
      |outlet.out.client-id=carts.counter.out
      |outlet.out.delivery=exactly-once
      |outlet.out.transactional-id-prefix=flow.shop.carts.counter.out.e1
      |outlet.out.kafka.bootstrap.servers=kafka.kafka.svc:9092
      |outlet.out.kafka.transaction.timeout.ms=900000
      |""".stripMargin

  private val parsed = Settings(
    pipeline = "carts",
    streamlet = "counter",
    namespace = "shop",
    local = false,
    parameters = Map("window" -> "1m"),
    inlets = Vector(
      InletSettings(
        "in",
        "carts.clean-carts",
        "carts.counter.in",
        "carts.counter.in",
        Map(
          "bootstrap.servers" -> "kafka.kafka.svc:9092",
          "isolation.level"   -> "read_committed"
        )
      )
    ),
    outlets = Vector(
      OutletSettings(
        "out",
        "carts.cart-counts",
        "carts.counter.out",
        Delivery.ExactlyOnce,
        Some("flow.shop.carts.counter.out.e1"),
        Map(
          "bootstrap.servers"      -> "kafka.kafka.svc:9092",
          "transaction.timeout.ms" -> "900000"
        )
      )
    )
  )

  test("the contract's example parses to its ports and parameters"):
    assertEquals(Settings.parse(example), Right(parsed))

  test("the contract's example is written back byte for byte"):
    assertEquals(Settings.write(parsed), example)

  test("ports, parameters and kafka properties are written sorted"):
    val shuffled = parsed.copy(
      parameters = Map("z" -> "1", "a" -> "2"),
      inlets = Vector(
        parsed.inlets.head.copy(port = "b"),
        parsed.inlets.head.copy(port = "a")
      )
    )
    val text = Settings.write(shuffled)
    assert(text.indexOf("parameter.a=") < text.indexOf("parameter.z="), text)
    assert(text.indexOf("inlet.a.topic") < text.indexOf("inlet.b.topic"), text)
    assert(
      text.indexOf("inlet.a.kafka.bootstrap") < text.indexOf("inlet.a.kafka.isolation"),
      text
    )
    assertEquals(
      Settings.parse(text).map(_.inlets.map(_.port)),
      Right(Vector("a", "b"))
    )

  test("read finds streamlet.properties in the directory"):
    val dir = Files.createTempDirectory("settings")
    Files.write(dir.resolve(Settings.FileName), example.getBytes(StandardCharsets.ISO_8859_1))
    assertEquals(Settings.read(dir), Right(parsed))

  test("read names the file when it is not there"):
    val dir     = Files.createTempDirectory("settings")
    val Left(e) = Settings.read(dir): @unchecked
    assert(e.contains("streamlet.properties"), e)

  test("a missing key fails naming it"):
    for key <- Vector(
        "pipeline",
        "streamlet",
        "namespace",
        "local",
        "inlet.in.group",
        "inlet.in.client-id",
        "outlet.out.client-id",
        "outlet.out.delivery",
        "outlet.out.transactional-id-prefix"
      )
    do
      val text = example.linesIterator.filterNot(_.startsWith(key + "=")).mkString("\n")
      assertEquals(Settings.parse(text), Left(Settings.missing(key)), key)

  test("a port's topic missing fails naming it"):
    val text = example.linesIterator.filterNot(_.startsWith("inlet.in.topic=")).mkString("\n")
    assertEquals(Settings.parse(text), Left(Settings.missing("inlet.in.topic")))

  test("at-least-once carries no transactional id prefix"):
    val alo = parsed.copy(outlets =
      parsed.outlets.map(_.copy(delivery = Delivery.AtLeastOnce, transactionalIdPrefix = None))
    )
    val text = Settings.write(alo)
    assert(!text.contains("transactional-id-prefix"), text)
    assertEquals(Settings.parse(text), Right(alo))

  test("an unknown delivery is refused naming it"):
    val Left(e) = Settings.parse(example.replace("=exactly-once", "=sometimes")): @unchecked
    assert(e.contains("outlet.out.delivery") && e.contains("sometimes"), e)

  test("a local that is neither true nor false is refused"):
    val Left(e) = Settings.parse(example.replace("local=false", "local=yes")): @unchecked
    assert(e.contains("local"), e)

  test("non-ASCII values survive, written as \\u escapes"):
    val s    = parsed.copy(parameters = Map("greeting" -> "café ✓ 日本"))
    val text = Settings.write(s)
    assert(text.contains("parameter.greeting=caf\\u00E9 \\u2713 \\u65E5\\u672C"), text)
    assert(text.forall(_ < 0x80), text)
    assertEquals(Settings.parse(text), Right(s))

  test("separators, backslashes, leading spaces and newlines survive"):
    val s = parsed.copy(parameters =
      Map("odd" -> " a=b:c #d !e \\f\ng\th", "empty" -> "", "url" -> "http://x:1/y?z=1")
    )
    val text = Settings.write(s)
    assert(text.contains("parameter.url=http://x:1/y?z=1"), text)
    assertEquals(Settings.parse(text), Right(s))
    val props = new java.util.Properties()
    props.load(new java.io.StringReader(text))
    assertEquals(props.getProperty("parameter.odd"), " a=b:c #d !e \\f\ng\th")

  test("a file of no ports holds only its names and parameters"):
    val s = parsed.copy(inlets = Vector.empty, outlets = Vector.empty)
    assertEquals(
      Settings.write(s),
      "pipeline=carts\nstreamlet=counter\nnamespace=shop\nlocal=false\n\nparameter.window=1m\n"
    )
    assertEquals(Settings.parse(Settings.write(s)), Right(s))

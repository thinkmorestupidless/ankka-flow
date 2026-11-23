package com.thinkmorestupidless.ankka.flow.cli

import java.nio.charset.StandardCharsets.ISO_8859_1
import java.nio.file.{Files, Path}

import com.thinkmorestupidless.ankka.flow.descriptor.{Delivery, Settings}

import CliFixtures.*

/** `flow local`: one `streamlet.properties` per streamlet, for a laptop's broker. */
class LocalSuite extends munit.FunSuite:

  private def local(dir: Path, out: Path, extra: String*) =
    flow(
      Seq(
        "local",
        dir.resolve("blueprint.conf").toString,
        "--descriptors",
        dir.resolve("descriptors").toString,
        "--bootstrap-servers",
        "localhost:9092",
        "-o",
        out.toString
      ) ++ extra*
    )

  private def read(p: Path) = new String(Files.readAllBytes(p), ISO_8859_1)

  test("writes each streamlet's settings, as a cluster would name them, on the laptop's broker") {
    val out = Files.createTempDirectory("local")
    val r   = local(cart, out)
    assertEquals(r.code, 0, r.err)
    for s <- Seq("cleaner", "counter") do
      val written = out.resolve(s"$s/streamlet.properties")
      assertEquals(read(written), read(cart.resolve(s"local/$s/streamlet.properties")), s)
      assert(r.out.contains(s"wrote $written"), r.out)
    assertEquals(r.out.linesIterator.count(_.startsWith("wrote ")), 2)
  }

  test("every file is local, and every outlet at least once") {
    val out = Files.createTempDirectory("local")
    assertEquals(local(cart, out).code, 0)
    for s <- Seq("cleaner", "counter") do
      val settings = Settings.read(out.resolve(s)).fold(e => fail(e), identity)
      assert(settings.local, s)
      assert(settings.outlets.forall(_.delivery == Delivery.AtLeastOnce), s)
      assert(settings.outlets.forall(_.transactionalIdPrefix.isEmpty), s)
  }

  test("deploy-time configuration sets the parameters it names") {
    val out = Files.createTempDirectory("local")
    val r   = local(cart, out, "--conf", cart.resolve("overrides.conf").toString)
    assertEquals(r.code, 0, r.err)
    assert(read(out.resolve("cleaner/streamlet.properties")).contains("parameter.max-total=250\n"))
  }

  test("a blueprint that does not verify writes nothing") {
    val dir =
      variant(blueprint = _.replace("consumers  = [counter.in]", "consumers  = [counter.nope]"))
    val out = Files.createTempDirectory("local")
    val r   = local(dir, out)
    assertEquals(r.code, 1)
    assert(r.err.contains("nope"), r.err)
    assertEquals(Files.list(out).count(), 0L)
  }

  test("an ankka topic without --ankka-bootstrap-servers is refused") {
    val dir = variant(blueprint =
      _.replace(
        "managed    = false\n      topic.name = \"shop.cart-events.v1\"",
        "ankka = \"shop/cart-events\""
      )
    )
    val r = local(dir, Files.createTempDirectory("local"))
    assertEquals(r.code, 1)
    assert(r.err.contains("--ankka-bootstrap-servers"), r.err)

    val out = Files.createTempDirectory("local")
    val ok  = local(dir, out, "--ankka-bootstrap-servers", "localhost:9192")
    assertEquals(ok.code, 0, ok.err)
    val cleaner = Files.readString(out.resolve("cleaner/streamlet.properties"))
    assert(cleaner.contains("inlet.in.topic=shop.cart-events"), cleaner)
    assert(cleaner.contains("inlet.in.kafka.bootstrap.servers=localhost:9192"), cleaner)
    assert(cleaner.contains("outlet.out.kafka.bootstrap.servers=localhost:9092"), cleaner)
  }

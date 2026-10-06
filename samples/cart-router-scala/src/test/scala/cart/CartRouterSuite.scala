package cart

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Paths}

import com.thinkmorestupidless.ankka.flow.protocol.{DescriptorJson, Json}
import com.thinkmorestupidless.ankka.flow.sdk.testkit.Harness

class CartRouterSuite extends munit.FunSuite:

  private def bytes(s: String)                = s.getBytes(UTF_8)
  private def event(cart: String, total: Int) = bytes(s"""{"cartId": "$cart", "total": $total}""")

  // docs:start routes-by-total
  test("routes by total") {
    val h = Harness(new CartRouter, Map("review-threshold" -> "50"))
    h.inlet("in")
      .put(
        event("cart-1", 10),
        key = Some(bytes("cart-1")),
        headers = Seq("ce_type" -> bytes("ItemAdded"))
      )
    h.inlet("in").put(event("cart-2", 99), key = Some(bytes("cart-2")))
    h.run()
    assertEquals(h.outlet("valid").records.flatMap(_.keyString), Vector("cart-1"))
    assertEquals(h.outlet("review").records.flatMap(_.keyString), Vector("cart-2"))
    assertEquals(
      h.outlet("valid").records.head.headers.map((k, v) => k -> new String(v, UTF_8)),
      Seq("ce_type" -> "ItemAdded")
    )
  }
  // docs:end routes-by-total

  // docs:start ordering
  test("each cart stays in order") {
    val h = Harness(new CartRouter)
    (0 until 50).foreach { i =>
      val cart = s"cart-${i % 10}"
      h.inlet("in").put(event(cart, i * 5), key = Some(bytes(cart)))
    }
    h.run(partitions = Harness.hashPartitioner(3), maxRecords = Some(7))
    assert(h.failures.isEmpty && h.skipped.isEmpty)
    val everything = h.outlet("valid").records ++ h.outlet("review").records
    assertEquals(everything.size, 50)
    everything.groupBy(_.keyString).values.foreach { ofOneCart =>
      val totals =
        ofOneCart.map(r => Json.parse(r.valueString).toOption.flatMap(_.field("total")).get)
      assertEquals(totals.distinct.size, totals.size)
    }
  }
  // docs:end ordering

  test("the committed descriptor declares the fixture's streamlet") {
    val root = Paths.get(sys.props.getOrElse("flow.repo.root", ".")).toAbsolutePath.normalize
    def read(rel: String) =
      DescriptorJson.read(new String(Files.readAllBytes(root.resolve(rel)), UTF_8)).toOption.get
    val committed = read("samples/cart-router-scala/flow/descriptor.json")
    val fixture   = read("protocol/fixtures/descriptors/cart-router.json")
    assertEquals(committed.streamlet, fixture.streamlet)
    assertEquals(committed.getSdk.name, "ankka-flow-scala")
  }

package cart.cleaner

import java.nio.charset.StandardCharsets.UTF_8

// docs:start test
import com.thinkmorestupidless.ankka.flow.pekko.{PekkoHarness, Record}

class CleanerSuite extends munit.FunSuite:

  private def event(json: String) = Record(Some("ignored"), json.getBytes(UTF_8))

  test("keeps cart events within the maximum, keyed by their cart, and drops the rest") {
    val result = PekkoHarness(Cleaner, "max-total" -> "250").run(
      Cleaner.in -> Seq(
        event("""{"cartId":"c1","seq":1,"total":100,"at":1000}"""),
        event("""{"cartId":"c2","seq":1,"total":900,"at":1001}"""), // above the maximum
        event("""not a cart event"""),                              // does not decode
        event("""{"cartId":"c1","seq":2,"total":250,"at":1002}""")
      )
    )
    assertEquals(
      result(Cleaner.out),
      Vector(
        Record(Some("c1"), CleanCart("c1", 1, 100, 1000)),
        Record(Some("c1"), CleanCart("c1", 2, 250, 1002))
      )
    )
    // Every record is committed, the dropped ones too.
    assertEquals(result.committed, Vector(0, 1, 2, 3))
  }
// docs:end test

  test("the maximum defaults to 10000") {
    val result = PekkoHarness(Cleaner).run(
      Cleaner.in -> Seq(
        event("""{"cartId":"c1","seq":1,"total":10000,"at":1}"""),
        event("""{"cartId":"c1","seq":2,"total":10001,"at":2}""")
      )
    )
    assertEquals(result(Cleaner.out).map(_.value.seq), Vector(1L))
  }

  test("an event missing a field required by its schema is dropped") {
    val result = PekkoHarness(Cleaner).run(Cleaner.in -> Seq(event("""{"cartId":"c1","seq":1}""")))
    assertEquals(result(Cleaner.out), Vector.empty)
  }

  test("the descriptor is the fixture's, written by this SDK") {
    val fixture = new String(
      java.nio.file.Files.readAllBytes(
        java.nio.file.Paths
          .get(sys.props.getOrElse("flow.repo.root", "."))
          .resolve("descriptor/fixtures/descriptors/cart-cleaner.json")
      ),
      UTF_8
    )
    import com.thinkmorestupidless.ankka.flow.descriptor.{DescriptorJson, SdkInfo}
    assertEquals(DescriptorJson.write(Cleaner.descriptor(SdkInfo("fixture", "0.0.0"))), fixture)
  }

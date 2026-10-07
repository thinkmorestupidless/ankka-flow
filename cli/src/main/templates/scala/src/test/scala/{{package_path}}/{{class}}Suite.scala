package {{package}}

import java.nio.charset.StandardCharsets.UTF_8

import com.thinkmorestupidless.ankka.flow.sdk.testkit.Harness

/** The streamlet through the SDK's harness: no Kafka, no sidecar. */
class {{class}}Suite extends munit.FunSuite:

  private def bytes(s: String) = s.getBytes(UTF_8)

  test("adds the greeting to each record and keeps its key and headers") {
    val h = Harness(new {{class}})
    h.inlet("in").put(bytes("""{"id": 1}"""), key = Some(bytes("k-1")), headers = Seq("ce-id" -> bytes("e-1")))
    h.run()
    val records = h.outlet("out").records
    assertEquals(records.map(_.valueString), Vector("""{"greeting":"hello, ankka-flow","id":1}"""))
    assertEquals(records.flatMap(_.keyString), Vector("k-1"))
    assertEquals(records.head.headers.map((k, v) => k -> new String(v, UTF_8)), Seq("ce-id" -> "e-1"))
  }

  test("the greeting is the parameter's deploy-time value") {
    val h = Harness(new {{class}}, Map("greeting" -> "hej"))
    h.inlet("in").put(bytes("""{"id": 1}"""))
    h.run()
    assertEquals(h.outlet("out").records.map(_.valueString), Vector("""{"greeting":"hej","id":1}"""))
  }

  test("a value that is not a JSON object fails the batch") {
    val h = Harness(new {{class}})
    h.inlet("in").put(bytes("not json"))
    h.run()
    assertEquals(h.failures.size, 1)
    assertEquals(h.outlet("out").records, Vector.empty)
  }

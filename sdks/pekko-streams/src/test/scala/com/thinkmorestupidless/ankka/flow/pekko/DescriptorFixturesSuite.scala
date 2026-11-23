package com.thinkmorestupidless.ankka.flow.pekko

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Paths}

import scala.concurrent.duration.*

import com.thinkmorestupidless.ankka.flow.descriptor.{DescriptorJson, Engine, SdkInfo}

/**
 * The descriptor fixtures of the `pekko-streams` engine, each declared in this SDK, write exactly
 * the bytes of descriptor/fixtures/descriptors/<name>.json; a declaration the descriptor refuses is
 * refused where it is written.
 */
class DescriptorFixturesSuite extends munit.FunSuite:

  private val root = Paths.get(sys.props.getOrElse("flow.repo.root", ".")).toAbsolutePath.normalize
  private val fixtures = root.resolve("descriptor/fixtures/descriptors")
  private val fixture  = SdkInfo("fixture", "0.0.0")

  FixtureStreamlets.all.foreach { (name, make) =>
    test(s"the descriptor declared in the Pekko Streams SDK equals the fixture's bytes: $name") {
      val expected = new String(Files.readAllBytes(fixtures.resolve(s"$name.json")), UTF_8)
      assertEquals(DescriptorJson.write(make().descriptor(fixture)), expected)
    }
  }

  test("every pekko-streams fixture is covered, and only those") {
    val pekko = Files
      .list(fixtures)
      .toArray
      .map(_.toString)
      .filter(_.endsWith(".json"))
      .filter(f =>
        new String(Files.readAllBytes(Paths.get(f)), UTF_8).contains("\"pekko-streams\"")
      )
      .map(f => Paths.get(f).getFileName.toString.stripSuffix(".json"))
      .toSet
    assertEquals(pekko, FixtureStreamlets.all.map(_._1).toSet)
  }

  test("a descriptor names the engine its streamlet is written for, and the SDK that wrote it") {
    val d = new FixtureStreamlets.Minimal().descriptor()
    assertEquals(d.streamlet.engine, Engine.PekkoStreams)
    assertEquals(d.streamlet.flink, None)
    assertEquals(d.sdk.map(_.name), Some("ankka-flow-pekko-streams"))
    assert(DescriptorJson.write(d).contains("\"engine\": \"pekko-streams\""))
  }

  private def refused(problem: String)(declare: => PekkoStreamlet): Unit =
    val e = intercept[IllegalArgumentException](declare.descriptor())
    assert(e.getMessage.contains(problem), s"'${e.getMessage}' does not mention '$problem'")

  private abstract class Bad(name: String = "bad") extends FixtureStreamlets.Declared(name)

  test("two ports of one name are refused before a descriptor is written") {
    refused("port 'in' is declared 2 times")(new Bad:
      inlet("in", Contract("x.v1"), Codec.bytes)
      outlet("in", Contract("x.v1"), Codec.bytes))
  }

  test("a port name with a capital letter is refused") {
    refused("outlet name 'Out' must match")(
      new Bad:
        outlet("Out", Contract("x.v1"), Codec.bytes)
    )
  }

  test("a contract name ankka would refuse is refused") {
    refused("contract name 'X v1' must match")(
      new Bad:
        inlet("in", Contract("X v1"), Codec.bytes)
    )
  }

  test("a schema the classpath does not hold is refused naming it") {
    refused("nowhere.json")(
      new Bad:
        inlet("in", Contract("x.v1", schema = "nowhere.json"), Codec.bytes)
    )
  }

  test("a streamlet name the descriptor refuses is refused") {
    refused("streamlet name '-bad'")(new Bad("-bad") {})
  }

  test("a parameter key declared twice is refused") {
    refused("parameter 'n' is declared 2 times")(new Bad:
      stringParameter("n")
      stringParameter("n"))
  }

  test("a default of the wrong form is refused where it is declared") {
    intercept[IllegalArgumentException](
      new Bad:
        durationParameter("d", default = "soon")
    )
  }

  test("every parameter type is written with its default as text") {
    class EveryType extends Bad:
      val a = stringParameter("a-string", default = "hello")
      val b = intParameter("an-integer", default = 42)
      val c = doubleParameter("a-double", default = 0.5)
      val d = booleanParameter("a-boolean", default = true)
      val e = durationParameter("a-duration", default = "100 ms")
      val f = memorySizeParameter("a-memory-size", default = "1 MiB")
      val g = stringParameter("required")
    val s  = new EveryType
    val ps = s.descriptor().streamlet.parameters.map(p => p.key -> p.default).toMap
    assertEquals(
      ps,
      Map(
        "a-string"      -> Some("hello"),
        "an-integer"    -> Some("42"),
        "a-double"      -> Some("0.5"),
        "a-boolean"     -> Some("true"),
        "a-duration"    -> Some("100 ms"),
        "a-memory-size" -> Some("1 MiB"),
        "required"      -> None
      )
    )
    assertEquals(s.e.parse("100 ms"), Right(100.millis))
    assertEquals(s.f.parse("1 MiB"), Right(1024L * 1024))
  }

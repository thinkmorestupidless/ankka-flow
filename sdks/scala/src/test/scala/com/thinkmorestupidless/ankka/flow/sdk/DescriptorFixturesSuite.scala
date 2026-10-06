package com.thinkmorestupidless.ankka.flow.sdk

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Paths}

import ankka.flow.v1.discovery.SdkInfo

/**
 * The descriptor fixtures: each of the six declared streamlets, declared in the Scala SDK, writes
 * exactly the bytes of protocol/fixtures/descriptors/<name>.json. And a declaration the protocol
 * refuses is refused where it is declared.
 */
class DescriptorFixturesSuite extends munit.FunSuite:

  private val root = Paths.get(sys.props.getOrElse("flow.repo.root", ".")).toAbsolutePath.normalize
  private val fixtures = root.resolve("protocol/fixtures/descriptors")
  private val fixture  = SdkInfo("fixture", "0.0.0")

  FixtureStreamlets.all.foreach { (name, make) =>
    test(s"the descriptor written in Scala equals the fixture's bytes: $name") {
      val expected = new String(Files.readAllBytes(fixtures.resolve(s"$name.json")), UTF_8)
      assertEquals(Descriptor.write(make(), fixture), expected)
    }
  }

  test("every fixture is covered") {
    val files = Files.list(fixtures).toArray.map(_.toString).filter(_.endsWith(".json"))
    assertEquals(files.length, FixtureStreamlets.all.size)
  }

  private def refused(problem: String)(declare: => Streamlet): Unit =
    val e = intercept[IllegalArgumentException](declare)
    assert(e.getMessage.contains(problem), s"'${e.getMessage}' does not mention '$problem'")

  private abstract class Bad(name: String = "bad") extends FixtureStreamlets.NoOp(name)

  test("a declaration with two ports of one name is refused before a descriptor is written") {
    refused("port 'in' is declared more than once")(new Bad:
      inlet("in", schemaName = "x.v1")
      outlet("in", schemaName = "x.v1"))
  }

  test("a declaration with an inlet with an empty schema name is refused") {
    refused("inlet 'in' has no schema name")(
      new Bad:
        inlet("in", schemaName = "")
    )
  }

  test("a declaration with a parameter with a default of another type is refused") {
    refused("parameter 'n': 'many' is not a integer")(
      new Bad:
        parameter.integer("n", default = "many")
    )
  }

  test("a declaration with a port name with a capital letter is refused") {
    refused("outlet name 'Out' must match")(
      new Bad:
        outlet("Out", schemaName = "x.v1")
    )
  }

  test("a streamlet name the protocol refuses is refused") {
    refused("streamlet name '-bad'")(new Bad("-bad") {})
  }

  test("a parameter key declared twice is refused") {
    refused("parameter 'n' is declared more than once")(new Bad:
      parameter.string("n")
      parameter.string("n"))
  }

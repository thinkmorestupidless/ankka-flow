package com.thinkmorestupidless.ankka.flow.protocol

import java.nio.file.Files

/**
 * The built-in descriptors, as canonical JSON committed under `protocol/fixtures/builtin/`. A byte
 * difference fails this suite; `-Dflow.fixtures.regenerate=on` rewrites the files from `Builtins`,
 * as the SDK fixtures are rewritten.
 */
class BuiltinsSuite extends munit.FunSuite:

  private val regenerate = sys.props.get("flow.fixtures.regenerate").contains("on")

  Builtins.all.foreach { spec =>
    val name = spec.getStreamlet.name
    test(s"built-in $name") {
      val path    = Fixtures.dir.resolve(s"builtin/$name.json")
      val written = DescriptorJson.write(spec)
      if regenerate then
        Files.createDirectories(path.getParent)
        Files.write(path, written.getBytes("UTF-8")): Unit
      else
        assert(Files.exists(path), s"$path is missing; run with -Dflow.fixtures.regenerate=on")
        assertEquals(written, Fixtures.read(path))
      assertEquals(DescriptorValidation.validate(spec), Vector.empty)
    }
  }

  test("a built-in is found by its bare name, never by the prefixed one") {
    assertEquals(
      Builtins.byName("neo4j-merge-sink").map(_.getStreamlet.name),
      Some("neo4j-merge-sink")
    )
    assertEquals(Builtins.byName("builtin/neo4j-merge-sink"), None)
  }

  test("the merge sink reads the graph delta contract and writes nowhere") {
    val d = Builtins.neo4jMergeSink.getStreamlet
    assertEquals(
      d.inlets.map(p => p.name -> p.getContract.schemaName),
      Seq("in" -> "ankka.graph-delta.v1")
    )
    assertEquals(d.outlets, Seq.empty)
    assertEquals(d.configParameters.map(_.key).toSet, Set("secret", "transaction-timeout"))
  }

package com.thinkmorestupidless.ankka.flow.protocol

import java.nio.file.Files

/**
 * The descriptor fixtures every SDK must reproduce byte for byte (FR-002). With
 * `-Dflow.fixtures.regenerate=on` this suite rewrites them from the Scala declarations; otherwise a
 * byte difference fails it.
 */
class DescriptorFixturesSuite extends munit.FunSuite:

  private val regenerate = sys.props.get("flow.fixtures.regenerate").contains("on")

  Fixtures.all.foreach { (name, spec) =>
    test(s"fixture $name") {
      val path    = Fixtures.dir.resolve(s"descriptors/$name.json")
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

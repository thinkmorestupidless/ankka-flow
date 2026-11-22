package com.thinkmorestupidless.ankka.flow.descriptor

import java.nio.file.Files

/**
 * Each fixture declared with the model is written as the fixture's bytes, and each fixture reads
 * back and is written unchanged. `-Dflow.fixtures.regenerate=on` rewrites the files instead, for a
 * deliberate change to the canonical form; the SDKs' suites then hold them to it.
 */
class DescriptorFixturesSuite extends munit.FunSuite:

  private val regenerate = sys.props.get("flow.fixtures.regenerate").contains("on")
  private val dir        = Fixtures.dir.resolve("descriptors")

  test("the six fixtures, four for Pekko Streams and two for Flink"):
    assertEquals(Fixtures.all.size, 6)
    assertEquals(Fixtures.all.count(_._2.streamlet.engine == Engine.PekkoStreams), 4)
    assertEquals(Fixtures.all.map(_._1).toSet.size, 6)
    val declared =
      Files
        .list(Fixtures.dir.resolve("declarations"))
        .toArray
        .map(_.toString)
        .filter(_.endsWith(".md"))
    assertEquals(declared.length, 7) // the six and the README

  Fixtures.all.foreach { (name, d) =>
    test(s"$name is written as its fixture's bytes"):
      val file = dir.resolve(s"$name.json")
      val text = DescriptorJson.write(d)
      if regenerate then Files.writeString(file, text): Unit
      assertEquals(text, Fixtures.read(file))
      assertEquals(DescriptorValidation.check(text).map(DescriptorJson.write), Right(text))
  }

  test("every fixture file is one of the six"):
    val files = Files.list(dir).toArray.map(_.toString).filter(_.endsWith(".json")).toSet
    assertEquals(files, Fixtures.all.map(n => dir.resolve(s"${n._1}.json").toString).toSet)

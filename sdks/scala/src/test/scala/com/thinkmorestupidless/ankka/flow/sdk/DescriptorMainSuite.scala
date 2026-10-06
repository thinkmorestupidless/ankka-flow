package com.thinkmorestupidless.ankka.flow.sdk

import java.io.{ByteArrayOutputStream, PrintStream}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files

/** The descriptor command: writes the file, checks it, and names what it cannot construct. */
class DescriptorMainSuite extends munit.FunSuite:

  private val router = classOf[FixtureStreamlets.CartRouter].getName

  private def run(args: String*): (Int, String) =
    val buf  = new ByteArrayOutputStream
    val ps   = new PrintStream(buf, true, UTF_8)
    val code = Descriptor.run(args.toList, ps, ps)
    (code, buf.toString(UTF_8))

  private val dir = FunFixture[java.nio.file.Path](
    _ => Files.createTempDirectory("descriptor"),
    d => d.toFile.listFiles.foreach(_.delete())
  )

  dir.test("the command writes the descriptor the declaration writes") { d =>
    val path = d.resolve("flow/descriptor.json")
    assertEquals(run(router, path.toString)._1, 0)
    assertEquals(
      new String(Files.readAllBytes(path), UTF_8),
      Descriptor.write(new FixtureStreamlets.CartRouter)
    )
  }

  dir.test("--check passes on the written file and fails on a changed one, naming it") { d =>
    val path = d.resolve("descriptor.json")
    run(router, path.toString)
    assertEquals(run(router, path.toString, "--check")._1, 0)
    Files.write(path, "{}\n".getBytes(UTF_8))
    val (code, out) = run(router, path.toString, "--check")
    assertEquals(code, 1)
    assert(out.contains(path.toString), out)
  }

  dir.test("--check fails when there is no file") { d =>
    assertEquals(run(router, d.resolve("missing.json").toString, "--check")._1, 1)
  }

  test("a class that is not a streamlet is named, with exit 2") {
    val (code, out) = run("java.lang.String", "x.json")
    assertEquals(code, 2)
    assert(out.contains("java.lang.String"), out)
  }

  test("a streamlet that refuses its own declaration is named, with exit 2") {
    val (code, out) = run(classOf[DescriptorMainSuite.Refused].getName, "x.json")
    assertEquals(code, 2)
    assert(out.contains("declared more than once"), out)
  }

  test("no arguments prints the usage, with exit 2") {
    assertEquals(run()._1, 2)
  }

object DescriptorMainSuite:
  final class Refused extends FixtureStreamlets.NoOp("refused"):
    val a = inlet("in", schemaName = "x.v1")
    val b = inlet("in", schemaName = "x.v1")

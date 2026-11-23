package com.thinkmorestupidless.ankka.flow.pekko

import java.io.{ByteArrayOutputStream, PrintStream}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path}

import com.thinkmorestupidless.ankka.flow.descriptor.DescriptorJson

/**
 * The main's descriptor modes: `--descriptor <path>` writes, `--descriptor-check <path>` compares.
 */
class DescriptorMainSuite extends munit.FunSuite:

  private def run(streamlet: PekkoStreamlet, args: String*): (Option[Int], String) =
    val buf  = new ByteArrayOutputStream
    val ps   = new PrintStream(buf, true, UTF_8)
    val code = DescriptorFiles.handle(streamlet, args.toList, ps, ps)
    (code, buf.toString(UTF_8))

  private val dir = FunFixture[Path](
    _ => Files.createTempDirectory("descriptor"),
    d => d.toFile.listFiles.foreach(_.delete())
  )

  private def cleaner = new FixtureStreamlets.CartCleaner

  dir.test("--descriptor writes the descriptor the declaration writes") { d =>
    val path = d.resolve("flow/cleaner.json")
    assertEquals(run(cleaner, "--descriptor", path.toString)._1, Some(0))
    assertEquals(
      new String(Files.readAllBytes(path), UTF_8),
      DescriptorJson.write(cleaner.descriptor())
    )
  }

  dir.test("--descriptor-check passes on the written file and fails on a changed one, naming it") {
    d =>
      val path = d.resolve("cleaner.json")
      run(cleaner, "--descriptor", path.toString)
      assertEquals(run(cleaner, "--descriptor-check", path.toString)._1, Some(0))
      Files.write(path, "{}\n".getBytes(UTF_8))
      val (code, out) = run(cleaner, "--descriptor-check", path.toString)
      assertEquals(code, Some(1))
      assert(out.contains(path.toString), out)
  }

  dir.test("--descriptor-check fails when there is no file") { d =>
    assertEquals(run(cleaner, "--descriptor-check", d.resolve("missing.json").toString)._1, Some(1))
  }

  test("a streamlet that refuses its own declaration is named, with exit 2") {
    val (code, out) = run(new DescriptorMainSuite.Refused, "--descriptor", "x.json")
    assertEquals(code, Some(2))
    assert(out.contains("declared 2 times"), out)
  }

  test("a descriptor mode without its path is a usage error, exit 2") {
    assertEquals(run(cleaner, "--descriptor")._1, Some(2))
    assertEquals(run(cleaner, "--frobnicate")._1, Some(2))
  }

  test("no arguments is not a descriptor mode: the main runs the streamlet") {
    assertEquals(run(cleaner)._1, None)
  }

object DescriptorMainSuite:
  final class Refused extends FixtureStreamlets.Declared("refused"):
    val a = inlet("in", Contract("x.v1"), Codec.bytes)
    val b = inlet("in", Contract("x.v1"), Codec.bytes)

package com.thinkmorestupidless.ankka.flow.cli

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path}
import java.util.concurrent.TimeUnit

import scala.concurrent.duration.*

/**
 * `flow init`'s projects, built: each language's project rendered through the CLI, pointed at this
 * repository's SDK, and held to its own tests, its descriptor check and `flow verify`. A template
 * that renders but does not build is the failure this suite exists for.
 *
 * `-Dflow.template.tests=scala,python` (the default), a comma list, or `off`. The Scala project
 * resolves the SDK this build published locally (`sdk/publishLocal` and `protocol/publishLocal` run
 * before this suite when Scala is on); the Python project gets a path source for `sdks/python`.
 */
class TemplateSuite extends munit.FunSuite:

  override val munitTimeout: Duration = 20.minutes

  private val enabled: Set[String] =
    sys.props.get("flow.template.tests").map(_.trim) match
      case Some("off") => Set.empty
      case Some(list)  => list.split(',').map(_.trim).filter(_.nonEmpty).toSet
      case None        => Set("scala", "python")

  /** Runs `cmd` in `dir`, failing the test with its output when it does not exit 0. */
  private def sh(dir: Path, cmd: String*): String =
    val log = Files.createTempFile("template", ".log")
    val p = new ProcessBuilder(cmd*)
      .directory(dir.toFile)
      .redirectErrorStream(true)
      .redirectOutput(log.toFile)
      .start()
    if !p.waitFor(15, TimeUnit.MINUTES) then
      p.destroyForcibly()
      fail(s"${cmd.mkString(" ")} did not finish in 15 minutes")
    val out = Files.readString(log, UTF_8)
    if p.exitValue != 0 then
      fail(s"${cmd.mkString(" ")} in $dir exited ${p.exitValue}:\n${out.takeRight(6000)}")
    out

  private def init(language: Init.Language): Path =
    val dir = Files.createTempDirectory(s"init-${language.id}").resolve("order-greeter")
    val r =
      CliFixtures.flow("init", "order-greeter", "--language", language.id, "--dir", dir.toString)
    assertEquals(r.code, 0, r.err)
    dir

  private def verified(dir: Path): Unit =
    val r = CliFixtures.flow(
      "verify",
      dir.resolve("blueprint.conf").toString,
      "--descriptors",
      dir.resolve("flow").toString
    )
    assertEquals(r.code, 0, r.err)
    assert(r.out.contains("verified: 1 streamlets, 2 topics"), r.out)

  test(
    "a Scala project from flow init passes its tests and its descriptor check, and its blueprint verifies"
  ) {
    assume(enabled("scala"), "-Dflow.template.tests leaves Scala out")
    val dir = init(Init.Language.Scala)
    sh(dir, "sbt", "-batch", "test", "descriptorCheck")
    verified(dir)
  }

  test(
    "a Python project from flow init passes its tests and its descriptor check, and its blueprint verifies"
  ) {
    assume(enabled("python"), "-Dflow.template.tests leaves Python out")
    val dir = init(Init.Language.Python)
    val sdk = CliFixtures.repoRoot.resolve("sdks/python")
    Files.writeString(
      dir.resolve("pyproject.toml"),
      Files.readString(dir.resolve("pyproject.toml")) +
        s"""\n[tool.uv.sources]\nankka-flow = { path = "$sdk", editable = true }\n"""
    )
    sh(dir, "uv", "sync", "-q")
    sh(dir, "uv", "run", "pytest", "-q")
    sh(dir, "uv", "run", "descriptor", "--check")
    verified(dir)
  }

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

  /** The generated workflow: actionlint when it is installed, else the steps it must have. */
  private def workflowChecked(dir: Path, steps: String*): Unit =
    val wf = dir.resolve(".github/workflows/ci.yml")
    val onPath = sys.env
      .getOrElse("PATH", "")
      .split(java.io.File.pathSeparator)
      .exists(d => Files.isExecutable(Path.of(d, "actionlint")))
    if onPath then sh(dir, "actionlint", wf.toString): Unit
    val text = Files.readString(wf)
    steps.foreach(step => assert(text.contains(step), s"the workflow does not run '$step'"))

  /** The image's exposed ports: none, because the sidecar dials the process on loopback. */
  private def exposesNoPort(image: String): Unit =
    val ports = sh(
      Path.of("."),
      "docker",
      "image",
      "inspect",
      "--format",
      "{{json .Config.ExposedPorts}}",
      image
    ).trim
    assert(ports == "null" || ports == "{}", s"$image exposes $ports")

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
    sh(dir, "sbt", "-batch", "test", "descriptorCheck", "Docker/publishLocal"): Unit
    verified(dir)
    workflowChecked(dir, "sbt test descriptorCheck")
    exposesNoPort("order-greeter:0.1.0")
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
    sh(dir, "uv", "sync", "-q"): Unit
    sh(dir, "uv", "run", "pytest", "-q"): Unit
    sh(dir, "uv", "run", "descriptor", "--check"): Unit
    verified(dir)
    workflowChecked(dir, "uv run pytest -q", "uv run descriptor --check")
  }

  /**
   * The Python image installs the SDK from PyPI, which holds released versions only; a project
   * rendered at the latest release builds as a reader's does, with its Dockerfile unchanged.
   */
  test("a Python project's image builds from its own Dockerfile, at a released SDK") {
    assume(enabled("python"), "-Dflow.template.tests leaves Python out")
    val dir     = Files.createTempDirectory("init-python-image").resolve("order-greeter")
    val request = Init.Request("order-greeter", Init.Language.Python, dir, None)
    Scaffold.write(
      dir,
      Scaffold.files(Init.Language.Python, Init.tokens(request, TemplateSuite.ReleasedSdk))
    ) match
      case Left(problems) => fail(problems.mkString("\n"))
      case Right(_)       => ()
    sh(dir, "docker", "build", "-q", "-t", "order-greeter-python:test", "."): Unit
    exposesNoPort("order-greeter-python:test")
  }

object TemplateSuite:
  /** The latest SDK release on PyPI and Maven Central, for the image a reader's project builds. */
  val ReleasedSdk = "0.4.1"

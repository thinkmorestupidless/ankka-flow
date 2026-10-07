package com.thinkmorestupidless.ankka.flow.cli

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path}

import scala.jdk.CollectionConverters.*

/** `flow init`: the tokens, the rendering, the index, and every refusal leaving nothing written. */
class InitSuite extends munit.FunSuite:

  private val tmp = FunFixture[Path](_ => Files.createTempDirectory("init"), _ => ())

  private def files(dir: Path): Set[String] =
    Files
      .walk(dir)
      .iterator
      .asScala
      .filter(Files.isRegularFile(_))
      .map(dir.relativize(_).toString)
      .toSet

  test("the tokens of a name") {
    val t =
      Init.tokens(Init.Request("order-greeter", Init.Language.Scala, Path.of("x"), None), "0.5.0")
    assertEquals(t("name"), "order-greeter")
    assertEquals(t("class"), "OrderGreeter")
    assertEquals(t("package"), "ordergreeter")
    assertEquals(t("package_path"), "ordergreeter")
    assertEquals(t("module"), "order_greeter")
    assertEquals(t("flow_version"), "0.5.0")
    assertEquals(t("sdk_version"), "0.5.0")
    assertEquals(t("protocol_version"), "1.0")
    assertEquals(t("sbt_version"), BuildInfo.sbtVersion)
    assertEquals(t("native_packager_version"), BuildInfo.nativePackagerVersion)
  }

  test("a package names the Scala package and its path, or the Python module") {
    val s = Init.tokens(
      Init.Request("greeter", Init.Language.Scala, Path.of("x"), Some("com.acme.flow")),
      "0.5.0"
    )
    assertEquals((s("package"), s("package_path")), ("com.acme.flow", "com/acme/flow"))
    val p = Init.tokens(
      Init.Request("greeter", Init.Language.Python, Path.of("x"), Some("acme_flow")),
      "0.5.0"
    )
    assertEquals(p("module"), "acme_flow")
  }

  test("the SDK version is the release's, or 0.0.0 for any other build") {
    def sdk(v: String) =
      Init.tokens(Init.Request("g", Init.Language.Scala, Path.of("x"), None), v)("sdk_version")
    assertEquals(sdk("0.5.0"), "0.5.0")
    assertEquals(sdk("0.4.1+7-5b9fed1e-SNAPSHOT"), "0.0.0")
    assertEquals(sdk("0.4.1+7-5b9fed1e+20261007-0958-SNAPSHOT"), "0.0.0")
  }

  test("rendering replaces every token in paths and contents and leaves GitHub's own expressions") {
    val tokens = Map("name" -> "g", "class" -> "G", "package_path" -> "a/b")
    assertEquals(Scaffold.render("src/{{package_path}}/{{class}}.scala", tokens), "src/a/b/G.scala")
    assertEquals(Scaffold.render("x: ${{ github.sha }} {{name}}", tokens), "x: ${{ github.sha }} g")
    assertEquals(Scaffold.leftover("${{ github.sha }} and {{name}}"), Vector("{{name}}"))
  }

  Init.Language.values.foreach { language =>
    tmp.test(
      s"a ${language.id} project holds exactly its index, with no token left and the plugin's skills"
    ) { dir =>
      val target = dir.resolve("greeter")
      val r =
        CliFixtures.flow("init", "greeter", "--language", language.id, "--dir", target.toString)
      assertEquals(r.code, 0, r.err)
      assertEquals(
        files(target),
        Scaffold
          .index(language)
          .map(
            Scaffold.render(
              _,
              Init.tokens(Init.Request("greeter", language, target, None), BuildInfo.version)
            )
          )
          .toSet
      )
      files(target).filterNot(Scaffold.verbatim).foreach { f =>
        val text = Files.readString(target.resolve(f), UTF_8)
        assertEquals(Scaffold.leftover(text), Vector.empty, s"$f")
      }
      val skill = ".claude/skills/ankka-flow/SKILL.md"
      val plugin =
        CliFixtures.repoRoot.resolve("marketplace/plugins/ankka-flow/skills/ankka-flow/SKILL.md")
      assertEquals(Files.readString(target.resolve(skill)), Files.readString(plugin))
    }
  }

  private def refused(args: String*)(problem: String)(using dir: Path): Unit =
    val target = dir.resolve("out")
    val r      = CliFixtures.flow(("init" +: args :+ "--dir" :+ target.toString)*)
    assertEquals(r.code, 2, r.out)
    assert(r.err.contains(problem), s"'${r.err}' does not say '$problem'")
    assert(!Files.exists(target), "something was written")

  Seq(
    ("a name with a capital letter", Seq("Greeter"), "must be 1-63 of [a-z0-9-]"),
    ("a name longer than sixty-three characters", Seq("g" * 64), "must be 1-63 of [a-z0-9-]"),
    ("a name longer than a pipeline id", Seq("g" * 41), "must be at most 40 characters"),
    ("a name ending with a hyphen", Seq("greeter-"), "must be 1-63 of [a-z0-9-]"),
    ("an empty name", Seq(""), "must be 1-63 of [a-z0-9-]"),
    ("a name starting with a digit", Seq("1greeter"), "must start with a letter"),
    (
      "a Scala package that is not one",
      Seq("greeter", "--package", "Com.Acme"),
      "is not a Scala package"
    ),
    (
      "a Python module that is not one",
      Seq("greeter", "-l", "python", "--package", "acme.flow"),
      "is not a Python module"
    ),
    (
      "a Python module that is a keyword",
      Seq("greeter", "-l", "python", "--package", "class"),
      "is not a Python module"
    )
  ).foreach { (what, args, problem) =>
    tmp.test(s"flow init refuses $what") { dir =>
      given Path = dir
      refused(args*)(problem)
    }
  }

  tmp.test(
    "flow init refuses a directory to write into that already holds files, and touches nothing"
  ) { dir =>
    val target = Files.createDirectories(dir.resolve("out"))
    Files.writeString(target.resolve("keep.txt"), "mine")
    val r = CliFixtures.flow("init", "greeter", "--dir", target.toString)
    assertEquals(r.code, 2)
    assert(r.err.contains("is not empty"), r.err)
    assertEquals(files(target), Set("keep.txt"))
  }

  tmp.test("an existing empty directory is used") { dir =>
    val target = Files.createDirectories(dir.resolve("out"))
    assertEquals(CliFixtures.flow("init", "greeter", "--dir", target.toString).code, 0)
    assert(Files.exists(target.resolve("blueprint.conf")))
  }

  tmp.test("a language that does not exist is refused as usage") { dir =>
    val r = CliFixtures.flow("init", "greeter", "-l", "cobol", "--dir", dir.resolve("out").toString)
    assertEquals(r.code, 2)
    assert(r.err.contains("scala") && r.err.contains("python"), r.err)
  }

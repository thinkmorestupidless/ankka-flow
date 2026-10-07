package com.thinkmorestupidless.ankka.flow.cli

import java.nio.file.Files

import com.thinkmorestupidless.ankka.flow.cli.mcp.{NamedCluster, ProjectFile}

class ProjectFileSuite extends munit.FunSuite:

  test(
    "context and namespace are read; comments, blanks, a table header and other keys are ignored"
  ) {
    val dir = Files.createTempDirectory("pf")
    Files.writeString(
      dir.resolve("flow.toml"),
      "# the cluster\n\n[cluster]\ncontext = \"kind-ankka\"  # local\nnamespace=\"greeter\"\nother = \"x\"\n"
    )
    assertEquals(ProjectFile.read(dir), Some(NamedCluster("kind-ankka", "greeter")))
  }

  test("a missing file, or a missing or empty key, names no cluster") {
    val dir = Files.createTempDirectory("pf")
    assertEquals(ProjectFile.read(dir), None)
    Files.writeString(dir.resolve("flow.toml"), "context = \"kind-ankka\"\n")
    assertEquals(ProjectFile.read(dir), None)
    Files.writeString(dir.resolve("flow.toml"), "context = \"\"\nnamespace = \"x\"\n")
    assertEquals(ProjectFile.read(dir), None)
  }

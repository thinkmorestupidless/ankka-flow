package com.thinkmorestupidless.ankka.flow.sidecar

import java.nio.file.{Files, Path}

class Neo4jSecretSuite extends munit.FunSuite:

  private def dir(files: (String, String)*): Path =
    val d = Files.createTempDirectory("neo4j-secret")
    files.foreach((k, v) => Files.writeString(d.resolve(k), v))
    d

  test("the four files, trimmed, with the database defaulted") {
    assertEquals(
      Neo4jSecret.read(
        dir("uri" -> "bolt://neo4j:7687\n", "username" -> "neo4j", "password" -> "s3cret\n")
      ),
      Right(Neo4jSecret("bolt://neo4j:7687", "neo4j", "s3cret", "neo4j"))
    )
    assertEquals(
      Neo4jSecret
        .read(
          dir("uri" -> "bolt://n:7687", "username" -> "u", "password" -> "p", "database" -> "graph")
        )
        .map(_.database),
      Right("graph")
    )
  }

  test("a missing or empty required file is named") {
    val d = dir("uri" -> "bolt://n:7687", "username" -> "u", "password" -> "  ")
    assertEquals(Neo4jSecret.read(d), Left(s"credentials directory $d has no 'password'"))
  }

  test("redaction removes the password and URI userinfo, and leaves other text alone") {
    val s = Neo4jSecret("bolt://n:7687", "neo4j", "s3cret", "neo4j")
    assertEquals(
      s.redact("auth failed for s3cret at bolt://neo4j:s3cret@n:7687/db"),
      "auth failed for <redacted> at bolt://<redacted>@n:7687/db"
    )
    assertEquals(s.redact("connection refused"), "connection refused")
    assert(!s.toString.contains("s3cret"))
  }

  test("a file that is there but cannot be read says so, rather than that it is missing") {
    val d = dir("uri" -> "bolt://n:7687", "username" -> "u", "password" -> "p")
    val p = d.resolve("password")
    Files.setPosixFilePermissions(p, java.util.Set.of())
    try
      val result = Neo4jSecret.read(d)
      assert(result.left.exists(_.startsWith("cannot read 'password'")), result.toString)
    finally
      Files.setPosixFilePermissions(
        p,
        java.nio.file.attribute.PosixFilePermissions.fromString("rw-------")
      ): Unit
  }

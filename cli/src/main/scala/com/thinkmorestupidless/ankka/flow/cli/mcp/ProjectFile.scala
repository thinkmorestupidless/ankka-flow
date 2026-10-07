package com.thinkmorestupidless.ankka.flow.cli.mcp

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path}

/** The cluster a project's tools may touch, from `flow.toml` beside the project. */
final case class NamedCluster(context: String, namespace: String)

/**
 * `flow.toml`: two keys, `context` and `namespace`, as `key = "value"` lines; `#` comments, blank
 * lines and a `[cluster]` table header are ignored, as is any other key. Written by `flow init`,
 * read by `flow mcp`, never written by it.
 */
private[cli] object ProjectFile:

  val FileName = "flow.toml"

  val HowToName: String =
    s"""no cluster named: write the cluster the tools may touch in $FileName beside the project:
       |  context   = "<a kubectl context, such as kind-ankka>"
       |  namespace = "<the pipeline's namespace>"
       |Nothing else is ever used, whatever kubectl points at.""".stripMargin

  private val Line = """\s*([A-Za-z_][A-Za-z0-9_-]*)\s*=\s*"([^"]*)"\s*(#.*)?""".r

  def parse(text: String): Map[String, String] =
    text.linesIterator.collect { case Line(key, value, _) => key -> value }.toMap

  /** The named cluster, when the file is there and names both. */
  def read(dir: Path): Option[NamedCluster] =
    val file = dir.resolve(FileName)
    if !Files.isRegularFile(file) then None
    else
      val values = parse(new String(Files.readAllBytes(file), UTF_8))
      for
        context   <- values.get("context").map(_.trim).filter(_.nonEmpty)
        namespace <- values.get("namespace").map(_.trim).filter(_.nonEmpty)
      yield NamedCluster(context, namespace)

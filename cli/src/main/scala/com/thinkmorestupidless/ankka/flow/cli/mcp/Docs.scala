package com.thinkmorestupidless.ankka.flow.cli.mcp

import java.nio.charset.StandardCharsets.UTF_8

/**
 * The documentation of the ankka-flow version this CLI was built from, carried inside it under
 * `ankka-flow/docs/` with an `index.txt`, so a model talking to `flow mcp` reads the pages that
 * match the CLI it is driving, offline, and never a newer site describing commands it does not
 * have.
 */
private[cli] object Docs:

  final case class Page(path: String, title: String, description: String, kind: String):
    def uri: String = s"ankka-flow://docs/$path"

  private val Root = "ankka-flow/docs/"

  private def resource(path: String): Option[String] =
    Option(getClass.getClassLoader.getResourceAsStream(Root + path)).map { stream =>
      try String(stream.readAllBytes(), UTF_8)
      finally stream.close()
    }

  lazy val pages: Vector[Page] =
    resource("index.txt").toVector
      .flatMap(_.linesIterator.map(_.trim).filter(_.nonEmpty))
      .flatMap { path =>
        read(path).map { text =>
          val meta = frontmatter(text)
          Page(
            path,
            meta.getOrElse("title", path),
            meta.getOrElse("description", ""),
            meta.getOrElse("kind", "")
          )
        }
      }

  def read(path: String): Option[String] = if path.contains("..") then None else resource(path)

  private def frontmatter(text: String): Map[String, String] =
    if !text.startsWith("---\n") then Map.empty
    else
      val end = text.indexOf("\n---\n", 4)
      if end < 0 then Map.empty
      else
        text
          .substring(4, end)
          .linesIterator
          .flatMap { line =>
            line.split(":", 2) match
              case Array(key, value) if !key.startsWith(" ") && !key.startsWith("-") =>
                Some(key.trim -> unquote(value.trim))
              case _ => None
          }
          .toMap

  private def unquote(value: String): String =
    val quoted = value.length >= 2 && Set('"', '\'').exists(q => value.head == q && value.last == q)
    if quoted then value.substring(1, value.length - 1) else value

  /** Pages ranked by how many of the query's words appear in their title, description and body. */
  def search(query: String, limit: Int): Vector[Page] =
    val words = query.toLowerCase.split("[^a-z0-9_-]+").filter(_.length > 1).distinct
    if words.isEmpty then Vector.empty
    else
      pages
        .flatMap { page =>
          val head = (page.title + " " + page.description).toLowerCase
          val body = read(page.path).getOrElse("").toLowerCase
          val score = words
            .map(w => (if head.contains(w) then 5 else 0) + (if body.contains(w) then 1 else 0))
            .sum
          Option.when(score > 0)(page -> score)
        }
        .sortBy((page, score) => (-score, page.path))
        .take(limit)
        .map(_._1)

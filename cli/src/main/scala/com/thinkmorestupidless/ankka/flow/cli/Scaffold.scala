package com.thinkmorestupidless.ankka.flow.cli

import java.io.InputStream
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path}

/**
 * Renders a template the CLI carries (`ankka-flow/templates/<language>/`, listed by its
 * `index.txt`) into a directory: exact `{{token}}` replacement in every path and file, nothing
 * else. GitHub's `${{ … }}` never matches a token and passes through. The agent skills under
 * `.claude/` are the plugin's, copied as they are: their examples hold tokens of their own.
 */
object Scaffold:

  /** Copied byte for byte, never rendered. */
  def verbatim(rel: String): Boolean = rel.startsWith(".claude/")

  private val Token = """(?<!\$)\{\{([a-z_]+)\}\}""".r

  def render(text: String, tokens: Map[String, String]): String =
    Token.replaceAllIn(
      text,
      m => java.util.regex.Matcher.quoteReplacement(tokens.getOrElse(m.group(1), m.matched))
    )

  /** Every `{{token}}` still in `text`. */
  def leftover(text: String): Vector[String] = Token.findAllIn(text).toVector

  private def resource(path: String): InputStream =
    Option(getClass.getClassLoader.getResourceAsStream(path))
      .getOrElse(
        throw new IllegalStateException(
          s"the CLI carries no $path; its templates were not built into it"
        )
      )

  /** The template's files, relative paths with their tokens. */
  def index(language: Init.Language): Vector[String] =
    new String(
      resource(s"ankka-flow/templates/${language.id}/index.txt").readAllBytes,
      UTF_8
    ).linesIterator
      .filter(_.nonEmpty)
      .toVector

  /** The rendered project, path by path, without writing it. */
  def files(language: Init.Language, tokens: Map[String, String]): Vector[(String, String)] =
    index(language).map { rel =>
      val text =
        new String(resource(s"ankka-flow/templates/${language.id}/$rel").readAllBytes, UTF_8)
      render(rel, tokens) -> (if verbatim(rel) then text else render(text, tokens))
    }

  /** Write the project under `dir`; refuse, writing nothing, if a token would survive. */
  def write(dir: Path, files: Vector[(String, String)]): Either[Vector[String], Vector[Path]] =
    val left = files.flatMap { (rel, text) =>
      (leftover(rel) ++ (if verbatim(rel) then Vector.empty else leftover(text)))
        .map(t => s"$rel: $t is not a known token")
    }
    if left.nonEmpty then Left(left)
    else
      Right(files.map { (rel, text) =>
        val target = dir.resolve(rel)
        Files.createDirectories(target.getParent)
        Files.write(target, text.getBytes(UTF_8))
      })

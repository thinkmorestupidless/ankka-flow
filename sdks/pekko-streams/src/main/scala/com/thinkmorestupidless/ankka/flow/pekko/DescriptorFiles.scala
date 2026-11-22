package com.thinkmorestupidless.ankka.flow.pekko

import java.io.PrintStream
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path, Paths}

import scala.util.control.NonFatal

import com.thinkmorestupidless.ankka.flow.descriptor.DescriptorJson

/**
 * The main's two descriptor modes: `--descriptor <path>` writes the descriptor, and
 * `--descriptor-check <path>` exits 1 when the file there is missing or differs. Exit 2 for a
 * declaration that is refused, or for arguments that are neither mode nor empty.
 */
object DescriptorFiles:

  /** The exit code of a descriptor mode, or `None` when `args` ask to run the streamlet. */
  def handle(
      streamlet: PekkoStreamlet,
      args: List[String],
      out: PrintStream = System.out,
      err: PrintStream = System.err
  ): Option[Int] =
    args match
      case Nil => None
      case "--descriptor" :: path :: Nil =>
        Some(withText(streamlet, err) { text =>
          write(Paths.get(path), text)
          out.println(s"wrote $path")
          0
        })
      case "--descriptor-check" :: path :: Nil =>
        Some(withText(streamlet, err) { text =>
          val file = Paths.get(path)
          if Files.isRegularFile(file) && new String(Files.readAllBytes(file), UTF_8) == text then
            out.println(s"$path is current")
            0
          else
            err.println(
              s"$path differs from what ${streamlet.name} declares; write it with --descriptor"
            )
            1
        })
      case _ =>
        err.println(
          "usage: <main> [--descriptor <path> | --descriptor-check <path>]; with no arguments it runs the streamlet"
        )
        Some(2)

  private def withText(streamlet: PekkoStreamlet, err: PrintStream)(f: String => Int): Int =
    try f(DescriptorJson.write(streamlet.descriptor()))
    catch
      case NonFatal(e) =>
        err.println(s"${streamlet.name}: ${e.getMessage}")
        2

  private def write(file: Path, text: String): Unit =
    Option(file.toAbsolutePath.getParent).foreach(Files.createDirectories(_))
    Files.write(file, text.getBytes(UTF_8)): Unit

package com.thinkmorestupidless.ankka.flow.cli

import java.nio.file.Path

import scala.jdk.CollectionConverters.*
import scala.util.Try

import com.typesafe.config.ConfigFactory

/** Streamlet name to image reference, from `--images <file>` (HOCON map) and `--image name=ref`. */
object Images:

  def load(file: Option[Path], pairs: List[String]): Either[Vector[String], Map[String, String]] =
    val fromFile = file match
      case None => Right(Map.empty[String, String])
      case Some(f) =>
        Try {
          val c = ConfigFactory.parseFile(f.toFile).resolve()
          c.root.keySet.asScala.map(k => k -> c.getString(s"\"$k\"")).toMap
        }.toEither.left.map(e => Vector(s"images: ${e.getMessage}"))
    val fromPairs = pairs.map { p =>
      p.split("=", 2) match
        case Array(n, ref) if n.nonEmpty && ref.nonEmpty => Right(n -> ref)
        case _ => Left(s"--image '$p' is not name=reference")
    }
    val problems = fromFile.left.toSeq.flatten ++ fromPairs.collect { case Left(e) => e }
    if problems.nonEmpty then Left(problems.toVector)
    else Right(fromFile.getOrElse(Map.empty) ++ fromPairs.collect { case Right(kv) => kv })

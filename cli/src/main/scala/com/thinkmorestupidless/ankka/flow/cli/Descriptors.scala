package com.thinkmorestupidless.ankka.flow.cli

import java.nio.file.{Files, Path}

import scala.jdk.CollectionConverters.*
import scala.util.Try

import com.thinkmorestupidless.ankka.flow.descriptor.{Descriptor, DescriptorValidation}

/** Every `*.json` in a directory, read and validated as a descriptor (DESCRIPTOR.md). */
object Descriptors:

  final case class Loaded(byName: Map[String, Descriptor], problems: Vector[String])

  def load(dir: Path): Loaded =
    if !Files.isDirectory(dir) then
      Loaded(Map.empty, Vector(s"descriptors: '$dir' is not a directory"))
    else
      val files = Try(Files.list(dir).iterator.asScala.toVector)
        .getOrElse(Vector.empty)
        .filter(_.getFileName.toString.endsWith(".json"))
        .sortBy(_.getFileName.toString)
      val read = files.map { f =>
        val name = f.getFileName.toString
        val parsed = Try(new String(Files.readAllBytes(f), "UTF-8")).toEither.left
          .map(e => Vector(s"descriptor $name: ${e.getMessage}"))
          .flatMap(t =>
            DescriptorValidation.check(t).left.map(_.distinct.map(p => s"descriptor $name: $p"))
          )
        name -> parsed
      }
      val good = read.collect { case (_, Right(d)) => d }
      val duplicates = good
        .groupBy(_.streamlet.name)
        .collect { case (n, ds) if ds.size > 1 => s"${ds.size} descriptors declare streamlet '$n'" }
        .toVector
        .sorted
      val empty = Option.when(files.isEmpty)(s"descriptors: '$dir' holds no *.json files").toVector
      Loaded(
        good.map(d => d.streamlet.name -> d).toMap,
        empty ++ read.collect { case (_, Left(ps)) => ps }.flatten ++ duplicates
      )

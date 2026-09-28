package com.thinkmorestupidless.ankka.flow.sidecar

import java.nio.file.{Files, Path}

import scala.util.Try

import ankka.flow.v1.discovery.{Spec, StreamletDescriptor}
import com.thinkmorestupidless.ankka.flow.protocol.{DescriptorJson, DescriptorValidation}

/** The deployed descriptor: `descriptor.json` in FLOW_CONFIG_DIR, validated on load. */
final case class Descriptor(spec: Spec):
  def streamlet: StreamletDescriptor = spec.getStreamlet

  /** Every way the process's answer to Discover differs from what was deployed (FR-008). */
  def compare(discovered: Spec): Vector[String] =
    discovered.streamlet match
      case None    => Vector("the process's Spec names no streamlet")
      case Some(d) => DescriptorValidation.compare(streamlet, d)

object Descriptor:

  def load(file: Path): Either[Vector[String], Descriptor] =
    Try(new String(Files.readAllBytes(file), "UTF-8")).toEither.left
      .map(e => Vector(s"${file.getFileName}: ${e.getMessage}"))
      .flatMap(text => DescriptorJson.read(text).left.map(e => Vector(s"${file.getFileName}: $e")))
      .flatMap { spec =>
        val problems = DescriptorValidation.validate(spec)
        if problems.isEmpty then Right(Descriptor(spec))
        else Left(problems.map(p => s"${file.getFileName}: $p"))
      }

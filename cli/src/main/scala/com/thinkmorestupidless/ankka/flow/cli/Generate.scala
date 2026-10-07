package com.thinkmorestupidless.ankka.flow.cli

import java.nio.file.Path

import scala.sys.process.{Process, ProcessLogger}
import scala.util.Try

import com.thinkmorestupidless.ankka.flow.blueprint.{
  BlueprintProblem,
  BuiltinHasImage,
  MissingImage
}
import com.thinkmorestupidless.ankka.flow.crd.{FlowSerialization, OnDelete}

/**
 * `flow generate`'s work as a value, so the command and the MCP tool write the same bytes: verify,
 * check the images, name the pipeline, write the resource as YAML.
 */
object Generate:

  final case class Request(
      in: Verify.Inputs,
      images: Option[Path],
      image: List[String],
      pipeline: Option[String],
      version: Option[String],
      namespace: Option[String],
      deleteManagedTopics: Boolean
  )

  /** The notes to print on stderr, and the YAML. */
  final case class Generated(notes: Vector[String], yaml: String, pipeline: String)

  private val PipelineName = """[a-z0-9]([-a-z0-9]*[a-z0-9])?""".r

  def run(g: Request): Either[Vector[String], Generated] =
    val verified = Verify.run(g.in)
    val images   = Images.load(g.images, g.image)
    val missing = verified.toOption.toVector.flatMap { v =>
      val known              = images.getOrElse(Map.empty)
      val (builtin, process) = v.blueprint.streamlets.partition(_.descriptor.builtin)
      process
        .filterNot(s => known.contains(s.name))
        .map(s => BlueprintProblem.toMessage(MissingImage(s.name))) ++
        builtin
          .filter(s => known.contains(s.name))
          .map(s => BlueprintProblem.toMessage(BuiltinHasImage(s.name)))
    }
    val pipeline = g.pipeline
      .orElse(verified.toOption.flatMap(_.blueprint.name))
      .getOrElse(g.in.blueprint.getFileName.toString.takeWhile(_ != '.'))
    val nameProblem =
      Option.when(!PipelineName.matches(pipeline) || pipeline.length > 40)(
        s"pipeline id '$pipeline' must be 1-40 of [a-z0-9-], not starting or ending with '-'"
      )
    val problems =
      (verified.left.toSeq.flatten ++ images.left.toSeq.flatten ++ missing ++ nameProblem).toVector
    if problems.nonEmpty then Left(problems)
    else
      val v        = verified.toOption.get
      val version  = g.version.getOrElse(gitVersion(g.in.blueprint))
      val onDelete = OnDelete(if g.deleteManagedTopics then OnDelete.Delete else OnDelete.Keep)
      val resource =
        ResourceWriter.write(v, images.toOption.get, pipeline, version, g.namespace, onDelete)
      Right(Generated(v.notes.toVector, FlowSerialization.toYaml(resource), pipeline))

  private def gitVersion(blueprint: Path): String =
    val dir = Option(blueprint.toAbsolutePath.getParent).getOrElse(Path.of("."))
    Try(
      Process(Seq("git", "-C", dir.toString, "describe", "--tags", "--always", "--dirty"))
        .!!(ProcessLogger(_ => ()))
        .trim
    ).toOption
      .filter(_.nonEmpty)
      .getOrElse("unversioned")

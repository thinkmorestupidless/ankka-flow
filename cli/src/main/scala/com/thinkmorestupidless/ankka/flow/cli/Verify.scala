package com.thinkmorestupidless.ankka.flow.cli

import java.nio.file.{Files, Path}

import scala.util.Try

import com.thinkmorestupidless.ankka.flow.blueprint.*
import com.thinkmorestupidless.ankka.flow.descriptor.Json
import com.typesafe.config.ConfigFactory

/**
 * Blueprint verification over descriptor files, with no language runtime and no network (FR-006):
 * every problem in one pass (FR-004, SC-002).
 */
object Verify:

  /** `descriptors` is optional on the command line; without it every streamlet is unknown. */
  final case class Inputs(blueprint: Path, descriptors: Option[Path], conf: List[Path])

  final case class Verified(
      blueprint: VerifiedBlueprint,
      descriptors: Descriptors.Loaded,
      overrides: Overrides,
      parameters: Map[String, Vector[(String, Json)]],
      replicas: Map[String, Int],
      parallelism: Map[String, Option[Int]],
      flink: Map[String, Option[FlinkOverrides]],
      notes: Vector[String],
      blueprintFile: Path
  )

  def run(in: Inputs): Either[Vector[String], Verified] =
    val loaded = in.descriptors.fold(Descriptors.Loaded(Map.empty, Vector.empty))(Descriptors.load)
    val overrides =
      in.conf.foldLeft[Either[Vector[String], Overrides]](Right(Overrides.empty)) { (acc, f) =>
        acc.flatMap { o =>
          Try(ConfigFactory.parseFile(f.toFile).resolve()).toEither.left
            .map(e => Vector(s"--conf $f: ${e.getMessage}"))
            .map(c => Overrides(c.withFallback(o.config)))
        }
      }
    val parsed =
      if !Files.isRegularFile(in.blueprint) then
        Left(Vector(s"blueprint: '${in.blueprint}' is not a file"))
      else
        Try(ConfigFactory.parseFile(in.blueprint.toFile).resolve()).toEither.left
          .map(e => Vector(BlueprintProblem.toMessage(BlueprintFormatError(e.getMessage))))
          .map(c =>
            Blueprint.parseConfig(
              c,
              loaded.byName.values.toVector.sortBy(_.streamlet.name).map(StreamletDescriptor(_))
            )
          )

    val blueprintProblems = parsed.fold(identity, _.problems.map(BlueprintProblem.toMessage))
    val notes =
      parsed.toOption.toVector.flatMap(_.notes.map(n => s"note: ${BlueprintProblem.toMessage(n)}"))
    val verified = parsed.toOption.flatMap(_.verified.toOption)

    val overrideProblems = (overrides, verified) match
      case (Left(ps), _) => ps
      case (Right(o), Some(v)) =>
        (o.unknownNames(v) ++ o.settingProblems(v) ++ v.streamlets.flatMap(s =>
          o.parameters(s).left.toSeq.flatten ++ o.replicas(s).left.toSeq ++
            o.parallelism(s).left.toSeq ++ o.flink(s).left.toSeq
        )).distinct
      case _ => Vector.empty

    val problems = loaded.problems ++ blueprintProblems ++ overrideProblems
    (verified, overrides) match
      case (Some(v), Right(o)) if problems.isEmpty =>
        Right(
          Verified(
            v,
            loaded,
            o,
            v.streamlets.map(s => s.name -> o.parameters(s).toOption.get).toMap,
            v.streamlets.map(s => s.name -> o.replicas(s).toOption.get).toMap,
            v.streamlets.map(s => s.name -> o.parallelism(s).toOption.get).toMap,
            v.streamlets.map(s => s.name -> o.flink(s).toOption.get).toMap,
            notes,
            in.blueprint
          )
        )
      case _ => Left(if problems.isEmpty then Vector("the blueprint did not verify") else problems)

package com.thinkmorestupidless.ankka.flow.cli

import java.nio.file.{Files, Path}

import scala.util.Try

import com.thinkmorestupidless.ankka.flow.blueprint.*
import com.thinkmorestupidless.ankka.flow.protocol.{Builtins, Json}
import com.typesafe.config.ConfigFactory

/**
 * Blueprint verification over descriptor files, with no language runtime and no network (FR-006):
 * every problem in one pass (FR-004, SC-002).
 */
object Verify:

  /** `descriptors` is absent when the blueprint uses only built-in descriptors. */
  final case class Inputs(blueprint: Path, descriptors: Option[Path], conf: List[Path])

  final case class Verified(
      blueprint: VerifiedBlueprint,
      descriptors: Descriptors.Loaded,
      overrides: Overrides,
      parameters: Map[String, Vector[(String, Json)]],
      replicas: Map[String, Int],
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
              loaded.byName.values.toVector.map(s => StreamletDescriptor(s.getStreamlet)) ++
                Builtins.all.map(s => StreamletDescriptor(s.getStreamlet, builtin = true))
            )
          )

    val blueprintProblems = parsed.fold(identity, _.problems.map(BlueprintProblem.toMessage))
    val notes =
      parsed.toOption.toVector.flatMap(_.notes.map(n => s"note: ${BlueprintProblem.toMessage(n)}"))
    val verified = parsed.toOption.flatMap(_.verified.toOption)

    val overrideProblems = (overrides, verified) match
      case (Left(ps), _) => ps
      case (Right(o), Some(v)) =>
        o.unknownNames(v) ++ v.streamlets.flatMap(s =>
          o.parameters(s).left.toSeq.flatten ++ o.replicas(s.name).left.toSeq
        )
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
            v.streamlets.map(s => s.name -> o.replicas(s.name).toOption.get).toMap,
            notes,
            in.blueprint
          )
        )
      case _ => Left(if problems.isEmpty then Vector("the blueprint did not verify") else problems)

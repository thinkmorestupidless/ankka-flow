package com.thinkmorestupidless.ankka.flow.cli

import java.nio.file.{Files, Path}

import scala.util.Try

import com.thinkmorestupidless.ankka.flow.blueprint.*
import com.thinkmorestupidless.ankka.flow.descriptor.{Contract, Json}

/**
 * The topics an ankka project declares, as `ankka projects topics list -p <project> -o json` prints
 * them: an array of objects with `name` and, when the topic declares one,
 * `contract: {name, fingerprint}`. Every other field is ignored.
 */
final case class AnkkaDeclarations(project: String, topics: Map[String, Option[Contract]])

object AnkkaDeclarations:

  /** `--ankka-topics <project>=<file>`, read; the problem with the option or the file otherwise. */
  def load(option: String): Either[String, AnkkaDeclarations] =
    option.split("=", 2) match
      case Array(project, file) if project.nonEmpty && file.nonEmpty =>
        val path = Path.of(file)
        for
          text <- Try(Files.readString(path)).toEither.left.map(e =>
            s"--ankka-topics $option: cannot read '$file': ${e.getMessage}"
          )
          json   <- Json.parse(text).left.map(e => s"--ankka-topics $option: '$file': $e")
          topics <- parse(json).left.map(e => s"--ankka-topics $option: '$file': $e")
        yield AnkkaDeclarations(project, topics)
      case _ => Left(s"--ankka-topics $option: expected <project>=<file>")

  private def parse(json: Json): Either[String, Map[String, Option[Contract]]] =
    json match
      case Json.Arr(items) =>
        items.foldLeft[Either[String, Map[String, Option[Contract]]]](Right(Map.empty)) {
          (acc, item) =>
            acc.flatMap { m =>
              item.field("name") match
                case Some(Json.Str(name)) =>
                  // No contract, or null, is a topic that declares none; anything else must be one.
                  val contract: Either[String, Option[Contract]] = item.field("contract") match
                    case None | Some(Json.Null) => Right(None)
                    case Some(c: Json.Obj) =>
                      c.field("name") match
                        case Some(Json.Str(n)) =>
                          Right(
                            Some(
                              Contract(n, c.field("fingerprint").collect { case Json.Str(f) => f })
                            )
                          )
                        case _ => Left(s"topic '$name': a contract must have a name")
                    case Some(_) => Left(s"topic '$name': a contract must have a name")
                  contract.map(c => m + (name -> c))
                case _ => Left("every topic must have a name")
            }
        }
      case _ => Left("expected the JSON array ankka projects topics list -o json prints")

  /**
   * Each ankka topic of the blueprint checked against its project's declarations: the problems, and
   * a note for each project named with no declarations to check against.
   */
  def check(
      blueprint: VerifiedBlueprint,
      declarations: Vector[AnkkaDeclarations]
  ): (Vector[String], Vector[String]) =
    val byProject = declarations.map(d => d.project -> d).toMap
    val ankkaTopics =
      blueprint.topics.flatMap(t => t.ankka.map(t -> _)).sortBy(_._2.toString)
    val named = ankkaTopics.map(_._2.project).distinct
    val unnamed = declarations
      .map(_.project)
      .filterNot(named.contains)
      .map(p => BlueprintProblem.toMessage(AnkkaProjectNotInBlueprint(p)))
    val problems = ankkaTopics
      .flatMap { (topic, ref) =>
        byProject.get(ref.project).toVector.flatMap { d =>
          d.topics.get(ref.name) match
            case None =>
              Vector(AnkkaTopicNotDeclared(ref, d.topics.keys.toVector.sorted))
            case Some(None) => Vector(AnkkaTopicWithoutContract(ref))
            case Some(Some(declared)) =>
              topic.connections
                .filterNot(p => Contract.connects(p.contract, declared))
                .map(p => AnkkaContractMismatch(p.portPath.toString, p.contract, ref, declared))
        }
      }
      .map(BlueprintProblem.toMessage)
    val notes = named
      .filterNot(byProject.contains)
      .map(p => s"note: ${BlueprintProblem.toMessage(AnkkaTopicsNotChecked(p))}")
    (unnamed ++ problems, notes)

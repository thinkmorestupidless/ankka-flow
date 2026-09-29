package com.thinkmorestupidless.ankka.flow.operator

import scala.util.Try

import com.thinkmorestupidless.ankka.flow.crd.StreamletSpec
import com.thinkmorestupidless.ankka.flow.protocol.{Builtins, DescriptorJson, Json}

/**
 * What the operator adds for a streamlet whose descriptor is built in (feature 002,
 * contracts/built-in-streamlets.md): the Secret its stage reads, mounted into the sidecar, and the
 * refusals when that cannot be done.
 */
object BuiltinStages:

  /** Where the stage's Secret is mounted in the sidecar container, one file per key. */
  val CredentialsDir = "/etc/flow/neo4j"

  /** The keys the Neo4j merge sink cannot open without. */
  val RequiredKeys: Vector[String] = Vector("uri", "username", "password")

  /** The Secret a built-in streamlet names in its `secret` parameter, if it names one. */
  def secretName(s: StreamletSpec): Option[String] =
    Option(s.config)
      .flatMap(_.get("secret"))
      .filter(_.isTextual)
      .map(_.asText.trim)
      .filter(_.nonEmpty)

  /** The built-in's name from the embedded descriptor. */
  def descriptorName(s: StreamletSpec): Option[String] =
    Option(s.descriptor).flatMap(node =>
      Try(Json.parse(node.toString).flatMap(DescriptorJson.streamletFromJson)).toOption
        .flatMap(_.toOption)
        .map(_.name)
    )

  /** Every reason the operator cannot run this built-in streamlet, or none. */
  def problems(s: StreamletSpec, namespace: String, observed: Observed): Vector[String] =
    if !s.builtin then Vector.empty
    else
      val image =
        Option.when(Option(s.image).exists(_.nonEmpty))(
          s"streamlet '${s.name}' is built in and takes no image"
        )
      val known = descriptorName(s).filterNot(n => Builtins.byName(n).isDefined).map { n =>
        s"streamlet '${s.name}' names built-in '$n', which this operator does not know"
      }
      val secret = secretName(s) match
        case None =>
          Vector(s"streamlet '${s.name}' is built in and names no Secret in its 'secret' parameter")
        case Some(name) =>
          observed.secretProblems.get(name) match
            case Some(problem) => Vector(s"streamlet '${s.name}': Secret '$name': $problem")
            case None =>
              observed.secrets.get(name) match
                case None =>
                  Vector(
                    s"streamlet '${s.name}' names Secret '$name', which does not exist in namespace '$namespace'"
                  )
                case Some(state) =>
                  RequiredKeys
                    .filterNot(state.keys.contains)
                    .map(k => s"streamlet '${s.name}': Secret '$name' has no key '$k'")
      image.toVector ++ known ++ secret

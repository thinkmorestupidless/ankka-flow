package com.thinkmorestupidless.ankka.flow.descriptor

import scala.util.Try

import com.typesafe.config.ConfigFactory

/**
 * The rules every descriptor obeys (`DESCRIPTOR.md`). `flow` and the operator apply the same
 * function, so a descriptor that verifies anywhere verifies everywhere.
 */
object DescriptorValidation:

  val FlinkSection = "a flink streamlet's descriptor has a flink section, and no other has"

  private val StreamletName = """[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?""".r
  private val PortName      = """[a-z][a-z0-9-]{0,62}""".r
  private val ParamKey      = """[a-z][a-z0-9-]*""".r

  /** Read and validate: the descriptor, or every problem with it. */
  def check(text: String): Either[Vector[String], Descriptor] =
    DescriptorJson.read(text) match
      case Left(problem) => Left(Vector(problem))
      case Right(d) =>
        val problems = validate(d)
        if problems.isEmpty then Right(d) else Left(problems)

  def validate(d: Descriptor): Vector[String] =
    val version = DescriptorJson.majorOf(d.descriptorVersion) match
      case Some(Descriptor.Major) => Vector.empty
      case _                      => Vector(DescriptorJson.unreadableVersion(d.descriptorVersion))
    version ++ validateStreamlet(d.streamlet)

  def validateStreamlet(d: StreamletDescriptor): Vector[String] =
    val name =
      if StreamletName.matches(d.name) then Vector.empty
      else
        Vector(
          s"streamlet name '${d.name}' must be 1-63 of [a-z0-9-], not starting or ending with '-'"
        )

    val flink = (d.engine, d.flink) match
      case (Engine.Flink, Some(job)) =>
        val entry =
          if job.entryClass.trim.nonEmpty then Vector.empty
          else Vector("a flink streamlet's descriptor names its entry class")
        val version =
          if FlinkJob.Versions.contains(job.version) then Vector.empty
          else
            Vector(
              s"flink version ${job.version} is not one this flow runs (${FlinkJob.Versions.mkString(", ")})"
            )
        entry ++ version
      case (Engine.Flink, None) | (_, Some(_)) => Vector(FlinkSection)
      case _                                   => Vector.empty

    val all = d.inlets.map("inlet" -> _) ++ d.outlets.map("outlet" -> _)
    val portNames = all.collect {
      case (kind, p) if !PortName.matches(p.name) =>
        s"$kind name '${p.name}' must match [a-z][a-z0-9-]{0,62}"
    }
    val duplicates = all
      .groupBy(_._2.name)
      .collect { case (n, ps) if ps.size > 1 => s"port '$n' is declared ${ps.size} times" }
      .toVector
      .sorted
    val contracts = all.flatMap { (kind, p) =>
      val contractName =
        if Contract.validName(p.contract.name) then Vector.empty
        else
          Vector(
            s"$kind '${p.name}': contract name '${p.contract.name}' must match ${Contract.NameRule}"
          )
      val fingerprint = p.contract.fingerprint.toVector.collect {
        case fp if !Contract.validFingerprint(fp) =>
          s"$kind '${p.name}': fingerprint '$fp' must be sha256: and 64 lowercase hex digits"
      }
      contractName ++ fingerprint
    }

    val params = d.parameters.flatMap { p =>
      val key =
        if ParamKey.matches(p.key) then Vector.empty
        else Vector(s"parameter key '${p.key}' must match [a-z][a-z0-9-]*")
      val default = p.default.toVector.flatMap(v =>
        parseValue(p.kind, v).left.toSeq.map(e => s"parameter '${p.key}': $e")
      )
      key ++ default
    }
    val paramDupes = d.parameters
      .groupBy(_.key)
      .collect { case (k, ps) if ps.size > 1 => s"parameter '$k' is declared ${ps.size} times" }
      .toVector
      .sorted

    name ++ flink ++ portNames ++ duplicates ++ contracts ++ params ++ paramDupes

  /** A parameter value as its type. */
  def parseValue(t: ParameterType, value: String): Either[String, Json] =
    def bad = Left(s"'$value' is not a ${typeName(t)}")
    t match
      case ParameterType.String => Right(Json.Str(value))
      case ParameterType.Integer =>
        value.trim.toLongOption.map(n => Json.Num(BigDecimal(n))).toRight("").left.flatMap(_ => bad)
      case ParameterType.Double =>
        value.trim.toDoubleOption
          .map(n => Json.Num(BigDecimal(n)))
          .toRight("")
          .left
          .flatMap(_ => bad)
      case ParameterType.Boolean =>
        value.trim match
          case "true"  => Right(Json.Bool(true))
          case "false" => Right(Json.Bool(false))
          case _       => bad
      case ParameterType.Duration =>
        Try(ConfigFactory.parseString(s"v = \"$value\"").getDuration("v")).toOption
          .map(_ => Json.Str(value.trim))
          .toRight("")
          .left
          .flatMap(_ => bad)
      case ParameterType.MemorySize =>
        Try(ConfigFactory.parseString(s"v = \"$value\"").getMemorySize("v")).toOption
          .map(_ => Json.Str(value.trim))
          .toRight("")
          .left
          .flatMap(_ => bad)

  def typeName(t: ParameterType): String = t.wire.toLowerCase.replace('_', ' ')

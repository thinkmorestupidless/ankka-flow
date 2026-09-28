package com.thinkmorestupidless.ankka.flow.protocol

import scala.util.Try

import ankka.flow.v1.discovery.*
import com.typesafe.config.ConfigFactory

/**
 * The rules every descriptor obeys (DESCRIPTOR.md, *Validation*). The CLI, the operator and the
 * sidecar apply the same function, so a descriptor that verifies anywhere verifies everywhere.
 */
object DescriptorValidation:

  private val StreamletName = """[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?""".r
  private val PortName      = """[a-z][a-z0-9-]{0,62}""".r
  private val ParamKey      = """[a-z][a-z0-9-]*""".r

  def validate(spec: Spec): Vector[String] =
    val version = ProtocolVersion.parse(spec.protocolVersion).left.toSeq.toVector
    val body = spec.streamlet match
      case None    => Vector("the descriptor names no streamlet")
      case Some(d) => validateStreamlet(d)
    version ++ body

  def validateStreamlet(d: StreamletDescriptor): Vector[String] =
    val name =
      if StreamletName.matches(d.name) then Vector.empty
      else
        Vector(
          s"streamlet name '${d.name}' must be 1-63 of [a-z0-9-], not starting or ending with '-'"
        )

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
      p.contract match
        case None => Vector(s"$kind '${p.name}' has no contract")
        case Some(c) =>
          val format =
            if c.format == Fingerprint.Format then Vector.empty
            else
              Vector(
                s"$kind '${p.name}' uses format '${c.format}', which this version does not support"
              )
          val schema =
            if c.schemaName.nonEmpty then Vector.empty
            else Vector(s"$kind '${p.name}' has no schema name")
          val fp =
            if c.schemaName.isEmpty || c.fingerprint == Fingerprint.fingerprint(c.schemaName) then
              Vector.empty
            else
              Vector(
                s"$kind '${p.name}' fingerprint does not match schema name '${c.schemaName}' (was the descriptor edited by hand?)"
              )
          format ++ schema ++ fp
    }

    val params = d.configParameters.flatMap { p =>
      val key =
        if ParamKey.matches(p.key) then Vector.empty
        else Vector(s"parameter key '${p.key}' must match [a-z][a-z0-9-]*")
      val default =
        if p.defaultValue.isEmpty then Vector.empty
        else parseValue(p.`type`, p.defaultValue).left.toSeq.map(e => s"parameter '${p.key}': $e")
      key ++ default
    }
    val paramDupes = d.configParameters
      .groupBy(_.key)
      .collect { case (k, ps) if ps.size > 1 => s"parameter '$k' is declared ${ps.size} times" }
      .toVector
      .sorted

    name ++ portNames ++ duplicates ++ contracts ++ params ++ paramDupes

  /** A parameter value as its type: the canonical rendering Start.config_json carries. */
  def parseValue(t: ConfigType, value: String): Either[String, Json] =
    def bad = Left(s"'$value' is not a ${typeName(t)}")
    t match
      case ConfigType.STRING => Right(Json.Str(value))
      case ConfigType.INTEGER =>
        value.trim.toLongOption.map(n => Json.Num(BigDecimal(n))).toRight("").left.flatMap(_ => bad)
      case ConfigType.DOUBLE =>
        value.trim.toDoubleOption
          .map(n => Json.Num(BigDecimal(n)))
          .toRight("")
          .left
          .flatMap(_ => bad)
      case ConfigType.BOOLEAN =>
        value.trim match
          case "true"  => Right(Json.Bool(true))
          case "false" => Right(Json.Bool(false))
          case _       => bad
      case ConfigType.DURATION =>
        Try(ConfigFactory.parseString(s"v = \"$value\"").getDuration("v")).toOption
          .map(_ => Json.Str(value.trim))
          .toRight("")
          .left
          .flatMap(_ => bad)
      case ConfigType.MEMORY_SIZE =>
        Try(ConfigFactory.parseString(s"v = \"$value\"").getMemorySize("v")).toOption
          .map(_ => Json.Str(value.trim))
          .toRight("")
          .left
          .flatMap(_ => bad)
      case ConfigType.Unrecognized(n) => Left(s"unknown parameter type $n")

  def typeName(t: ConfigType): String = t.name.toLowerCase.replace('_', ' ')

  /**
   * Every difference between the deployed descriptor and what the process declared in discovery
   * (FR-008). Field by field, so a message names what changed, not merely that something did.
   */
  def compare(deployed: StreamletDescriptor, discovered: StreamletDescriptor): Vector[String] =
    def diff(what: String, a: String, b: String) =
      Option.when(a != b)(s"$what: deployed '$a', process declares '$b'")
    val top = Vector(
      diff("streamlet name", deployed.name, discovered.name)
    ).flatten
    def portDiffs(kind: String, a: Seq[Port], b: Seq[Port]): Vector[String] =
      val am = a.map(p => p.name -> p).toMap
      val bm = b.map(p => p.name -> p).toMap
      val missing = (am.keySet -- bm.keySet).toVector.sorted.map(n =>
        s"$kind '$n' is deployed but the process does not declare it"
      )
      val extra = (bm.keySet -- am.keySet).toVector.sorted.map(n =>
        s"$kind '$n' is declared by the process but not deployed"
      )
      val changed = (am.keySet intersect bm.keySet).toVector.sorted.flatMap { n =>
        val (x, y) = (am(n).contract, bm(n).contract)
        Option.when(x != y)(
          s"$kind '$n' contract: deployed ${render(x)}, process declares ${render(y)}"
        )
      }
      missing ++ extra ++ changed
    val pa = deployed.configParameters.map(p => p.key -> p).toMap
    val pb = discovered.configParameters.map(p => p.key -> p).toMap
    val params =
      (pa.keySet -- pb.keySet).toVector.sorted.map(k =>
        s"parameter '$k' is deployed but the process does not declare it"
      ) ++
        (pb.keySet -- pa.keySet).toVector.sorted.map(k =>
          s"parameter '$k' is declared by the process but not deployed"
        ) ++
        (pa.keySet intersect pb.keySet).toVector.sorted.flatMap { k =>
          val (x, y) = (pa(k), pb(k))
          Vector(
            Option.when(x.`type` != y.`type`)(
              s"parameter '$k' type: deployed ${typeName(x.`type`)}, process declares ${typeName(y.`type`)}"
            ),
            Option.when(x.defaultValue != y.defaultValue)(
              s"parameter '$k' default: deployed '${x.defaultValue}', process declares '${y.defaultValue}'"
            )
          ).flatten
        }
    top ++ portDiffs("inlet", deployed.inlets, discovered.inlets) ++
      portDiffs("outlet", deployed.outlets, discovered.outlets) ++ params

  private def render(c: Option[Contract]): String =
    c.fold("none")(c => s"${c.format} ${c.schemaName}")

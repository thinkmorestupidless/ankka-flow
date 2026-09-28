package com.thinkmorestupidless.ankka.flow.protocol

import ankka.flow.v1.discovery.*

/**
 * The descriptor file: the discovery `Spec` as canonical JSON (DESCRIPTOR.md, research R5).
 *
 * Proto field names in snake_case, ports sorted by name, parameters by key, enums by name, every
 * field at its proto3 default omitted. `write` always produces the canonical form, whatever order
 * the `Spec` was built in.
 */
object DescriptorJson:
  import Json.*

  def write(spec: Spec): String = Json.canonical(toJson(spec))

  def toJson(spec: Spec): Json =
    obj(
      "protocol_version" -> str(spec.protocolVersion),
      "sdk" -> spec.sdk.map(s => Obj(fields("name" -> str(s.name), "version" -> str(s.version)))),
      "streamlet" -> spec.streamlet.map(streamletToJson)
    )

  def streamletToJson(d: StreamletDescriptor): Json =
    Obj(
      fields(
        "name"        -> str(d.name),
        "description" -> str(d.description),
        "inlets"      -> ports(d.inlets),
        "outlets"     -> ports(d.outlets),
        "config_parameters" -> list(d.configParameters.sortBy(_.key).map { p =>
          Obj(
            fields(
              "key"           -> str(p.key),
              "description"   -> str(p.description),
              "type"          -> (if p.`type`.isString then None else Some(Str(p.`type`.name))),
              "default_value" -> str(p.defaultValue)
            )
          )
        })
      )
    )

  private def ports(ps: Seq[Port]): Option[Json] =
    list(ps.sortBy(_.name).map { p =>
      Obj(
        fields(
          "name" -> str(p.name),
          "contract" -> p.contract.map(c =>
            Obj(
              fields(
                "format"      -> str(c.format),
                "schema_name" -> str(c.schemaName),
                "fingerprint" -> str(c.fingerprint)
              )
            )
          )
        )
      )
    })

  private def str(s: String): Option[Json]      = Option.when(s.nonEmpty)(Str(s))
  private def list(xs: Seq[Json]): Option[Json] = Option.when(xs.nonEmpty)(Arr(xs.toVector))
  private def fields(fs: (String, Option[Json])*) = fs.toVector.collect { case (k, Some(v)) =>
    k -> v
  }
  private def obj(fs: (String, Option[Json])*): Json = Obj(fields(fs*))

  // ── Reading ─────────────────────────────────────────────────────────────

  def read(text: String): Either[String, Spec] = Json.parse(text).flatMap(fromJson)

  def fromJson(json: Json): Either[String, Spec] =
    for
      _         <- asObj(json, "descriptor")
      version   <- optStr(json, "protocol_version")
      sdk       <- json.field("sdk").map(sdkFromJson).fold(Right(None))(_.map(Some(_)))
      streamlet <- json.field("streamlet").map(streamletFromJson).fold(Right(None))(_.map(Some(_)))
    yield Spec(protocolVersion = version, sdk = sdk, streamlet = streamlet)

  private def sdkFromJson(j: Json): Either[String, SdkInfo] =
    for
      _ <- asObj(j, "sdk")
      n <- optStr(j, "name")
      v <- optStr(j, "version")
    yield SdkInfo(n, v)

  def streamletFromJson(j: Json): Either[String, StreamletDescriptor] =
    for
      _       <- asObj(j, "streamlet")
      name    <- optStr(j, "name")
      desc    <- optStr(j, "description")
      inlets  <- portsFromJson(j, "inlets")
      outlets <- portsFromJson(j, "outlets")
      params  <- arr(j, "config_parameters").flatMap(traverse(_)(paramFromJson))
    yield StreamletDescriptor(name, desc, inlets, outlets, params)

  private def portsFromJson(j: Json, key: String): Either[String, Seq[Port]] =
    arr(j, key).flatMap(traverse(_) { p =>
      for
        _    <- asObj(p, key)
        name <- optStr(p, "name")
        c <- p.field("contract") match
          case None => Right(None)
          case Some(c) =>
            for
              _  <- asObj(c, "contract")
              f  <- optStr(c, "format")
              s  <- optStr(c, "schema_name")
              fp <- optStr(c, "fingerprint")
            yield Some(Contract(f, s, fp))
      yield Port(name, c)
    })

  private def paramFromJson(p: Json): Either[String, ConfigParameter] =
    for
      _     <- asObj(p, "config_parameters")
      key   <- optStr(p, "key")
      desc  <- optStr(p, "description")
      tname <- optStr(p, "type")
      t <-
        if tname.isEmpty then Right(ConfigType.STRING)
        else ConfigType.fromName(tname).toRight(s"unknown config parameter type '$tname'")
      default <- optStr(p, "default_value")
    yield ConfigParameter(key, desc, t, default)

  private def asObj(j: Json, what: String): Either[String, Unit] = j match
    case Obj(_) => Right(())
    case _      => Left(s"$what must be an object")

  private def optStr(j: Json, key: String): Either[String, String] = j.field(key) match
    case None         => Right("")
    case Some(Str(s)) => Right(s)
    case Some(_)      => Left(s"'$key' must be a string")

  private def arr(j: Json, key: String): Either[String, Vector[Json]] = j.field(key) match
    case None          => Right(Vector.empty)
    case Some(Arr(xs)) => Right(xs)
    case Some(_)       => Left(s"'$key' must be an array")

  private def traverse[A, B](xs: Vector[A])(f: A => Either[String, B]): Either[String, Seq[B]] =
    xs.foldLeft[Either[String, Vector[B]]](Right(Vector.empty)) { (acc, a) =>
      acc.flatMap(bs => f(a).map(bs :+ _))
    }

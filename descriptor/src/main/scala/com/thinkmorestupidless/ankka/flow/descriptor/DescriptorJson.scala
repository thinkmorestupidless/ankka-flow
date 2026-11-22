package com.thinkmorestupidless.ankka.flow.descriptor

/**
 * A descriptor as canonical JSON (`DESCRIPTOR.md`): snake_case, keys sorted, ports sorted by name,
 * parameters by key, every field at its default omitted. `write` always produces the canonical
 * form, whatever order the descriptor was built in.
 *
 * `read` refuses what is not a descriptor of this version with the messages of the descriptor's
 * contract: one that names no engine (version one's among them), one that names an engine `flow`
 * does not know, and one whose major version is not 2. The rest is `DescriptorValidation`'s.
 */
object DescriptorJson:
  import Json.*

  val NoEngine: String = s"a descriptor must name an engine (${Engine.known})"

  def unknownEngine(id: String): String = s"engine '$id' is not one flow knows (${Engine.known})"

  def unreadableVersion(version: String): String =
    s"descriptor version $version is not one this flow reads (${Descriptor.Major}.x)"

  def write(d: Descriptor): String = Json.canonical(toJson(d))

  def toJson(d: Descriptor): Json =
    obj(
      "descriptor_version" -> str(d.descriptorVersion),
      "sdk"       -> d.sdk.map(s => obj("name" -> str(s.name), "version" -> str(s.version))),
      "streamlet" -> Some(streamletToJson(d.streamlet))
    )

  def streamletToJson(s: StreamletDescriptor): Json =
    obj(
      "name"        -> str(s.name),
      "description" -> str(s.description),
      "engine"      -> str(s.engine.id),
      "flink" -> s.flink.map(f =>
        obj("entry_class" -> str(f.entryClass), "version" -> str(f.version))
      ),
      "inlets"  -> ports(s.inlets),
      "outlets" -> ports(s.outlets),
      "config_parameters" -> list(s.parameters.sortBy(_.key).map { p =>
        obj(
          "key"           -> str(p.key),
          "description"   -> str(p.description),
          "type"          -> Option.when(p.kind != ParameterType.String)(Str(p.kind.wire)),
          "default_value" -> p.default.flatMap(str)
        )
      })
    )

  private def ports(ps: Vector[Port]): Option[Json] =
    list(ps.sortBy(_.name).map { p =>
      obj(
        "name" -> str(p.name),
        "contract" -> Some(
          obj("name" -> str(p.contract.name), "fingerprint" -> p.contract.fingerprint.flatMap(str))
        )
      )
    })

  private def str(s: String): Option[Json]      = Option.when(s.nonEmpty)(Str(s))
  private def list(xs: Seq[Json]): Option[Json] = Option.when(xs.nonEmpty)(Arr(xs.toVector))
  private def obj(fs: (String, Option[Json])*): Json =
    Obj(fs.toVector.collect { case (k, Some(v)) => k -> v })

  // ── Reading ─────────────────────────────────────────────────────────────

  def read(text: String): Either[String, Descriptor] = Json.parse(text).flatMap(fromJson)

  def fromJson(json: Json): Either[String, Descriptor] =
    for
      _       <- asObj(json, "descriptor")
      _       <- if json.field("protocol_version").isDefined then Left(NoEngine) else Right(())
      version <- optStr(json, "descriptor_version")
      _ <- majorOf(version) match
        case Some(Descriptor.Major) => Right(())
        case _ => Left(if version.isEmpty then NoEngine else unreadableVersion(version))
      sdk <- json.field("sdk").map(sdkFromJson).fold(Right(None))(_.map(Some(_)))
      streamlet <- json.field("streamlet") match
        case None    => Left("the descriptor names no streamlet")
        case Some(j) => streamletFromJson(j)
    yield Descriptor(version, sdk, streamlet)

  /** The major of `MAJOR.MINOR` with both parts unsigned integers. */
  def majorOf(version: String): Option[Int] = version.split('.') match
    case Array(major, minor)
        if major.nonEmpty && minor.nonEmpty && (major + minor).forall(_.isDigit) =>
      major.toIntOption
    case _ => None

  private def sdkFromJson(j: Json): Either[String, SdkInfo] =
    for
      _ <- asObj(j, "sdk")
      n <- optStr(j, "name")
      v <- optStr(j, "version")
    yield SdkInfo(n, v)

  def streamletFromJson(j: Json): Either[String, StreamletDescriptor] =
    for
      _        <- asObj(j, "streamlet")
      engineId <- optStr(j, "engine")
      engine <-
        if engineId.isEmpty then Left(NoEngine)
        else Engine.fromId(engineId).toRight(unknownEngine(engineId))
      name    <- optStr(j, "name")
      desc    <- optStr(j, "description")
      inlets  <- portsFromJson(j, "inlets")
      outlets <- portsFromJson(j, "outlets")
      params  <- arr(j, "config_parameters").flatMap(traverse(_)(paramFromJson))
      flink <- j.field("flink") match
        case None => Right(None)
        case Some(f) =>
          for
            _ <- asObj(f, "flink")
            c <- optStr(f, "entry_class")
            v <- optStr(f, "version")
          yield Some(FlinkJob(c, v))
    yield StreamletDescriptor(name, desc, engine, inlets, outlets, params, flink)

  private def portsFromJson(j: Json, key: String): Either[String, Vector[Port]] =
    arr(j, key).flatMap(traverse(_) { p =>
      for
        _    <- asObj(p, key)
        name <- optStr(p, "name")
        contract <- p.field("contract") match
          case None => Left(s"$key '$name' has no contract")
          case Some(c) =>
            for
              _  <- asObj(c, "contract")
              n  <- optStr(c, "name")
              fp <- optStr(c, "fingerprint")
            yield Contract(n, Option.when(fp.nonEmpty)(fp))
      yield Port(name, contract)
    })

  private def paramFromJson(p: Json): Either[String, Parameter] =
    for
      _     <- asObj(p, "config_parameters")
      key   <- optStr(p, "key")
      desc  <- optStr(p, "description")
      tname <- optStr(p, "type")
      kind <-
        if tname.isEmpty then Right(ParameterType.String)
        else ParameterType.fromWire(tname).toRight(s"unknown config parameter type '$tname'")
      default <- optStr(p, "default_value")
    yield Parameter(key, desc, kind, Option.when(default.nonEmpty)(default))

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

  private def traverse[A, B](xs: Vector[A])(f: A => Either[String, B]): Either[String, Vector[B]] =
    xs.foldLeft[Either[String, Vector[B]]](Right(Vector.empty)) { (acc, a) =>
      acc.flatMap(bs => f(a).map(bs :+ _))
    }

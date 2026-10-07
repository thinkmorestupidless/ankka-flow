package com.thinkmorestupidless.ankka.flow.cli

import java.nio.file.{Files, Path}

import com.thinkmorestupidless.ankka.flow.protocol.{Fingerprint, ProtocolVersion}

/** `flow init`'s request, what it refuses, and the tokens a template is rendered with. */
object Init:

  enum Language(val id: String):
    case Scala  extends Language("scala")
    case Python extends Language("python")

  object Language:
    def parse(s: String): Either[String, Language] =
      values
        .find(_.id == s.toLowerCase)
        .toRight(s"language '$s' is not one of ${values.map(_.id).mkString(", ")}")

  final case class Request(name: String, language: Language, dir: Path, packageName: Option[String])

  private val StreamletName = """[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?""".r
  private val ScalaPackage  = """[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)*""".r
  private val PythonModule  = """[a-z_][a-z0-9_]*""".r
  private val PythonKeywords = Set(
    "false",
    "none",
    "true",
    "and",
    "as",
    "assert",
    "async",
    "await",
    "break",
    "class",
    "continue",
    "def",
    "del",
    "elif",
    "else",
    "except",
    "finally",
    "for",
    "from",
    "global",
    "if",
    "import",
    "in",
    "is",
    "lambda",
    "nonlocal",
    "not",
    "or",
    "pass",
    "raise",
    "return",
    "try",
    "while",
    "with",
    "yield"
  )

  /** Every reason the request cannot be written as a working project; empty when it can. */
  def problems(r: Request): Vector[String] =
    val name =
      if !StreamletName.matches(r.name) then
        Vector(s"name '${r.name}' must be 1-63 of [a-z0-9-], not starting or ending with '-'")
      else if r.name.length > 40 then
        Vector(s"name '${r.name}' must be at most 40 characters: it becomes the pipeline id")
      else if !r.name.head.isLetter then
        Vector(
          s"name '${r.name}' must start with a letter: it becomes a class, a package and a module"
        )
      else Vector.empty
    val pkg = (r.language, r.packageName) match
      case (Language.Scala, Some(p)) if !ScalaPackage.matches(p) =>
        Vector(s"'$p' is not a Scala package: dotted lower-case identifiers, such as com.acme.flow")
      case (Language.Python, Some(p)) if !PythonModule.matches(p) || PythonKeywords(p) =>
        Vector(
          s"'$p' is not a Python module: a lower-case identifier that is not a keyword, such as acme_flow"
        )
      case _ => Vector.empty
    val dir =
      if Files.exists(r.dir) && (!Files.isDirectory(r.dir) || Files.list(r.dir).findAny.isPresent)
      then
        Vector(
          s"${r.dir} is not empty: flow init writes a new project into an empty or new directory"
        )
      else Vector.empty
    name ++ pkg ++ dir

  /** A version a registry holds: a release, not a build between releases. */
  def isRelease(version: String): Boolean = !version.contains("+") && !version.endsWith("SNAPSHOT")

  /** The tokens of research R2, for a request and the CLI's version. */
  def tokens(r: Request, version: String): Map[String, String] =
    val pkg =
      r.packageName.filter(_ => r.language == Language.Scala).getOrElse(r.name.replace("-", ""))
    Map(
      "name"         -> r.name,
      "class"        -> r.name.split('-').filter(_.nonEmpty).map(_.capitalize).mkString,
      "package"      -> pkg,
      "package_path" -> pkg.replace('.', '/'),
      "module" -> r.packageName
        .filter(_ => r.language == Language.Python)
        .getOrElse(r.name.replace('-', '_')),
      "fingerprint"  -> Fingerprint.fingerprint(s"${r.name}.v1"),
      "flow_version" -> version,
      "sdk_version"  -> (if isRelease(version) then version else "0.0.0"),
      // A Docker tag may not hold '+', which a version between releases does.
      "image_version"           -> version.replace('+', '-'),
      "protocol_version"        -> ProtocolVersion.Current.toString,
      "scala_version"           -> BuildInfo.scalaLtsVersion,
      "sbt_version"             -> BuildInfo.sbtVersion,
      "native_packager_version" -> BuildInfo.nativePackagerVersion
    )

package com.thinkmorestupidless.ankka.flow.sidecar

import java.nio.file.{Files, Path}

import scala.util.Try

/**
 * The merge sink's connection, read from a directory of one file per key: the Secret the operator
 * mounts at `/etc/flow/neo4j`, or a directory beside a compose file
 * (contracts/neo4j-merge-sink.md).
 */
final case class Neo4jSecret(uri: String, username: String, password: String, database: String):

  /** The message with the password, and any `user:password@` in a URI, taken out. */
  def redact(message: String): String =
    Option(message)
      .getOrElse("")
      .replace(password, "<redacted>")
      .replaceAll("""(?<=://)[^/@\s]+@""", "<redacted>@")

  override def toString: String = s"Neo4jSecret($uri, $username, <redacted>, $database)"

object Neo4jSecret:

  val DefaultDatabase = "neo4j"

  def read(dir: Path): Either[String, Neo4jSecret] =
    /** `Right(None)` when the file is absent or empty; `Left` when it is there but unreadable. */
    def file(key: String): Either[String, Option[String]] =
      val p = dir.resolve(key)
      if !Files.isRegularFile(p) then Right(None)
      else
        Try(Files.readString(p).trim).toEither.left
          .map(e =>
            s"cannot read '$key' in credentials directory $dir: ${e.getClass.getSimpleName}"
          )
          .map(Option(_).filter(_.nonEmpty))
    def required(key: String) =
      file(key).flatMap(_.toRight(s"credentials directory $dir has no '$key'"))
    for
      uri      <- required("uri")
      username <- required("username")
      password <- required("password")
      database <- file("database")
    yield Neo4jSecret(uri, username, password, database.getOrElse(DefaultDatabase))

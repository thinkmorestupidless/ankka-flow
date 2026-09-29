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
    def file(key: String): Option[String] =
      val p = dir.resolve(key)
      Option
        .when(Files.isRegularFile(p))(Try(Files.readString(p).trim).toOption)
        .flatten
        .filter(_.nonEmpty)
    def required(key: String) = file(key).toRight(s"credentials directory $dir has no '$key'")
    for
      uri      <- required("uri")
      username <- required("username")
      password <- required("password")
    yield Neo4jSecret(uri, username, password, file("database").getOrElse(DefaultDatabase))

package com.thinkmorestupidless.ankka.flow.pekko

import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.*
import scala.util.Try

import com.thinkmorestupidless.ankka.flow.descriptor.{DescriptorValidation, ParameterType}
import com.typesafe.config.ConfigFactory

/**
 * A declared setting of a streamlet, read in the logic with `ctx(parameter)`. Its default is kept
 * as the descriptor carries it, as text: `100`, `0.5`, `true`, `100 ms`, `1 MiB`.
 */
final class Parameter[A] private[pekko] (
    val key: String,
    val kind: ParameterType,
    val default: Option[String],
    val description: String,
    private[pekko] val parse: String => Either[String, A]
):
  override def toString: String = s"Parameter($key, ${DescriptorValidation.typeName(kind)})"

private[pekko] object Parameter:
  private def hocon(text: String) =
    ConfigFactory.parseString(s"v = \"${text.replace("\\", "\\\\").replace("\"", "\\\"")}\"")

  private def notA(kind: String, text: String) = s"'$text' is not a $kind"

  val string: String => Either[String, String] = Right(_)
  val int: String => Either[String, Int]       = t => t.trim.toIntOption.toRight(notA("integer", t))
  val double: String => Either[String, Double] = t =>
    t.trim.toDoubleOption.toRight(notA("double", t))
  val boolean: String => Either[String, Boolean] = t =>
    t.trim match
      case "true"  => Right(true)
      case "false" => Right(false)
      case _       => Left(notA("boolean", t))
  val duration: String => Either[String, FiniteDuration] = t =>
    Try(hocon(t).getDuration("v").toScala).toOption.toRight(notA("duration", t))
  val memorySize: String => Either[String, Long] = t =>
    Try(hocon(t).getMemorySize("v").toBytes).toOption.toRight(notA("memory size", t))

  /** A declaration, with its default checked against its type where it is written. */
  def declare[A](
      key: String,
      kind: ParameterType,
      default: Option[String],
      description: String,
      parse: String => Either[String, A]
  ): Parameter[A] =
    default.foreach(d =>
      parse(d).left.foreach(e => throw new IllegalArgumentException(s"parameter '$key': $e"))
    )
    new Parameter(key, kind, default, description, parse)

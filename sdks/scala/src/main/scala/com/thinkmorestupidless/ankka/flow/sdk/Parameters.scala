package com.thinkmorestupidless.ankka.flow.sdk

import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.*
import scala.util.Try

import ankka.flow.v1.discovery.ConfigType
import com.thinkmorestupidless.ankka.flow.protocol.{DescriptorValidation, Json}
import com.typesafe.config.ConfigFactory

/**
 * A declared configuration parameter. `config(parameter)` reads its value as `T`. The default is
 * kept as the protocol carries it, as text: `100`, `0.5`, `true`, `100 ms`, `1 MiB`.
 */
final class Parameter[T] private[sdk] (
    val key: String,
    val configType: ConfigType,
    val defaultValue: String,
    val description: String,
    private[sdk] val parse: String => Either[String, T]
):
  override def toString: String = s"Parameter($key, ${DescriptorValidation.typeName(configType)})"

object Parameter:
  private def hocon(text: String) =
    ConfigFactory.parseString(s"v = \"${text.replace("\"", "\\\"")}\"")

  private def notA(kind: String, text: String) = s"'$text' is not a $kind"

  private[sdk] val string: String => Either[String, String] = Right(_)
  private[sdk] val integer: String => Either[String, Long] = t =>
    t.trim.toLongOption.toRight(notA("integer", t))
  private[sdk] val double: String => Either[String, Double] = t =>
    t.trim.toDoubleOption.toRight(notA("double", t))
  private[sdk] val boolean: String => Either[String, Boolean] = t =>
    t.trim match
      case "true"  => Right(true)
      case "false" => Right(false)
      case _       => Left(notA("boolean", t))
  private[sdk] val duration: String => Either[String, FiniteDuration] = t =>
    Try(hocon(t).getDuration("v").toScala).toOption.toRight(notA("duration", t))
  private[sdk] val memorySize: String => Either[String, Long] = t =>
    Try(hocon(t).getMemorySize("v").toBytes).toOption.toRight(notA("memory size", t))

  /** A duration as the Python SDK renders one: whole milliseconds as `ms`, else microseconds. */
  private[sdk] def renderDuration(d: FiniteDuration): String =
    val micros = d.toMicros
    if micros % 1000 == 0 then s"${micros / 1000} ms" else s"$micros us"

/**
 * A streamlet's resolved configuration: the declared defaults until the sidecar's `Start` applies
 * the deployed values.
 */
final class Config private[sdk] (values: Map[String, Any]):

  /** The parameter's value, typed as declared. A parameter with no default and no value throws. */
  def apply[T](p: Parameter[T]): T =
    values.get(p.key) match
      case Some(v) => v.asInstanceOf[T]
      case None =>
        throw new NoSuchElementException(s"parameter '${p.key}' has no default and no value")

  /** The value of the parameter `key`, typed, when it has one. */
  def get(key: String): Option[Any] = values.get(key)

  override def toString: String = values.toVector.sortBy(_._1).mkString("Config(", ", ", ")")

object Config:

  /**
   * The defaults of `parameters` overlaid by `values`, each the text the protocol carries. A key no
   * parameter declares is refused, as is a value that is not of its parameter's type.
   */
  def resolve(parameters: Seq[Parameter[?]], values: Map[String, String]): Config =
    val byKey   = parameters.map(p => p.key -> p).toMap
    val unknown = (values.keySet -- byKey.keySet).toVector.sorted
    if unknown.nonEmpty then
      throw new IllegalArgumentException(s"no parameter declared for ${unknown.mkString(", ")}")
    val resolved = parameters.flatMap { p =>
      val text = values.get(p.key).orElse(Option.when(p.defaultValue.nonEmpty)(p.defaultValue))
      text.map(t =>
        p.key -> p
          .parse(t)
          .fold(e => throw new IllegalArgumentException(s"parameter '${p.key}': $e"), identity)
      )
    }
    new Config(resolved.toMap)

  /** `Start.config_json`'s object as the text of each value. */
  private[sdk] def fromJson(json: String): Map[String, String] =
    if json.isEmpty then Map.empty
    else
      Json.parse(json) match
        case Right(Json.Obj(fields)) =>
          fields.collect {
            case (k, Json.Str(s))  => k -> s
            case (k, Json.Num(n))  => k -> n.bigDecimal.stripTrailingZeros.toPlainString
            case (k, Json.Bool(b)) => k -> b.toString
          }.toMap
        case Right(other) =>
          throw new IllegalArgumentException(
            s"config_json is not an object: ${Json.compact(other)}"
          )
        case Left(error) => throw new IllegalArgumentException(s"config_json is not JSON: $error")

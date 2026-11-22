package com.thinkmorestupidless.ankka.flow.descriptor

/**
 * What an SDK writes from a streamlet's declaration (descriptor version 2.0): the only thing `flow`
 * and the operator know about a streamlet. `DESCRIPTOR.md` is its canonical JSON.
 */
final case class Descriptor(
    descriptorVersion: String,
    sdk: Option[SdkInfo],
    streamlet: StreamletDescriptor
)

object Descriptor:

  /** The version every SDK writes. */
  val Version = "2.0"

  /** The major version `flow` and the operator read. */
  val Major = 2

final case class SdkInfo(name: String, version: String)

final case class StreamletDescriptor(
    name: String,
    description: String,
    engine: Engine,
    inlets: Vector[Port],
    outlets: Vector[Port],
    parameters: Vector[Parameter],
    flink: Option[FlinkJob]
):
  def ports: Vector[Port] = inlets ++ outlets

/** The engine a streamlet is written against and run by. */
enum Engine(val id: String):
  case PekkoStreams extends Engine("pekko-streams")
  case Flink        extends Engine("flink")

object Engine:
  def fromId(id: String): Option[Engine] = values.find(_.id == id)

  /** For messages: `pekko-streams, flink`. */
  def known: String = values.map(_.id).mkString(", ")

final case class Port(name: String, contract: Contract)

final case class Parameter(
    key: String,
    description: String,
    kind: ParameterType,
    default: Option[String]
)

enum ParameterType(val wire: String):
  case String     extends ParameterType("STRING")
  case Integer    extends ParameterType("INTEGER")
  case Double     extends ParameterType("DOUBLE")
  case Boolean    extends ParameterType("BOOLEAN")
  case Duration   extends ParameterType("DURATION")
  case MemorySize extends ParameterType("MEMORY_SIZE")

object ParameterType:
  def fromWire(name: java.lang.String): Option[ParameterType] = values.find(_.wire == name)

/** How a Flink streamlet's job starts: the class whose `main` runs it, and Flink's version. */
final case class FlinkJob(entryClass: String, version: String)

object FlinkJob:

  /** The Flink versions the operator can run, as `MAJOR.MINOR`. */
  val Versions: Vector[String] = Vector("2.2")

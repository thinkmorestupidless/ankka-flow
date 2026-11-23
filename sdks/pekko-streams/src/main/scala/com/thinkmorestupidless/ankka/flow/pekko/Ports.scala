package com.thinkmorestupidless.ankka.flow.pekko

import scala.util.Using
import scala.util.control.NonFatal

import com.thinkmorestupidless.ankka.flow.descriptor.Contract as Declared

/**
 * What a port carries, by name, as ankka names a contract. With a schema (a classpath resource,
 * usually a JSON Schema) the descriptor also carries the schema's fingerprint, so two ports stating
 * different schemas of one name do not connect.
 */
final class Contract private (val name: String, val schema: Option[String]):
  override def toString: String = schema.fold(s"Contract($name)")(s => s"Contract($name, $s)")

  /** The descriptor's contract: the name and, with a schema, its fingerprint. */
  private[pekko] def declared(loader: ClassLoader): Either[String, Declared] =
    schema match
      case None => Right(Declared(name, None))
      case Some(resource) =>
        Option(loader.getResourceAsStream(resource)) match
          case None => Left(s"contract '$name': schema '$resource' is not on the classpath")
          case Some(in) =>
            try
              val bytes = Using.resource(in)(_.readAllBytes())
              Declared
                .fingerprint(bytes)
                .left
                .map(e => s"contract '$name': schema '$resource' $e")
                .map(fp => Declared(name, Some(fp)))
            catch
              case NonFatal(e) =>
                Left(s"contract '$name': schema '$resource' could not be read: ${e.getMessage}")

object Contract:
  def apply(name: String): Contract                 = new Contract(name, None)
  def apply(name: String, schema: String): Contract = new Contract(name, Some(schema))

/** A port a streamlet reads. */
final class Inlet[A] private[pekko] (val name: String, val contract: Contract, val codec: Codec[A]):
  override def toString: String = s"Inlet($name, $contract)"

/** A port a streamlet writes. `out(key, value)` is one record for it. */
final class Outlet[A] private[pekko] (
    val name: String,
    val contract: Contract,
    val codec: Codec[A]
):
  def apply(key: String, value: A): Emit = Emit(this, Record(Some(key), value))
  def apply(record: Record[A]): Emit     = Emit(this, record)

  override def toString: String = s"Outlet($name, $contract)"

/**
 * One record for one outlet. The logic returns these; the SDK writes them and commits the input
 * they came from once every one is confirmed.
 */
final class Emit private[pekko] (val outlet: Outlet[?], val record: Record[?]):

  /** The record's value as its outlet's codec writes it. */
  private[pekko] def bytes: Array[Byte] =
    outlet.asInstanceOf[Outlet[Any]].codec.encode(record.value)

  override def toString: String = s"Emit(${outlet.name}, $record)"

  override def equals(other: Any): Boolean = other match
    case e: Emit => (e.outlet eq outlet) && e.record == record
    case _       => false

  override def hashCode: Int = (outlet.name, record).##

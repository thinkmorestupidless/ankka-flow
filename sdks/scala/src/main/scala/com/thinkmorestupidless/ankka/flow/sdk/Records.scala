package com.thinkmorestupidless.ankka.flow.sdk

import java.nio.charset.StandardCharsets.UTF_8
import java.util.Arrays

/**
 * A Kafka record as a streamlet sees it: value bytes, an optional key, ordered headers. Nothing is
 * decoded. Equality compares the bytes, so two records holding the same bytes are equal.
 */
final case class Record(
    value: Array[Byte],
    key: Option[Array[Byte]] = None,
    headers: Seq[(String, Array[Byte])] = Nil,
    offset: Long = -1,
    timestampMs: Long = 0
):
  /** The value as UTF-8 text: a convenience for tests and logs, not a decoding the SDK applies. */
  def valueString: String = new String(value, UTF_8)

  /** The key as UTF-8 text, when there is one. */
  def keyString: Option[String] = key.map(new String(_, UTF_8))

  override def equals(other: Any): Boolean = other match
    case r: Record =>
      Arrays.equals(value, r.value) &&
      key.map(_.toSeq) == r.key.map(_.toSeq) &&
      headers.map((k, v) => (k, v.toSeq)) == r.headers.map((k, v) => (k, v.toSeq)) &&
      offset == r.offset && timestampMs == r.timestampMs
    case _ => false

  override def hashCode: Int =
    (Arrays.hashCode(value), key.map(Arrays.hashCode), headers.map(_._1), offset).##

  override def toString: String =
    s"Record(key=${keyString.getOrElse("-")}, value=${valueString}, headers=${headers.map(_._1).mkString(",")}, offset=$offset)"

/** The records of one inlet partition, in offset order: one call of `process`. */
final case class Batch(inlet: String, partition: Int, records: Vector[Record])
    extends Iterable[Record]:
  def iterator: Iterator[Record] = records.iterator
  override def toString: String  = s"Batch($inlet/$partition, ${records.size} records)"

/** One record to one declared outlet. */
final case class Emit(outlet: String, record: Record)

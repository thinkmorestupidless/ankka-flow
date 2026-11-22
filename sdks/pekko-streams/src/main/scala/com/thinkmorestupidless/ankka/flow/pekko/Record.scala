package com.thinkmorestupidless.ankka.flow.pekko

/** What a port carries: a key, a value of the port's type, and the record's headers. */
final case class Record[A](
    key: Option[String],
    value: A,
    headers: Seq[(String, Array[Byte])] = Nil
):
  /** The same key and headers with another value. */
  def withValue[B](b: B): Record[B] = Record(key, b, headers)

  override def equals(other: Any): Boolean = other match
    case r: Record[?] =>
      key == r.key && valueEquals(r.value) &&
      headers.size == r.headers.size &&
      headers.zip(r.headers).forall((a, b) => a._1 == b._1 && a._2.sameElements(b._2))
    case _ => false

  override def hashCode: Int = (key, valueHash, headers.map(_._1)).##

  private def valueEquals(other: Any): Boolean = (value, other) match
    case (a: Array[Byte], b: Array[Byte]) => a.sameElements(b)
    case (a, b)                           => a == b

  private def valueHash: Int = value match
    case a: Array[Byte] => java.util.Arrays.hashCode(a)
    case v              => v.##

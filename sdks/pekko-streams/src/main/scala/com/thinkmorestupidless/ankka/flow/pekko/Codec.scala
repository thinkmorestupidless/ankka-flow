package com.thinkmorestupidless.ankka.flow.pekko

import java.nio.charset.StandardCharsets.UTF_8

import com.github.plokhotnyuk.jsoniter_scala.core.{JsonValueCodec, readFromArray, writeToArray}

/**
 * Bytes to a value and back. A record whose bytes do not decode fails the stream: never skipped. An
 * author who would rather drop such records reads `Codec.bytes` and decodes in the logic.
 */
trait Codec[A]:
  def encode(value: A): Array[Byte]
  def decode(bytes: Array[Byte]): A

object Codec:

  /** JSON through jsoniter-scala: derive the codec with `JsonCodecMaker.make`. */
  def json[A](using c: JsonValueCodec[A]): Codec[A] = new Codec[A]:
    def encode(value: A): Array[Byte] = writeToArray(value)
    def decode(bytes: Array[Byte]): A = readFromArray(bytes)

  /** The record's bytes as they are. */
  val bytes: Codec[Array[Byte]] = new Codec[Array[Byte]]:
    def encode(value: Array[Byte]): Array[Byte] = value
    def decode(bytes: Array[Byte]): Array[Byte] = bytes

  /** UTF-8 text. */
  val string: Codec[String] = new Codec[String]:
    def encode(value: String): Array[Byte] = value.getBytes(UTF_8)
    def decode(bytes: Array[Byte]): String = new String(bytes, UTF_8)

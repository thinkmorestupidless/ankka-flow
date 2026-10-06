package com.thinkmorestupidless.ankka.flow.sdk.conformance

import java.nio.charset.StandardCharsets.UTF_8

import com.thinkmorestupidless.ankka.flow.sdk.*

/**
 * The reference streamlet every SDK ships for the conformance suite: the `conformance` declaration
 * of protocol/fixtures/declarations, behaving by each record's key.
 */
final class Conformance extends Streamlet("conformance", "The conformance reference streamlet."):
  val in    = inlet("in", schemaName = "conformance.v1")
  val side  = inlet("side", schemaName = "conformance-side.v1")
  val out   = outlet("out", schemaName = "conformance.v1")
  val other = outlet("other", schemaName = "conformance-other.v1")
  val mode = parameter.string(
    "mode",
    default = "echo",
    description = "Unused by the suite; proves a string parameter arrives."
  )
  val factor =
    parameter.integer("factor", default = 1, description = "How many times the multiply key emits.")

  def process(batch: Batch): Iterable[Emit] =
    batch.records.flatMap { record =>
      record.keyString.getOrElse("") match
        case "echo" => Vector(out.emit(record))
        case "fan"  => Vector(out.emit(record), other.emit(record))
        case "skip" => Vector.empty
        case "fail" => throw new RuntimeException("the record said fail")
        case "late" =>
          Thread.sleep(300)
          Vector(out.emit(record))
        case "rogue-outlet" => Vector(Emit("nope", record))
        case "multiply" =>
          (1L to config(factor)).map(i =>
            out.emit(record.copy(headers = record.headers :+ ("n" -> i.toString.getBytes(UTF_8))))
          )
        case "unkeyed"     => Vector(out.emit(record.copy(key = None)))
        case "header-echo" => Vector(out.emit(record.copy(headers = record.headers.reverse)))
        case _             => Vector(out.emit(record))
    }

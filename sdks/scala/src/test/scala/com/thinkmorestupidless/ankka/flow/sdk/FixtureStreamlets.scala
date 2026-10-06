package com.thinkmorestupidless.ankka.flow.sdk

import scala.concurrent.duration.*

import com.thinkmorestupidless.ankka.flow.sdk.conformance.Conformance

/** The six declarations of protocol/fixtures/declarations, in the Scala SDK. */
object FixtureStreamlets:

  abstract class NoOp(name: String, description: String = "") extends Streamlet(name, description):
    def process(batch: Batch): Iterable[Emit] = Nil

  final class Minimal extends NoOp("minimal"):
    val in  = inlet("in", schemaName = "minimal.v1")
    val out = outlet("out", schemaName = "minimal.v1")

  final class CartRouter
      extends NoOp("cart-router", "Routes cart events to the valid or review outlet."):
    val in     = inlet("in", schemaName = "cart-events.v1")
    val valid  = outlet("valid", schemaName = "cart-events.v1")
    val review = outlet("review", schemaName = "cart-events.v1")
    val threshold = parameter.integer(
      "review-threshold",
      default = 100,
      description = "Carts with a total above this go to the review outlet."
    )

  final class EveryType extends NoOp("every-type", "Every parameter type, and \"unicode\": café ✓"):
    val in        = inlet("in", schemaName = "every.v1")
    val aString   = parameter.string("a-string", default = "hello", description = "A string.")
    val anInteger = parameter.integer("an-integer", default = 42, description = "An integer.")
    val aDouble   = parameter.double("a-double", default = 0.5, description = "A double.")
    val aBoolean  = parameter.boolean("a-boolean", default = true, description = "A boolean.")
    val aDuration =
      parameter.duration("a-duration", default = 100.millis, description = "A duration.")
    val aMemorySize =
      parameter.memorySize("a-memory-size", default = "1 MiB", description = "A memory size.")
    val required = parameter.string(
      "required",
      description = "Required: no default, so it must be set at deploy time."
    )

  final class ManyPorts extends NoOp("many-ports"):
    val inlets5 = Seq("in-e", "in-c", "in-a", "in-d", "in-b").map(inlet(_, schemaName = "many.v1"))
    val outlets5 =
      Seq("out-3", "out-1", "out-5", "out-2", "out-4").map(outlet(_, schemaName = "many.v1"))

  final class Sink extends NoOp("sink", "Consumes cart events and writes them elsewhere."):
    val in = inlet("in", schemaName = "cart-events.v1")

  val all: Vector[(String, () => Streamlet)] = Vector(
    "minimal"     -> (() => new Minimal),
    "cart-router" -> (() => new CartRouter),
    "every-type"  -> (() => new EveryType),
    "many-ports"  -> (() => new ManyPorts),
    "sink"        -> (() => new Sink),
    "conformance" -> (() => new Conformance)
  )

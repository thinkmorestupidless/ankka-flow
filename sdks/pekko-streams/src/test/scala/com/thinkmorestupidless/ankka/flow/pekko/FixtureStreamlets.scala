package com.thinkmorestupidless.ankka.flow.pekko

/** T010's four `pekko-streams` fixtures, declared in this SDK's API. */
object FixtureStreamlets:

  /** A streamlet whose logic does nothing: the fixtures are about the declaration. */
  abstract class Declared(name: String, description: String = "")
      extends PekkoStreamlet(name, description):
    def logic(ctx: Context): Logic = Logic.empty

  final class Minimal extends Declared("minimal"):
    val in  = inlet("in", Contract("minimal.v1"), Codec.bytes)
    val out = outlet("out", Contract("minimal.v1"), Codec.bytes)

  final class Sink extends Declared("sink", "Consumes cart events and writes them elsewhere."):
    val in = inlet("in", Contract("cart-events.v1"), Codec.bytes)

  final class ManyPorts extends Declared("many-ports"):
    val ins =
      Vector("in-e", "in-c", "in-a", "in-d", "in-b").map(inlet(_, Contract("many.v1"), Codec.bytes))
    val outs =
      Vector("out-3", "out-1", "out-5", "out-2", "out-4").map(
        outlet(_, Contract("many.v1"), Codec.bytes)
      )

  final class CartCleaner
      extends Declared(
        "cart-cleaner",
        "Drops cart events that do not decode or whose total is above the maximum, and writes the rest clean."
      ):
    val in  = inlet("in", Contract("cart-events.v1", schema = "cart-events.v1.json"), Codec.bytes)
    val out = outlet("out", Contract("clean-carts.v1"), Codec.bytes)
    val maxTotal =
      intParameter(
        "max-total",
        default = 10000,
        description = "Events whose total is above this are dropped."
      )

  val all: Vector[(String, () => PekkoStreamlet)] = Vector(
    "minimal"      -> (() => new Minimal),
    "sink"         -> (() => new Sink),
    "many-ports"   -> (() => new ManyPorts),
    "cart-cleaner" -> (() => new CartCleaner)
  )

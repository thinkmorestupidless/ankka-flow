package com.thinkmorestupidless.ankka.flow.descriptor

import java.nio.file.{Files, Path, Paths}

/** The six fixture declarations of descriptor/fixtures/declarations, built with the model. */
object Fixtures:

  val repoRoot: Path =
    Paths.get(sys.props.getOrElse("flow.repo.root", ".")).toAbsolutePath.normalize

  val dir: Path = repoRoot.resolve("descriptor/fixtures")

  def read(p: Path): String = new String(Files.readAllBytes(p), "UTF-8")

  private def port(name: String, contract: String) = Port(name, Contract(contract, None))

  private def schemaPort(name: String, contract: String, schema: String) =
    val bytes = Files.readAllBytes(repoRoot.resolve(schema))
    Port(
      name,
      Contract(contract, Some(Contract.fingerprint(bytes).fold(e => sys.error(e), identity)))
    )

  private def param(key: String, t: ParameterType, default: String, desc: String) =
    Parameter(key, desc, t, Option.when(default.nonEmpty)(default))

  private def pekko(
      name: String,
      description: String = "",
      inlets: Vector[Port],
      outlets: Vector[Port] = Vector.empty,
      parameters: Vector[Parameter] = Vector.empty
  ) = descriptor(
    StreamletDescriptor(name, description, Engine.PekkoStreams, inlets, outlets, parameters, None)
  )

  private def descriptor(s: StreamletDescriptor) =
    Descriptor(Descriptor.Version, Some(SdkInfo("fixture", "0.0.0")), s)

  val all: Vector[(String, Descriptor)] = Vector(
    "minimal" -> pekko(
      "minimal",
      inlets = Vector(port("in", "minimal.v1")),
      outlets = Vector(port("out", "minimal.v1"))
    ),
    "sink" -> pekko(
      "sink",
      "Consumes cart events and writes them elsewhere.",
      inlets = Vector(port("in", "cart-events.v1"))
    ),
    "every-type" -> descriptor(
      StreamletDescriptor(
        "every-type",
        "Every parameter type, and \"unicode\": café ✓",
        Engine.Flink,
        Vector(port("in", "every.v1")),
        Vector.empty,
        Vector(
          param("a-string", ParameterType.String, "hello", "A string."),
          param("an-integer", ParameterType.Integer, "42", "An integer."),
          param("a-double", ParameterType.Double, "0.5", "A double."),
          param("a-boolean", ParameterType.Boolean, "true", "A boolean."),
          param("a-duration", ParameterType.Duration, "100 ms", "A duration."),
          param("a-memory-size", ParameterType.MemorySize, "1 MiB", "A memory size."),
          param(
            "required",
            ParameterType.String,
            "",
            "Required: no default, so it must be set at deploy time."
          )
        ),
        Some(FlinkJob("fixture.EveryType", "2.2"))
      )
    ),
    "many-ports" -> pekko(
      "many-ports",
      inlets = Vector("in-e", "in-c", "in-a", "in-d", "in-b").map(port(_, "many.v1")),
      outlets = Vector("out-3", "out-1", "out-5", "out-2", "out-4").map(port(_, "many.v1"))
    ),
    "cart-cleaner" -> pekko(
      "cart-cleaner",
      "Drops cart events that do not decode or whose total is above the maximum, and writes the rest clean.",
      inlets = Vector(
        schemaPort("in", "cart-events.v1", "samples/cart-pipeline/schemas/cart-events.v1.json")
      ),
      outlets = Vector(port("out", "clean-carts.v1")),
      parameters = Vector(
        param(
          "max-total",
          ParameterType.Integer,
          "10000",
          "Events whose total is above this are dropped."
        )
      )
    ),
    "cart-counter" -> descriptor(
      StreamletDescriptor(
        "cart-counter",
        "Counts each cart's events in windows of the time they carry.",
        Engine.Flink,
        Vector(port("in", "clean-carts.v1")),
        Vector(port("out", "cart-counts.v1")),
        Vector(
          param("window", ParameterType.Duration, "1m", "The length of each counting window.")
        ),
        Some(FlinkJob("cart.counter.CartCounter", "2.2"))
      )
    )
  )

  def byName(name: String): Descriptor = all.find(_._1 == name).map(_._2).get

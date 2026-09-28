package com.thinkmorestupidless.ankka.flow.protocol

import java.nio.file.{Files, Path, Paths}

import ankka.flow.v1.discovery.*

/** The six fixture declarations of protocol/fixtures/declarations, built in Scala. */
object Fixtures:

  val repoRoot: Path =
    Paths.get(sys.props.getOrElse("flow.repo.root", ".")).toAbsolutePath.normalize

  val dir: Path = repoRoot.resolve("protocol/fixtures")

  def read(p: Path): String = new String(Files.readAllBytes(p), "UTF-8")

  private def json(name: String, schema: String) =
    Port(name, Some(Contract(Fingerprint.Format, schema, Fingerprint.fingerprint(schema))))

  private def param(key: String, t: ConfigType, default: String, desc: String) =
    ConfigParameter(key, desc, t, default)

  private def spec(d: StreamletDescriptor) =
    Spec(ProtocolVersion.Current.toString, Some(SdkInfo("fixture", "0.0.0")), Some(d))

  val all: Vector[(String, Spec)] = Vector(
    "minimal" -> spec(
      StreamletDescriptor(
        "minimal",
        "",
        Seq(json("in", "minimal.v1")),
        Seq(json("out", "minimal.v1"))
      )
    ),
    "cart-router" -> spec(
      StreamletDescriptor(
        "cart-router",
        "Routes cart events to the valid or review outlet.",
        Seq(json("in", "cart-events.v1")),
        Seq(json("valid", "cart-events.v1"), json("review", "cart-events.v1")),
        Seq(
          param(
            "review-threshold",
            ConfigType.INTEGER,
            "100",
            "Carts with a total above this go to the review outlet."
          )
        )
      )
    ),
    "every-type" -> spec(
      StreamletDescriptor(
        "every-type",
        "Every parameter type, and \"unicode\": café ✓",
        Seq(json("in", "every.v1")),
        Seq.empty,
        Seq(
          param("a-string", ConfigType.STRING, "hello", "A string."),
          param("an-integer", ConfigType.INTEGER, "42", "An integer."),
          param("a-double", ConfigType.DOUBLE, "0.5", "A double."),
          param("a-boolean", ConfigType.BOOLEAN, "true", "A boolean."),
          param("a-duration", ConfigType.DURATION, "100 ms", "A duration."),
          param("a-memory-size", ConfigType.MEMORY_SIZE, "1 MiB", "A memory size."),
          param(
            "required",
            ConfigType.STRING,
            "",
            "Required: no default, so it must be set at deploy time."
          )
        )
      )
    ),
    "many-ports" -> spec(
      StreamletDescriptor(
        "many-ports",
        "",
        Seq("in-e", "in-c", "in-a", "in-d", "in-b").map(json(_, "many.v1")),
        Seq("out-3", "out-1", "out-5", "out-2", "out-4").map(json(_, "many.v1"))
      )
    ),
    "sink" -> spec(
      StreamletDescriptor(
        "sink",
        "Consumes cart events and writes them elsewhere.",
        Seq(json("in", "cart-events.v1"))
      )
    ),
    "conformance" -> spec(
      StreamletDescriptor(
        "conformance",
        "The conformance reference streamlet.",
        Seq(json("in", "conformance.v1"), json("side", "conformance-side.v1")),
        Seq(json("out", "conformance.v1"), json("other", "conformance-other.v1")),
        Seq(
          param(
            "mode",
            ConfigType.STRING,
            "echo",
            "Unused by the suite; proves a string parameter arrives."
          ),
          param("factor", ConfigType.INTEGER, "1", "How many times the multiply key emits.")
        )
      )
    )
  )

  def byName(name: String): Spec = all.find(_._1 == name).map(_._2).get

package com.thinkmorestupidless.ankka.flow.cli

import java.io.{ByteArrayOutputStream, PrintStream}
import java.nio.file.{Files, Path, Paths}

import scala.jdk.CollectionConverters.*

/** The cart pipeline under test resources, and a way to run `flow` against a variant of it. */
object CliFixtures:

  val cart: Path =
    Paths.get(getClass.getResource("/blueprints/cart/blueprint.conf").toURI).getParent

  val repoRoot: Path =
    Paths.get(sys.props.getOrElse("flow.repo.root", ".")).toAbsolutePath.normalize

  /** A pipeline with a built-in streamlet: a mapper in front of the Neo4j merge sink. */
  val graph: Path =
    Paths.get(getClass.getResource("/blueprints/graph/blueprint.conf").toURI).getParent

  /** A copy of the graph pipeline in a temporary directory, with edits applied. */
  def graphVariant(
      blueprint: String => String = identity,
      mapper: String => String = identity
  ): Path =
    val dir = Files.createTempDirectory("graph")
    Files.createDirectories(dir.resolve("descriptors"))
    Files.writeString(
      dir.resolve("blueprint.conf"),
      blueprint(Files.readString(graph.resolve("blueprint.conf")))
    )
    Files.writeString(
      dir.resolve("descriptors/mapper.json"),
      mapper(Files.readString(graph.resolve("descriptors/mapper.json")))
    )
    dir

  final case class Result(code: Int, out: String, err: String):
    def lines: Vector[String] = err.linesIterator.toVector.filter(_.nonEmpty)

  def flow(args: String*): Result =
    val out = new ByteArrayOutputStream
    val err = new ByteArrayOutputStream
    val code = Main.run(
      args.toList,
      new PrintStream(out, true, "UTF-8"),
      new PrintStream(err, true, "UTF-8")
    )
    Result(code, out.toString("UTF-8"), err.toString("UTF-8"))

  /** A copy of the cart pipeline in a temporary directory, with edits applied. */
  def variant(
      blueprint: String => String = identity,
      descriptors: Map[String, String => String] = Map.empty,
      extraDescriptors: Map[String, String] = Map.empty
  ): Path =
    val dir = Files.createTempDirectory("cart")
    Files.createDirectories(dir.resolve("descriptors"))
    Files.writeString(
      dir.resolve("blueprint.conf"),
      blueprint(Files.readString(cart.resolve("blueprint.conf")))
    )
    Files.list(cart.resolve("descriptors")).iterator.asScala.foreach { f =>
      val name = f.getFileName.toString
      Files.writeString(
        dir.resolve(s"descriptors/$name"),
        descriptors.get(name).fold(Files.readString(f))(_(Files.readString(f)))
      )
    }
    extraDescriptors.foreach((n, t) => Files.writeString(dir.resolve(s"descriptors/$n"), t))
    Files.copy(cart.resolve("images.conf"), dir.resolve("images.conf"))
    dir

  def verify(dir: Path, extra: String*): Result =
    flow(
      Seq(
        "verify",
        dir.resolve("blueprint.conf").toString,
        "--descriptors",
        dir.resolve("descriptors").toString
      ) ++ extra*
    )

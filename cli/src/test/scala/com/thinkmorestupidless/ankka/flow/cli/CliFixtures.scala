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

  /**
   * How a case runs `flow`: in this JVM, or as the native binary `-Dflow.cli.binary` names. The
   * same cases, the same arguments, the same assertions; what differs is the process. A kubeconfig
   * reaches the in-process run as the `kubeconfig` system property and the binary as `KUBECONFIG`,
   * which are the two places fabric8's configuration looks.
   */
  sealed trait Driver:
    def run(args: List[String], kubeconfig: Option[Path]): Result

  object Driver:
    case object InProcess extends Driver:
      def run(args: List[String], kubeconfig: Option[Path]): Result =
        val out    = new ByteArrayOutputStream
        val err    = new ByteArrayOutputStream
        val before = Option(System.getProperty("kubeconfig"))
        kubeconfig.foreach(k => System.setProperty("kubeconfig", k.toString))
        try
          val code = Main.run(
            args,
            new PrintStream(out, true, "UTF-8"),
            new PrintStream(err, true, "UTF-8")
          )
          Result(code, out.toString("UTF-8"), err.toString("UTF-8"))
        finally
          before match
            case Some(b) => System.setProperty("kubeconfig", b): Unit
            case None    => System.clearProperty("kubeconfig"): Unit

    final case class Binary(path: Path) extends Driver:
      def run(args: List[String], kubeconfig: Option[Path]): Result =
        val builder = new ProcessBuilder((path.toString +: args).asJava)
        builder.directory(repoRoot.toFile)
        // The binary reads only what a user's shell gives it; nothing of this JVM's leaks in.
        builder.environment().remove("KUBECONFIG")
        kubeconfig.foreach(k => builder.environment().put("KUBECONFIG", k.toString))
        val process = builder.start()
        val out     = process.getInputStream.readAllBytes()
        val err     = process.getErrorStream.readAllBytes()
        if !process.waitFor(60, java.util.concurrent.TimeUnit.SECONDS) then
          process.destroyForcibly()
          throw IllegalStateException(s"$path ${args.mkString(" ")} did not finish in 60 seconds")
        Result(process.exitValue(), String(out, "UTF-8"), String(err, "UTF-8"))

    /** The driver the switch selects. A path that is not a file is refused, never ignored. */
    def from(switch: Option[String]): Driver = switch match
      case None => InProcess
      case Some(named) =>
        val path = Paths.get(named).toAbsolutePath
        if Files.isRegularFile(path) && Files.isExecutable(path) then Binary(path)
        else
          throw IllegalArgumentException(
            s"-Dflow.cli.binary=$named names no executable file; the suite will not fall back " +
              "to running in process"
          )

  lazy val driver: Driver = Driver.from(sys.props.get("flow.cli.binary"))

  def flow(args: String*): Result = driver.run(args.toList, None)

  /** `flow` with a kubeconfig, for the cases that reach a cluster. */
  def flowWith(kubeconfig: Path, args: String*): Result = driver.run(args.toList, Some(kubeconfig))

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

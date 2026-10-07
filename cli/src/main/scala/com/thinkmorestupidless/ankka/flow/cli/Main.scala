package com.thinkmorestupidless.ankka.flow.cli

import java.io.PrintStream
import java.nio.file.{Files, Path}

import scala.sys.process.{Process, ProcessLogger}
import scala.util.Try

import cats.syntax.all.*
import com.monovore.decline.*
import com.thinkmorestupidless.ankka.flow.blueprint.{
  BlueprintProblem,
  BuiltinHasImage,
  MissingImage
}
import com.thinkmorestupidless.ankka.flow.crd.{FlowSerialization, OnDelete}
import com.thinkmorestupidless.ankka.flow.protocol.ProtocolVersion

/**
 * `flow`: verify a blueprint, generate the resource the operator runs, request a reset. Exit codes:
 * 0 ok, 1 refused or failed (problems on stderr, one per line), 2 usage.
 */
object Main:

  def main(args: Array[String]): Unit = sys.exit(run(args.toList, System.out, System.err))

  private enum Cmd:
    case VerifyCmd(in: Verify.Inputs)
    case GenerateCmd(
        in: Verify.Inputs,
        images: Option[Path],
        image: List[String],
        pipeline: Option[String],
        version: Option[String],
        namespace: Option[String],
        out: Option[Path],
        deleteManagedTopics: Boolean
    )
    case ResetCmd(pipeline: String, streamlets: List[String], namespace: Option[String])
    case VersionCmd
    case InitCmd(
        name: String,
        language: Init.Language,
        dir: Option[Path],
        packageName: Option[String]
    )

  private val inputs: Opts[Verify.Inputs] =
    (
      Opts.argument[Path]("blueprint.conf"),
      Opts
        .option[Path](
          "descriptors",
          "Directory of descriptor files (*.json); optional when every streamlet is built in."
        )
        .orNone,
      Opts.options[Path]("conf", "Deploy-time configuration (HOCON); later files win.").orEmpty
    ).mapN(Verify.Inputs.apply)

  private val verify =
    Opts.subcommand("verify", "Verify a blueprint against streamlet descriptors.")(
      inputs.map(Cmd.VerifyCmd(_))
    )

  private val generate = Opts.subcommand("generate", "Verify, then write the AnkkaFlow resource.")(
    (
      inputs,
      Opts.option[Path]("images", "HOCON map of streamlet name to image.").orNone,
      Opts.options[String]("image", "name=image, repeatable.").orEmpty,
      Opts
        .option[String]("pipeline", "Pipeline id (default: blueprint.name, else the file name).")
        .orNone,
      Opts.option[String]("version", "Pipeline version (default: git describe).").orNone,
      Opts.option[String]("namespace", "Namespace for the resource.", "n").orNone,
      Opts.option[Path]("output", "Write here instead of stdout.", "o").orNone,
      Opts
        .flag(
          "delete-managed-topics",
          "Delete the topics the pipeline created when its resource is deleted (default: keep them)."
        )
        .orFalse
    ).mapN(Cmd.GenerateCmd.apply)
  )

  private val reset = Opts.subcommand(
    "reset",
    "Request that a pipeline's streamlets reread their inputs from the start."
  )(
    (
      Opts.argument[String]("pipeline"),
      Opts
        .options[String](
          "streamlet",
          "Streamlet to reset, repeatable (default: every one with an inlet)."
        )
        .orEmpty,
      Opts.option[String]("namespace", "Namespace of the pipeline.", "n").orNone
    ).mapN(Cmd.ResetCmd.apply)
  )

  private val version =
    Opts.subcommand("version", "Print the CLI and protocol versions.")(Opts(Cmd.VersionCmd))

  private val init = Opts.subcommand("init", "Write a new streamlet project, in Scala or Python.")(
    (
      Opts.argument[String]("name"),
      Opts
        .option[String]("language", "scala (the default) or python.", "l")
        .withDefault("scala")
        .mapValidated(s => Init.Language.parse(s).toValidatedNel),
      Opts.option[Path]("dir", "Where to write the project (default: the name).").orNone,
      Opts
        .option[String](
          "package",
          "The Scala package or the Python module (default: from the name)."
        )
        .orNone
    ).mapN(Cmd.InitCmd.apply)
  )

  private val command = Command("flow", "Verify, generate and operate ankka-flow pipelines.")(
    verify.orElse(generate).orElse(reset).orElse(init).orElse(version)
  )

  def run(args: List[String], out: PrintStream, err: PrintStream): Int =
    command.parse(args, sys.env) match
      case Left(help) =>
        (if help.errors.isEmpty then out else err).println(help)
        if help.errors.isEmpty then 0 else 2
      case Right(cmd) =>
        cmd match
          case Cmd.VersionCmd =>
            out.println(s"flow ${BuildInfo.version}, protocol ${ProtocolVersion.Current}")
            0
          case Cmd.VerifyCmd(in) =>
            Verify.run(in) match
              case Left(problems) => refuse(err, problems)
              case Right(v) =>
                v.notes.foreach(err.println)
                out.println(
                  s"verified: ${v.blueprint.streamlets.size} streamlets, ${v.blueprint.topics.size} topics"
                )
                0
          case g: Cmd.GenerateCmd => generateResource(g, out, err)
          case i: Cmd.InitCmd     => initProject(i, out, err)
          case Cmd.ResetCmd(pipeline, streamlets, namespace) =>
            Reset.kubernetes.request(pipeline, streamlets, namespace) match
              case Left(problems) => refuse(err, problems)
              case Right(id) =>
                out.println(s"reset requested for '$pipeline': $id")
                0

  private def initProject(i: Cmd.InitCmd, out: PrintStream, err: PrintStream): Int =
    val request  = Init.Request(i.name, i.language, i.dir.getOrElse(Path.of(i.name)), i.packageName)
    val problems = Init.problems(request)
    if problems.nonEmpty then
      problems.foreach(err.println)
      2
    else
      Scaffold.write(
        request.dir,
        Scaffold.files(i.language, Init.tokens(request, BuildInfo.version))
      ) match
        case Left(left) =>
          left.foreach(err.println)
          1
        case Right(written) =>
          val (test, check) = i.language match
            case Init.Language.Scala  => ("sbt test", "sbt descriptorCheck")
            case Init.Language.Python => ("uv run pytest -q", "uv run descriptor --check")
          out.println(s"wrote ${written.size} files to ${request.dir}")
          out.println(s"  cd ${request.dir}")
          out.println(s"  $test")
          out.println(s"  $check")
          out.println("  flow verify blueprint.conf --descriptors flow")
          0

  private def refuse(err: PrintStream, problems: Vector[String]): Int =
    problems.foreach(err.println)
    1

  private val PipelineName = """[a-z0-9]([-a-z0-9]*[a-z0-9])?""".r

  private def generateResource(g: Cmd.GenerateCmd, out: PrintStream, err: PrintStream): Int =
    val verified = Verify.run(g.in)
    val images   = Images.load(g.images, g.image)
    val missing = verified.toOption.toVector.flatMap { v =>
      val known              = images.getOrElse(Map.empty)
      val (builtin, process) = v.blueprint.streamlets.partition(_.descriptor.builtin)
      process
        .filterNot(s => known.contains(s.name))
        .map(s => BlueprintProblem.toMessage(MissingImage(s.name))) ++
        builtin
          .filter(s => known.contains(s.name))
          .map(s => BlueprintProblem.toMessage(BuiltinHasImage(s.name)))
    }
    val pipeline = g.pipeline
      .orElse(verified.toOption.flatMap(_.blueprint.name))
      .getOrElse(g.in.blueprint.getFileName.toString.takeWhile(_ != '.'))
    val nameProblem =
      Option.when(!PipelineName.matches(pipeline) || pipeline.length > 40)(
        s"pipeline id '$pipeline' must be 1-40 of [a-z0-9-], not starting or ending with '-'"
      )
    val problems: Vector[String] =
      (verified.left.toSeq.flatten ++ images.left.toSeq.flatten ++ missing ++ nameProblem).toVector
    if problems.nonEmpty then refuse(err, problems.toVector)
    else
      val v        = verified.toOption.get
      val version  = g.version.getOrElse(gitVersion(g.in.blueprint))
      val onDelete = OnDelete(if g.deleteManagedTopics then OnDelete.Delete else OnDelete.Keep)
      val resource =
        ResourceWriter.write(v, images.toOption.get, pipeline, version, g.namespace, onDelete)
      val yaml = FlowSerialization.toYaml(resource)
      v.notes.foreach(err.println)
      g.out match
        case Some(p) =>
          Files.write(p, yaml.getBytes("UTF-8"))
          err.println(s"wrote $p")
        case None => out.print(yaml)
      0

  private def gitVersion(blueprint: Path): String =
    val dir = Option(blueprint.toAbsolutePath.getParent).getOrElse(Path.of("."))
    Try(
      Process(Seq("git", "-C", dir.toString, "describe", "--tags", "--always", "--dirty"))
        .!!(ProcessLogger(_ => ()))
        .trim
    ).toOption
      .filter(_.nonEmpty)
      .getOrElse("unversioned")

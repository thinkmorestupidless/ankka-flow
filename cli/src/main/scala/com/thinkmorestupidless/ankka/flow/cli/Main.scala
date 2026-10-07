package com.thinkmorestupidless.ankka.flow.cli

import java.io.PrintStream
import java.nio.file.{Files, Path}

import cats.syntax.all.*
import com.monovore.decline.*
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
    case McpCmd
    case McpInstallCmd(req: mcp.McpInstall.Request)

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

  private val mcpInstall = Opts.subcommand(
    "install",
    "Configure Claude Code (for you, or a project's .mcp.json) or Claude Desktop to start `flow mcp`."
  )(
    (
      Opts
        .option[String]("client", "code (the default) or desktop.")
        .withDefault("code")
        .mapValidated {
          case "code"    => mcp.McpInstall.Client.Code.validNel
          case "desktop" => mcp.McpInstall.Client.Desktop.validNel
          case other     => s"client '$other' is not code or desktop".invalidNel
        },
      Opts
        .option[String](
          "scope",
          "user (the default: every project, for you) or project (a .mcp.json to commit)."
        )
        .withDefault("user")
        .mapValidated {
          case "user"    => mcp.McpInstall.Scope.User.validNel
          case "project" => mcp.McpInstall.Scope.Project.validNel
          case other     => s"scope '$other' is not user or project".invalidNel
        },
      Opts
        .option[Path]("dir", "The project, for --scope project (default: here).")
        .withDefault(Path.of(".")),
      Opts
        .option[String](
          "command",
          "The flow to start, for Claude Desktop (default: the first on PATH)."
        )
        .orNone,
      Opts.flag("force", "Replace an existing ankka-flow entry.").orFalse,
      Opts.flag("dry-run", "Print what would change; change nothing.").orFalse
    ).mapN((client, scope, dir, command, force, dryRun) =>
      Cmd.McpInstallCmd(mcp.McpInstall.Request(client, scope, dir, command, force, dryRun))
    )
  )

  private val mcpCommand =
    Opts.subcommand(
      "mcp",
      "Serve flow's tools and the documentation to a coding agent over MCP (stdio)."
    )(
      mcpInstall.orElse(Opts(Cmd.McpCmd))
    )

  private val command = Command("flow", "Verify, generate and operate ankka-flow pipelines.")(
    verify.orElse(generate).orElse(reset).orElse(init).orElse(mcpCommand).orElse(version)
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
          case Cmd.McpCmd =>
            val cluster = mcp.ProjectFile.read(Path.of(".").toAbsolutePath)
            val tools   = mcp.FlowTools(cluster)
            val server = mcp.McpServer(
              "ankka-flow",
              BuildInfo.version,
              tools.instructions,
              tools.all,
              tools.resources
            )
            server.serve(
              new java.io.BufferedReader(new java.io.InputStreamReader(System.in, "UTF-8")),
              out,
              err,
              interactive = mcp.McpServer.startedAtTerminal()
            )
            0
          case Cmd.McpInstallCmd(req) =>
            try
              out.println(mcp.McpInstall.perform(req))
              0
            catch
              case e: IllegalArgumentException =>
                err.println(e.getMessage)
                1
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

  private def generateResource(g: Cmd.GenerateCmd, out: PrintStream, err: PrintStream): Int =
    Generate.run(
      Generate.Request(
        g.in,
        g.images,
        g.image,
        g.pipeline,
        g.version,
        g.namespace,
        g.deleteManagedTopics
      )
    ) match
      case Left(problems) => refuse(err, problems)
      case Right(generated) =>
        generated.notes.foreach(err.println)
        g.out match
          case Some(p) =>
            Files.write(p, generated.yaml.getBytes("UTF-8"))
            err.println(s"wrote $p")
          case None => out.print(generated.yaml)
        0

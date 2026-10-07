package com.thinkmorestupidless.ankka.flow.cli.mcp

import java.nio.file.{Files, Path, Paths}

import com.thinkmorestupidless.ankka.flow.protocol.Json

import JsonText.*

/**
 * `flow mcp install`: tells an MCP client how to start `flow mcp`. Three targets, because a
 * client's configuration lives in three places: Claude Code for this user (through
 * `claude mcp add`, Claude Code's own command), a project's `.mcp.json` (the file `flow init`
 * writes; it names the command `flow`, since it is committed and read on other machines), and
 * Claude Desktop's `claude_desktop_config.json` (an absolute path, and `JAVA_HOME` for the JVM
 * build, because Desktop is started from the Dock, not a shell). Every write merges: other servers
 * are kept, and an existing `ankka-flow` entry is left alone unless `--force`. Ported from ankka's.
 */
private[cli] object McpInstall:

  val ServerName = "ankka-flow"

  final case class Launch(command: String, args: Vector[String], env: Vector[(String, String)]):
    def entry: Json = Json.Obj(
      Vector("command" -> str(command), "args" -> Json.Arr(args.map(str))) ++
        Option.when(env.nonEmpty)("env" -> Json.Obj(env.map((k, v) => k -> str(v))))
    )

  val ProjectLaunch: Launch = Launch("flow", Vector("mcp"), Vector.empty)

  private def refuse(message: String): Nothing = throw IllegalArgumentException(message)

  def locate(
      explicit: Option[String],
      path: String = sys.env.getOrElse("PATH", ""),
      self: () => Option[Path] = () => running()
  ): Path =
    explicit
      .map(Paths.get(_).toAbsolutePath)
      .orElse(onPath("flow", path))
      .orElse(self())
      .getOrElse(
        refuse(
          "cannot tell where this flow is installed: put it on PATH, or pass --command /path/to/flow"
        )
      )

  def onPath(name: String, path: String): Option[Path] =
    path
      .split(java.io.File.pathSeparator)
      .iterator
      .filter(_.nonEmpty)
      .map(dir => Paths.get(dir).resolve(name))
      .find(Files.isExecutable)

  def isNative: Boolean = sys.props.get("org.graalvm.nativeimage.imagecode").contains("runtime")

  private def running(): Option[Path] =
    if isNative then Option(ProcessHandle.current().info().command().orElse(null)).map(Paths.get(_))
    else
      Option(getClass.getProtectionDomain.getCodeSource)
        .flatMap(source => Option(source.getLocation))
        .map(url => Paths.get(url.toURI))
        .flatMap(jar => Option(jar.getParent).flatMap(lib => Option(lib.getParent)))
        .map(_.resolve("bin").resolve("flow"))
        .filter(Files.isExecutable)

  def desktopLaunch(command: Path, native: Boolean = isNative): Launch =
    val env =
      if native then Vector.empty
      else sys.props.get("java.home").map(home => "JAVA_HOME" -> home).toVector
    Launch(command.toString, Vector("mcp"), env)

  enum Outcome:
    case Added, Replaced, Unchanged
    case Kept(existing: Json)

  /** `config` with `mcpServers.ankka-flow` set to `entry`, every other field kept in its order. */
  def merge(config: Option[Json], entry: Json, force: Boolean): (Json, Outcome) =
    val root: Json.Obj = config.getOrElse(Json.Obj(Vector.empty)) match
      case o: Json.Obj => o
      case _           => refuse("the configuration is not a JSON object")
    val servers: Json.Obj = root.field("mcpServers") match
      case Some(o: Json.Obj) => o
      case None              => Json.Obj(Vector.empty)
      case Some(_)           => refuse("the configuration's mcpServers is not an object")
    servers.field(ServerName) match
      case Some(existing) if same(existing, entry) => (root, Outcome.Unchanged)
      case Some(existing) if !force                => (root, Outcome.Kept(existing))
      case existing =>
        (
          put(root, "mcpServers", put(servers, ServerName, entry)),
          if existing.isDefined then Outcome.Replaced else Outcome.Added
        )

  private def same(a: Json, b: Json): Boolean = (a, b) match
    case (Json.Obj(x), Json.Obj(y)) =>
      x.size == y.size && x.forall((k, v) =>
        y.collectFirst { case (`k`, w) => w }.exists(same(v, _))
      )
    case (Json.Arr(x), Json.Arr(y)) => x.size == y.size && x.zip(y).forall(same.tupled)
    case _                          => a == b

  private def put(o: Json.Obj, key: String, value: Json): Json.Obj =
    if o.fields.exists(_._1 == key) then
      Json.Obj(o.fields.map((k, v) => if k == key then k -> value else k -> v))
    else Json.Obj(o.fields :+ (key -> value))

  def read(file: Path): Option[Json] =
    if !Files.exists(file) then None
    else
      Json
        .parse(Files.readString(file))
        .fold(e => refuse(s"$file is not valid JSON, so it was left as it is: $e"), Some(_))

  def install(file: Path, entry: Json, force: Boolean, dryRun: Boolean): (Outcome, String) =
    val (merged, outcome) = merge(read(file), entry, force)
    val text              = merged.pretty + "\n"
    val writes = outcome match
      case Outcome.Added | Outcome.Replaced => true
      case _                                => false
    if writes && !dryRun then
      Option(file.getParent).foreach(parent => Files.createDirectories(parent): Unit)
      Files.writeString(file, text): Unit
    (outcome, text)

  /**
   * `-Dflow.claude.desktop.config` if set (a test never writes the developer's own), else Desktop's
   * file.
   */
  def desktopConfig(
      os: String = sys.props.getOrElse("os.name", ""),
      home: String = sys.props("user.home"),
      appData: Option[String] = sys.env.get("APPDATA")
  ): Path =
    sys.props.get("flow.claude.desktop.config").map(Paths.get(_)).getOrElse {
      val name = os.toLowerCase
      if name.contains("mac") then
        Paths.get(home, "Library", "Application Support", "Claude", "claude_desktop_config.json")
      else if name.contains("windows") then
        Paths.get(
          appData.getOrElse(Paths.get(home, "AppData", "Roaming").toString),
          "Claude",
          "claude_desktop_config.json"
        )
      else
        refuse(
          "Claude Desktop runs on macOS and Windows only; on this machine use `flow mcp install` for Claude Code"
        )
    }

  def claudeAdd(launch: Launch): Vector[String] =
    Vector("claude", "mcp", "add", "--scope", "user") ++ launch.env.flatMap((k, v) =>
      Vector("-e", s"$k=$v")
    ) ++
      Vector(ServerName, "--", launch.command) ++ launch.args

  val claudeGet: Vector[String] = Vector("claude", "mcp", "get", ServerName)
  val claudeRemove: Vector[String] =
    Vector("claude", "mcp", "remove", "--scope", "user", ServerName)

  type Runner = Vector[String] => (Int, String)

  val processRunner: Runner = argv =>
    val process = new ProcessBuilder(argv*).redirectErrorStream(true).start()
    val output  = new String(process.getInputStream.readAllBytes())
    (process.waitFor(), output.trim)

  enum Client:
    case Code, Desktop

  enum Scope:
    case User, Project

  final case class Request(
      client: Client = Client.Code,
      scope: Scope = Scope.User,
      dir: Path = Paths.get("."),
      command: Option[String] = None,
      force: Boolean = false,
      dryRun: Boolean = false
  )

  def perform(
      request: Request,
      runner: Runner = processRunner,
      path: String = sys.env.getOrElse("PATH", "")
  ): String =
    (request.client, request.scope) match
      case (Client.Desktop, Scope.Project) =>
        refuse("--scope project is for Claude Code; Claude Desktop has one configuration")
      case (Client.Code, Scope.Project) =>
        val file            = request.dir.resolve(".mcp.json").toAbsolutePath.normalize
        val (outcome, text) = install(file, ProjectLaunch.entry, request.force, request.dryRun)
        report(outcome, file.toString, text, request.dryRun) +
          (if outcome == Outcome.Added && !request.dryRun then
             "\nCommit it; Claude Code asks each person once before starting a project's server."
           else "")
      case (Client.Desktop, Scope.User) =>
        val file            = desktopConfig()
        val launch          = desktopLaunch(locate(request.command, path))
        val (outcome, text) = install(file, launch.entry, request.force, request.dryRun)
        report(outcome, file.toString, text, request.dryRun) +
          (if Set[Outcome](Outcome.Added, Outcome.Replaced)(outcome) && !request.dryRun then
             "\nQuit and reopen Claude Desktop to load it."
           else "")
      case (Client.Code, Scope.User) =>
        val launch = Launch(locate(request.command, path).toString, Vector("mcp"), Vector.empty)
        val add    = claudeAdd(launch)
        if onPath("claude", path).isEmpty then
          s"""Claude Code's `claude` command is not on PATH, so nothing was changed. Run:
             |
             |  ${shell(add)}
             |
             |or install the ankka-flow plugin inside Claude Code:
             |
             |  /plugin marketplace add thinkmorestupidless/ankka-marketplace
             |  /plugin install ankka-flow@ankka""".stripMargin
        else if request.dryRun then s"would run: ${shell(add)}"
        else
          val (present, existing) = runner(claudeGet)
          if present == 0 && !request.force then
            s"""Claude Code already has a server named '$ServerName'; left as it is:
               |
               |${existing.linesIterator.map("  " + _).mkString("\n")}
               |
               |Pass --force to replace it.""".stripMargin
          else
            if present == 0 then
              val (code, output) = runner(claudeRemove)
              if code != 0 then refuse(s"`${shell(claudeRemove)}` failed: $output")
            val (code, output) = runner(add)
            if code != 0 then refuse(s"`${shell(add)}` failed: $output")
            s"registered ankka-flow with Claude Code for this user: ${shell(add)}"

  private def report(outcome: Outcome, file: String, text: String, dryRun: Boolean): String =
    outcome match
      case Outcome.Unchanged => s"$file already starts flow mcp; nothing to change"
      case Outcome.Kept(existing) =>
        s"""$file already has a server named '$ServerName', different from this one; left as it is:
           |
           |  ${existing.render}
           |
           |Pass --force to replace it.""".stripMargin
      case Outcome.Added | Outcome.Replaced =>
        val verb =
          if outcome == Outcome.Added then "added ankka-flow to" else "replaced ankka-flow in"
        if dryRun then s"would write $file:\n\n$text" else s"$verb $file"

  def shell(argv: Vector[String]): String =
    argv
      .map(arg =>
        if arg.matches("[A-Za-z0-9_./:=@+-]+") then arg else s"'${arg.replace("'", "'\\''")}'"
      )
      .mkString(" ")

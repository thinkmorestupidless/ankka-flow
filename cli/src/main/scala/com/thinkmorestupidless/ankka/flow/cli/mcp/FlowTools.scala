package com.thinkmorestupidless.ankka.flow.cli.mcp

import java.nio.file.Path

import com.thinkmorestupidless.ankka.flow.cli.{BuildInfo, Generate, Verify}
import com.thinkmorestupidless.ankka.flow.protocol.{Json, ProtocolVersion}

import JsonText.*

/**
 * What `flow mcp` offers: the CLI's own verbs, a pipeline on the named cluster, and the
 * documentation. Every pure tool answers the bytes the command prints; every cluster tool acts on
 * the cluster `flow.toml` names and refuses without one.
 */
private[cli] final class FlowTools(
    cluster: Option[NamedCluster],
    clusterAccess: NamedCluster => Cluster = Cluster(_)
):

  private def required(args: Json, field: String): String =
    args
      .string(field)
      .filter(_.nonEmpty)
      .getOrElse(throw IllegalArgumentException(s"`$field` is required"))

  private def path(args: Json, field: String): Path = Path.of(required(args, field))

  private def schema(required: Seq[String], properties: (String, Json)*): Json =
    obj(
      "type"                 -> str("object"),
      "properties"           -> Json.Obj(properties.toVector),
      "required"             -> Json.Arr(required.map(str).toVector),
      "additionalProperties" -> bool(false)
    )

  private def string(description: String): Json =
    obj("type" -> str("string"), "description" -> str(description))
  private def integer(description: String): Json =
    obj("type" -> str("integer"), "description" -> str(description))
  private def boolean(description: String): Json =
    obj("type" -> str("boolean"), "description" -> str(description))
  private def strings(description: String): Json =
    obj(
      "type"        -> str("array"),
      "items"       -> obj("type" -> str("string")),
      "description" -> str(description)
    )
  private def stringMap(description: String): Json =
    obj(
      "type"                 -> str("object"),
      "additionalProperties" -> obj("type" -> str("string")),
      "description"          -> str(description)
    )

  private val blueprintArgs = Seq(
    "blueprint" -> string("Path of the blueprint (blueprint.conf)."),
    "descriptors" -> string(
      "Directory of descriptor files (*.json); optional when every streamlet is built in."
    ),
    "conf" -> strings("Deploy-time configuration files (HOCON); later files win.")
  )

  private def inputs(args: Json): Verify.Inputs =
    Verify.Inputs(
      path(args, "blueprint"),
      args.string("descriptors").map(Path.of(_)),
      args.strings("conf").toList.map(Path.of(_))
    )

  private def generateRequest(args: Json, namespace: Option[String]): Generate.Request =
    Generate.Request(
      inputs(args),
      images = None,
      image = args("images").map(_.objectFields).getOrElse(Vector.empty).toList.collect {
        case (k, Json.Str(v)) => s"$k=$v"
      },
      pipeline = args.string("pipeline"),
      version = args.string("version"),
      namespace = args.string("namespace").orElse(namespace),
      deleteManagedTopics = args.boolean("delete_managed_topics").getOrElse(false)
    )

  /** The bytes `flow verify` prints: stdout, then stderr. */
  private def verifyText(in: Verify.Inputs): ToolResult =
    Verify.run(in) match
      case Left(problems) => ToolResult(problems.mkString("\n"), isError = true)
      case Right(v) =>
        val summary =
          s"verified: ${v.blueprint.streamlets.size} streamlets, ${v.blueprint.topics.size} topics\n"
        ToolResult(summary + v.notes.map(_ + "\n").mkString)

  // ── Without a cluster ───────────────────────────────────────────────────

  private val pure: Vector[Tool] = Vector(
    Tool(
      "verify_blueprint",
      "Verify a blueprint",
      "Verify a blueprint against streamlet descriptors and deploy-time configuration, exactly as `flow verify` does: the summary and notes, or the refusals.",
      schema(Seq("blueprint"), blueprintArgs*),
      readOnly = true,
      idempotent = true
    )(args => verifyText(inputs(args))),
    Tool(
      "generate_resource",
      "Generate the pipeline resource",
      "Verify, then write the AnkkaFlow resource `flow generate` writes, as YAML. Nothing is applied; `apply_pipeline` applies it to the named cluster.",
      schema(
        Seq("blueprint"),
        (blueprintArgs ++ Seq(
          "images" -> stringMap(
            "Streamlet name to image reference, for every streamlet that is not built in."
          ),
          "pipeline"  -> string("Pipeline id (default: the blueprint's name)."),
          "version"   -> string("Pipeline version (default: git describe)."),
          "namespace" -> string("Namespace for the resource (default: the named cluster's)."),
          "delete_managed_topics" -> boolean(
            "Delete the topics the pipeline created when its resource is deleted (default: keep them)."
          )
        ))*
      ),
      readOnly = true,
      idempotent = true
    ) { args =>
      Generate.run(generateRequest(args, cluster.map(_.namespace))) match
        case Left(problems)   => ToolResult(problems.mkString("\n"), isError = true)
        case Right(generated) => ToolResult(generated.yaml + generated.notes.map(_ + "\n").mkString)
    },
    Tool(
      "flow_version",
      "The CLI and protocol versions",
      "What `flow version` prints: this CLI's version and the streamlet protocol version it writes.",
      schema(Nil),
      readOnly = true,
      idempotent = true
    )(_ => ToolResult(s"flow ${BuildInfo.version}, protocol ${ProtocolVersion.Current}\n"))
  )

  // ── The named cluster ───────────────────────────────────────────────────

  private def onCluster(f: Cluster => ToolResult): ToolResult =
    cluster match
      case None        => ToolResult(ProjectFile.HowToName, isError = true)
      case Some(named) => f(clusterAccess(named))

  private val onNamedCluster = "Acts only on the cluster and namespace `flow.toml` names."

  private val reads: Vector[Tool] = Vector(
    Tool(
      "list_pipelines",
      "List the pipelines",
      s"Every pipeline in the named namespace with its phase, its detail and its streamlets' ready counts. $onNamedCluster",
      schema(Nil),
      readOnly = true,
      idempotent = true,
      openWorld = true
    )(_ => onCluster(c => ToolResult(c.listPipelines()))),
    Tool(
      "get_pipeline",
      "Describe a pipeline",
      s"One pipeline: its streamlets and images, its status (phase, detail, streamlets, topics) and the recent events on it, including stalled partitions. $onNamedCluster",
      schema(Seq("name"), "name" -> string("The pipeline's name.")),
      readOnly = true,
      idempotent = true,
      openWorld = true
    )(args => onCluster(c => ToolResult(c.describePipeline(required(args, "name"))))),
    Tool(
      "pipeline_logs",
      "Read a streamlet's logs",
      s"The recent log lines of one streamlet's process container, or of its sidecar. $onNamedCluster",
      schema(
        Seq("name", "streamlet"),
        "name"      -> string("The pipeline's name."),
        "streamlet" -> string("The streamlet's name in the pipeline."),
        "container" -> string("process (the default) or sidecar."),
        "lines"     -> integer("How many lines from the end (default 200).")
      ),
      readOnly = true,
      idempotent = true,
      openWorld = true
    ) { args =>
      onCluster(c =>
        ToolResult(
          c.logs(
            required(args, "name"),
            required(args, "streamlet"),
            args.string("container").getOrElse("process"),
            args.int("lines").getOrElse(200)
          )
        )
      )
    },
    Tool(
      "pipeline_lag",
      "Read a pipeline's consumer lag",
      s"The consumer lag of every inlet partition of every streamlet, read from each sidecar's metrics. $onNamedCluster",
      schema(Seq("name"), "name" -> string("The pipeline's name.")),
      readOnly = true,
      idempotent = true,
      openWorld = true
    )(args => onCluster(c => ToolResult(c.lag(required(args, "name")))))
  )

  private val writes: Vector[Tool] = Vector(
    Tool(
      "apply_pipeline",
      "Apply a pipeline",
      s"Verify the blueprint, generate the AnkkaFlow resource and apply it to the named cluster, as `flow generate | kubectl apply` would. Creates the pipeline or updates it. $onNamedCluster",
      schema(
        Seq("blueprint"),
        (blueprintArgs ++ Seq(
          "images"   -> stringMap("Streamlet name to image reference."),
          "pipeline" -> string("Pipeline id (default: the blueprint's name)."),
          "version"  -> string("Pipeline version (default: git describe)."),
          "delete_managed_topics" -> boolean(
            "Delete the topics the pipeline created when its resource is deleted (default: keep them)."
          )
        ))*
      ),
      readOnly = false,
      destructive = true,
      idempotent = true,
      openWorld = true
    ) { args =>
      onCluster { c =>
        Generate.run(generateRequest(args, Some(c.named.namespace))) match
          case Left(problems)   => ToolResult(problems.mkString("\n"), isError = true)
          case Right(generated) => ToolResult(c.apply(generated.yaml))
      }
    },
    Tool(
      "reset_pipeline",
      "Reset a pipeline",
      s"Request that a pipeline's streamlets, or the named ones, reread their inputs from the start, exactly as `flow reset` requests it: a streamlet still running is refused. $onNamedCluster",
      schema(
        Seq("name"),
        "name"       -> string("The pipeline's name."),
        "streamlets" -> strings("Streamlets to reset (default: every one with an inlet).")
      ),
      readOnly = false,
      destructive = true,
      openWorld = true
    )(args => onCluster(c => c.reset(required(args, "name"), args.strings("streamlets").toList)))
  )

  // ── Documentation ───────────────────────────────────────────────────────

  private val documentation: Vector[Tool] = Vector(
    Tool(
      "search_docs",
      "Search the documentation",
      "Find documentation pages of this ankka-flow version by keywords. Returns each page's path, title and one-sentence description; read one with `read_doc`.",
      schema(
        Seq("query"),
        "query" -> string("Keywords, e.g. `blueprint unmanaged topic` or `reset`."),
        "limit" -> integer("How many pages (default 5).")
      ),
      readOnly = true,
      idempotent = true
    ) { args =>
      val pages = Docs.search(required(args, "query"), args.int("limit").getOrElse(5))
      if pages.isEmpty then ToolResult("no page matches; `read_doc` with `index.md` shows the map")
      else ToolResult(pages.map(p => s"${p.path} — ${p.title}: ${p.description}").mkString("\n"))
    },
    Tool(
      "read_doc",
      "Read a documentation page",
      "One documentation page, whole, as Markdown. Samples in it are copied from code the ankka-flow build compiles and tests.",
      schema(
        Seq("path"),
        "path" -> string("The page's path, e.g. `build/blueprints.md` or `reference/cli.md`.")
      ),
      readOnly = true,
      idempotent = true
    ) { args =>
      val p = required(args, "path").stripPrefix("/").stripPrefix("ankka-flow://docs/")
      Docs.read(p) match
        case Some(text) => ToolResult(text)
        case None       => ToolResult(s"no page '$p'; `search_docs` finds pages", isError = true)
    }
  )

  val all: Vector[Tool] = pure ++ reads ++ writes ++ documentation

  def resources(): Vector[Resource] =
    Docs.pages.map(page =>
      Resource(page.uri, page.path, page.title, page.description, "text/markdown")(() =>
        Docs.read(page.path).getOrElse("")
      )
    )

  val instructions: String =
    s"""Tools for ankka-flow, streaming pipelines of streamlets wired by a blueprint over Kafka topics, with a sidecar owning Kafka beside each streamlet.
       |
       |`verify_blueprint`, `generate_resource` and `flow_version` do what the `flow` commands do and touch no cluster. The cluster tools act only on ${cluster
        .fold(
          "the cluster `flow.toml` names — none is named here, so they refuse until one is written"
        )(c =>
          s"context `${c.context}`, namespace `${c.namespace}` (from flow.toml)"
        )}; `apply_pipeline` and `reset_pipeline` change it. `search_docs` and `read_doc` read this version's documentation; read the relevant page before writing a streamlet or a blueprint, because its samples are compiled and tested.""".stripMargin

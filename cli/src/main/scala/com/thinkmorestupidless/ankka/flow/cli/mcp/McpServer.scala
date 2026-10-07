package com.thinkmorestupidless.ankka.flow.cli.mcp

import java.io.{BufferedReader, PrintStream}

import com.thinkmorestupidless.ankka.flow.protocol.Json

import JsonText.*

/**
 * What a tool call produced: text for the model, and whether it is an error the model should see.
 */
private[cli] final case class ToolResult(text: String, isError: Boolean = false)

/**
 * One tool: a name, what it does, the JSON Schema of its arguments, and how far it reaches. The
 * hints are the protocol's tool annotations: a client uses them to decide what to confirm with a
 * person before running — a read-only tool changes nothing, a destructive one changes the cluster.
 */
private[cli] final case class Tool(
    name: String,
    title: String,
    description: String,
    inputSchema: Json,
    readOnly: Boolean,
    destructive: Boolean = false,
    idempotent: Boolean = false,
    openWorld: Boolean = false
)(val run: Json => ToolResult):

  def describe: Json = obj(
    "name"        -> str(name),
    "title"       -> str(title),
    "description" -> str(description),
    "inputSchema" -> inputSchema,
    "annotations" -> obj(
      "title"           -> str(title),
      "readOnlyHint"    -> bool(readOnly),
      "destructiveHint" -> bool(destructive),
      "idempotentHint"  -> bool(idempotent),
      "openWorldHint"   -> bool(openWorld)
    )
  )

/** A document the server hands over whole: one page of the documentation. */
private[cli] final case class Resource(
    uri: String,
    name: String,
    title: String,
    description: String,
    mimeType: String
)(val read: () => String):

  def describe: Json = obj(
    "uri"         -> str(uri),
    "name"        -> str(name),
    "title"       -> str(title),
    "description" -> str(description),
    "mimeType"    -> str(mimeType)
  )

/**
 * A Model Context Protocol server over stdio: JSON-RPC 2.0, one message per line. Stdout carries
 * protocol messages and nothing else; anything a person should see goes to stderr. Ported from
 * ankka's `ankka mcp`.
 */
private[cli] final class McpServer(
    name: String,
    version: String,
    instructions: String,
    tools: Vector[Tool],
    resources: () => Vector[Resource]
):

  private val byName = tools.map(t => t.name -> t).toMap

  /** Serves until the input ends, which is how a client says it is done. */
  def serve(
      in: BufferedReader,
      out: PrintStream,
      log: PrintStream,
      interactive: Boolean = false
  ): Unit =
    if interactive then
      log.println(McpServer.InteractiveNotice)
      log.flush()
    var line = in.readLine()
    while line != null do
      if line.trim.nonEmpty then
        handleLine(line, log).foreach { response =>
          out.println(response.render)
          out.flush()
        }
      line = in.readLine()

  /** One line in; the reply to send, if the message expects one. */
  def handleLine(line: String, log: PrintStream): Option[Json] =
    Json.parse(line) match
      case Left(problem) => Some(error(Json.Null, McpServer.ParseError, s"not JSON: $problem"))
      case Right(Json.Arr(messages)) =>
        // Batches were dropped from the protocol in 2025-06-18; answering them keeps an older client working.
        val replies = messages.flatMap(handle(_, log))
        Option.when(replies.nonEmpty)(Json.Arr(replies))
      case Right(message) => handle(message, log)

  private def handle(message: Json, log: PrintStream): Option[Json] =
    val id     = message("id")
    val method = message.string("method")
    val params = message("params").getOrElse(obj())
    (id, method) match
      case (None, Some(_)) => None // a notification
      case (_, None) if message("result").isDefined || message("error").isDefined => None
      case (_, None) => Some(error(id.getOrElse(Json.Null), McpServer.InvalidRequest, "no method"))
      case (Some(requestId), Some(m)) =>
        val reply =
          try dispatch(m, params)
          catch
            case failure: McpServer.Failure => Left(failure)
            case failure: Exception =>
              log.println(s"flow mcp: $m failed: ${failure.getMessage}")
              Left(
                McpServer.Failure(
                  McpServer.InternalError,
                  Option(failure.getMessage).getOrElse(failure.toString)
                )
              )
        Some(reply match
          case Right(result) => obj("jsonrpc" -> str("2.0"), "id" -> requestId, "result" -> result)
          case Left(failure) => error(requestId, failure.code, failure.message))

  private def dispatch(method: String, params: Json): Either[McpServer.Failure, Json] =
    method match
      case "initialize" =>
        val agreed = params
          .string("protocolVersion")
          .filter(McpServer.SupportedVersions.contains)
          .getOrElse(McpServer.SupportedVersions.head)
        Right(
          obj(
            "protocolVersion" -> str(agreed),
            "capabilities" -> obj(
              "tools"     -> obj("listChanged" -> bool(false)),
              "resources" -> obj("listChanged" -> bool(false))
            ),
            "serverInfo"   -> obj("name" -> str(name), "version" -> str(version)),
            "instructions" -> str(instructions)
          )
        )
      case "ping"       => Right(obj())
      case "tools/list" => Right(obj("tools" -> Json.Arr(tools.map(_.describe))))
      case "tools/call" =>
        val toolName = params
          .string("name")
          .getOrElse(throw McpServer.Failure(McpServer.InvalidParams, "no tool name"))
        val tool = byName.getOrElse(
          toolName,
          throw McpServer.Failure(McpServer.InvalidParams, s"no tool named '$toolName'")
        )
        val arguments = params("arguments").getOrElse(obj())
        val result =
          // Whatever a tool throws is the tool's failure, never the session's end: the model reads
          // it and the client keeps its server. Only a fatal error passes.
          try tool.run(arguments)
          catch
            case failure: IllegalArgumentException => ToolResult(failure.getMessage, isError = true)
            case scala.util.control.NonFatal(failure) =>
              ToolResult(
                s"${failure.getClass.getSimpleName}: ${Option(failure.getMessage).getOrElse("")}".trim,
                isError = true
              )
            case failure: LinkageError =>
              ToolResult(
                s"${failure.getClass.getSimpleName}: ${Option(failure.getMessage).getOrElse("")}".trim,
                isError = true
              )
        Right(
          obj(
            "content" -> arr(obj("type" -> str("text"), "text" -> str(result.text))),
            "isError" -> bool(result.isError)
          )
        )
      case "resources/list" => Right(obj("resources" -> Json.Arr(resources().map(_.describe))))
      case "resources/read" =>
        val uri =
          params.string("uri").getOrElse(throw McpServer.Failure(McpServer.InvalidParams, "no uri"))
        val resource = resources()
          .find(_.uri == uri)
          .getOrElse(throw McpServer.Failure(McpServer.ResourceNotFound, s"no resource '$uri'"))
        Right(
          obj(
            "contents" -> arr(
              obj(
                "uri"      -> str(resource.uri),
                "mimeType" -> str(resource.mimeType),
                "text"     -> str(resource.read())
              )
            )
          )
        )
      case "resources/templates/list" => Right(obj("resourceTemplates" -> arr()))
      case other => Left(McpServer.Failure(McpServer.MethodNotFound, s"no method '$other'"))

  private def error(id: Json, code: Int, message: String): Json =
    obj(
      "jsonrpc" -> str("2.0"),
      "id"      -> id,
      "error"   -> obj("code" -> num(code), "message" -> str(message))
    )

private[cli] object McpServer:

  /** Newest first: an unknown request is answered with the newest, as the protocol asks. */
  val SupportedVersions: Vector[String] =
    Vector("2025-11-25", "2025-06-18", "2025-03-26", "2024-11-05")

  val InteractiveNotice: String =
    "flow mcp: serving the Model Context Protocol on stdin and stdout, and waiting for a client.\n" +
      "It is meant to be started by an MCP client, for example: claude mcp add ankka-flow -- flow mcp\n" +
      "Press Ctrl-D to stop."

  /**
   * Whether this process was started at a terminal (JDK 21's null console, or 22+'s isTerminal).
   */
  def startedAtTerminal(): Boolean =
    Option(System.console()).exists { console =>
      try classOf[java.io.Console].getMethod("isTerminal").invoke(console).asInstanceOf[Boolean]
      catch case _: NoSuchMethodException => true
    }

  val ParseError       = -32700
  val InvalidRequest   = -32600
  val MethodNotFound   = -32601
  val InvalidParams    = -32602
  val InternalError    = -32603
  val ResourceNotFound = -32002

  final case class Failure(code: Int, message: String) extends RuntimeException(message)

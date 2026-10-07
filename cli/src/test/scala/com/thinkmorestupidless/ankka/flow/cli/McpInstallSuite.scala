package com.thinkmorestupidless.ankka.flow.cli

import java.nio.file.attribute.PosixFilePermissions
import java.nio.file.Files

import com.thinkmorestupidless.ankka.flow.cli.mcp.JsonText.*
import com.thinkmorestupidless.ankka.flow.cli.mcp.McpInstall
import com.thinkmorestupidless.ankka.flow.cli.mcp.McpInstall.{Outcome, Request}
import com.thinkmorestupidless.ankka.flow.protocol.Json

/** `flow mcp install`, everything under temporary directories and a scripted `claude`. */
class McpInstallSuite extends munit.FunSuite:

  private def parse(text: String): Json = Json.parse(text).fold(e => fail(e), identity)
  private val entry                     = McpInstall.ProjectLaunch.entry

  test(
    "a missing configuration gains one server; other servers and settings are kept in their order"
  ) {
    val (merged, outcome) = McpInstall.merge(None, entry, force = false)
    assertEquals(outcome, Outcome.Added)
    assertEquals(
      merged.render,
      """{"mcpServers":{"ankka-flow":{"command":"flow","args":["mcp"]}}}"""
    )
    val existing = parse(
      """{"theme":"dark","mcpServers":{"github":{"command":"gh-mcp"}},"globalShortcut":"Cmd+K"}"""
    )
    val (kept, o2) = McpInstall.merge(Some(existing), entry, force = false)
    assertEquals(o2, Outcome.Added)
    assertEquals(
      kept.render,
      """{"theme":"dark","mcpServers":{"github":{"command":"gh-mcp"},"ankka-flow":{"command":"flow","args":["mcp"]}},"globalShortcut":"Cmd+K"}"""
    )
  }

  test("the same entry in another key order is unchanged; a different one is kept unless forced") {
    val same = parse("""{"mcpServers":{"ankka-flow":{"args":["mcp"],"command":"flow"}}}""")
    assertEquals(McpInstall.merge(Some(same), entry, force = false)._2, Outcome.Unchanged)
    val theirs = """{"command":"/opt/flow-dev/bin/flow","args":["mcp"]}"""
    val existing =
      parse(s"""{"mcpServers":{"a":{"command":"a"},"ankka-flow":$theirs,"z":{"command":"z"}}}""")
    val (kept, outcome) = McpInstall.merge(Some(existing), entry, force = false)
    assertEquals(outcome, Outcome.Kept(parse(theirs)))
    assertEquals(kept, existing)
    val (replaced, forced) = McpInstall.merge(Some(existing), entry, force = true)
    assertEquals(forced, Outcome.Replaced)
    assertEquals(
      replaced.render,
      """{"mcpServers":{"a":{"command":"a"},"ankka-flow":{"command":"flow","args":["mcp"]},"z":{"command":"z"}}}"""
    )
  }

  test("--scope project writes .mcp.json, says to commit it, and a dry run writes nothing") {
    val dir = Files.createTempDirectory("proj")
    val dry =
      McpInstall.perform(Request(scope = McpInstall.Scope.Project, dir = dir, dryRun = true))
    assert(dry.startsWith("would write") && !Files.exists(dir.resolve(".mcp.json")), dry)
    val said = McpInstall.perform(Request(scope = McpInstall.Scope.Project, dir = dir))
    assert(said.contains("Commit it"), said)
    assertEquals(
      parse(Files.readString(dir.resolve(".mcp.json")))("mcpServers")
        .flatMap(_("ankka-flow"))
        .map(_.render),
      Some("""{"command":"flow","args":["mcp"]}""")
    )
    assert(
      McpInstall
        .perform(Request(scope = McpInstall.Scope.Project, dir = dir))
        .contains("nothing to change")
    )
  }

  test("Claude Desktop's configuration is merged at the property's path with an absolute flow") {
    val bin = Files.createTempDirectory("bin")
    val exe = Files.createFile(
      bin.resolve("flow"),
      PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwxr-xr-x"))
    )
    val config = Files.createTempDirectory("desktop").resolve("claude_desktop_config.json")
    System.setProperty("flow.claude.desktop.config", config.toString)
    try
      val said =
        McpInstall.perform(Request(client = McpInstall.Client.Desktop), path = bin.toString)
      assert(said.contains("Quit and reopen Claude Desktop"), said)
      val launch = parse(Files.readString(config))("mcpServers")
        .flatMap(_("ankka-flow"))
        .getOrElse(fail("no entry"))
      assertEquals(launch.string("command"), Some(exe.toString))
      assertEquals(launch.strings("args"), Vector("mcp"))
    finally System.clearProperty("flow.claude.desktop.config"): Unit
    assertEquals(
      McpInstall.desktopConfig(os = "Mac OS X", home = "/Users/x", appData = None).toString,
      "/Users/x/Library/Application Support/Claude/claude_desktop_config.json"
    )
  }

  test(
    "Claude Code for the user goes through claude mcp add, kept unless forced, and nothing without claude on PATH"
  ) {
    val bin = Files.createTempDirectory("bin")
    Files.createFile(
      bin.resolve("flow"),
      PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwxr-xr-x"))
    )
    val without = McpInstall.perform(Request(), path = bin.toString)
    assert(
      without.contains("is not on PATH") && without.contains(
        "claude mcp add --scope user ankka-flow --"
      ),
      without
    )
    Files.createFile(
      bin.resolve("claude"),
      PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwxr-xr-x"))
    )
    var ran = Vector.empty[Vector[String]]
    val present: McpInstall.Runner = argv => {
      ran :+= argv; if argv.contains("get") then (0, "ankka-flow: /old/flow mcp") else (0, "")
    }
    val kept = McpInstall.perform(Request(), runner = present, path = bin.toString)
    assert(kept.contains("already has a server named 'ankka-flow'"), kept)
    assertEquals(ran.map(_.take(3)), Vector(Vector("claude", "mcp", "get")))
    ran = Vector.empty
    val forced = McpInstall.perform(Request(force = true), runner = present, path = bin.toString)
    assert(forced.startsWith("registered ankka-flow"), forced)
    assertEquals(ran.map(_(2)), Vector("get", "remove", "add"))
  }

package com.thinkmorestupidless.ankka.flow.cli

import java.io.{BufferedReader, ByteArrayOutputStream, PrintStream, StringReader}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path}

import scala.jdk.CollectionConverters.*

import com.fasterxml.jackson.databind.ObjectMapper
import com.thinkmorestupidless.ankka.flow.cli.mcp.*
import com.thinkmorestupidless.ankka.flow.cli.mcp.JsonText.*
import com.thinkmorestupidless.ankka.flow.crd.*
import com.thinkmorestupidless.ankka.flow.protocol.Json
import io.fabric8.kubernetes.api.model.{
  EventBuilder,
  ObjectMetaBuilder,
  ObjectReferenceBuilder,
  PodBuilder
}
import io.fabric8.kubernetes.client.{KubernetesClient, KubernetesClientBuilder}
import io.fabric8.kubernetes.client.server.mock.{KubernetesMixedDispatcher, KubernetesMockServer}
import io.fabric8.mockwebserver.{Context, MockWebServer}

/**
 * `flow mcp` driven over its protocol: negotiation, listings, the pure tools byte-equal to the
 * commands, the docs as resources, and the cluster tools against a mock API server — or refused
 * when no cluster is named.
 */
class McpServerSuite extends munit.FunSuite:

  // ── the mock cluster ──────────────────────────────────────────────────────
  private val responses =
    new java.util.HashMap[io.fabric8.mockwebserver.ServerRequest, java.util.Queue[
      io.fabric8.mockwebserver.ServerResponse
    ]]()
  private val server = new KubernetesMockServer(
    new Context(),
    new MockWebServer(),
    responses,
    new KubernetesMixedDispatcher(responses),
    false
  )
  private var client: KubernetesClient = scala.compiletime.uninitialized

  override def beforeAll(): Unit =
    server.init()
    client = new KubernetesClientBuilder()
      .withConfig(server.createClient().getConfiguration)
      .withKubernetesSerialization(FlowSerialization())
      .build()

  override def afterAll(): Unit = server.destroy()

  private val named = NamedCluster("flow-test", "shop")
  private def access(n: NamedCluster): Cluster =
    Cluster(
      n,
      _ => { val c = server.createClient().getConfiguration; c.setNamespace(n.namespace); c }
    )

  private def tools(cluster: Option[NamedCluster]) = FlowTools(cluster, access)
  private def server(cluster: Option[NamedCluster]): McpServer =
    val t = tools(cluster)
    McpServer("ankka-flow", BuildInfo.version, t.instructions, t.all, t.resources)

  private def exchange(srv: McpServer, messages: String*): Vector[Json] =
    val log = new PrintStream(new ByteArrayOutputStream, true, UTF_8)
    messages.toVector.flatMap(m => srv.handleLine(m, log))

  private def call(srv: McpServer, tool: String, arguments: String): Json =
    exchange(
      srv,
      s"""{"jsonrpc":"2.0","id":9,"method":"tools/call","params":{"name":"$tool","arguments":$arguments}}"""
    ).head("result").getOrElse(fail("no result"))

  private def text(result: Json): String =
    result("content")
      .collect { case Json.Arr(xs) => xs.flatMap(_.string("text")).mkString }
      .getOrElse("")
  private def isError(result: Json): Boolean = result.boolean("isError").getOrElse(false)

  private val node = new ObjectMapper().readTree("{}")
  private def pipeline(name: String, phase: String, routerReplicas: Int = 1): AnkkaFlow =
    val f = AnkkaFlow(
      "shop",
      name,
      AnkkaFlowSpec(
        pipeline = name,
        streamlets = List(
          StreamletSpec(
            "router",
            "ghcr.io/x/router:1",
            routerReplicas,
            Map.empty,
            Map("in"  -> "t0"),
            Map("out" -> "t1"),
            node
          ),
          StreamletSpec(
            "sink",
            "ghcr.io/x/sink:1",
            0,
            Map.empty,
            Map("in" -> "t1"),
            Map.empty,
            node
          )
        )
      )
    )
    f.setStatus(
      AnkkaFlowStatus(
        phase = phase,
        detail = if phase == "Ready" then "" else "topic 't0' does not exist",
        streamlets = List(
          StreamletStatus("router", routerReplicas, routerReplicas),
          StreamletStatus("sink", 0, 0)
        ),
        topics = List(TopicStatus("t0", phase == "Ready"))
      )
    )
    f
  private def install(flow: AnkkaFlow): Unit =
    client
      .resources(classOf[AnkkaFlow])
      .inNamespace("shop")
      .resource(flow)
      .createOr(_.update()): Unit
  private def pod(pipeline: String, streamlet: String): String =
    val name = s"flow-$pipeline-$streamlet-x"
    client.pods
      .inNamespace("shop")
      .resource(
        new PodBuilder()
          .withMetadata(
            new ObjectMetaBuilder()
              .withName(name)
              .withLabels(
                java.util.Map.of(
                  "flow.ankka.thinkmorestupidless.com/pipeline",
                  pipeline,
                  "flow.ankka.thinkmorestupidless.com/streamlet",
                  streamlet
                )
              )
              .build()
          )
          .build()
      )
      .createOr(_.update())
    name

  // ── the protocol ──────────────────────────────────────────────────────────

  test("initialize agrees the client's version when it is one the server speaks, else the newest") {
    val srv = server(None)
    val agreed = exchange(
      srv,
      """{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18"}}"""
    ).head
    assertEquals(agreed("id"), Some(num(1)))
    assertEquals(agreed("result").flatMap(_.string("protocolVersion")), Some("2025-06-18"))
    assertEquals(
      agreed("result").flatMap(_("serverInfo")).flatMap(_.string("name")),
      Some("ankka-flow")
    )
    val newest = exchange(
      srv,
      """{"jsonrpc":"2.0","id":"a","method":"initialize","params":{"protocolVersion":"1999-01-01"}}"""
    ).head
    assertEquals(newest("id"), Some(str("a")))
    assertEquals(
      newest("result").flatMap(_.string("protocolVersion")),
      Some(McpServer.SupportedVersions.head)
    )
  }

  test(
    "a notification gets no reply; an unknown method, a bad line and an unknown tool are errors; the server keeps serving"
  ) {
    val replies = exchange(
      server(None),
      """{"jsonrpc":"2.0","method":"notifications/initialized"}""",
      """{"jsonrpc":"2.0","id":2,"method":"no/such"}""",
      "{not json",
      """{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"no_such_tool"}}""",
      """{"jsonrpc":"2.0","id":4,"method":"ping"}"""
    )
    assertEquals(
      replies.map(r => r("error").flatMap(_("code")).orElse(r("result").map(_ => Json.Null))),
      Vector(
        Some(num(McpServer.MethodNotFound)),
        Some(num(McpServer.ParseError)),
        Some(num(McpServer.InvalidParams)),
        Some(Json.Null)
      )
    )
  }

  test("started at a terminal the notice is on stderr, and stdout is only the protocol") {
    def run(interactive: Boolean) =
      val out = new ByteArrayOutputStream; val err = new ByteArrayOutputStream
      server(None).serve(
        new BufferedReader(new StringReader("""{"jsonrpc":"2.0","id":1,"method":"ping"}""")),
        new PrintStream(out, true, UTF_8),
        new PrintStream(err, true, UTF_8),
        interactive
      )
      (out.toString(UTF_8), err.toString(UTF_8))
    val (out, err) = run(true)
    assert(err.contains("claude mcp add ankka-flow -- flow mcp"), err)
    assertEquals(out.linesIterator.toVector, Vector("""{"jsonrpc":"2.0","id":1,"result":{}}"""))
    assertEquals(run(false)._2, "")
  }

  test(
    "every tool has a description, an object input schema and hints; apply and reset change the cluster, the rest are read-only"
  ) {
    val listed = exchange(server(None), """{"jsonrpc":"2.0","id":4,"method":"tools/list"}""")
      .head("result")
      .flatMap(_("tools")) match
      case Some(Json.Arr(xs)) => xs
      case other              => fail(s"no tools: $other")
    val names = listed.flatMap(_.string("name"))
    assertEquals(
      names.toSet,
      Set(
        "verify_blueprint",
        "generate_resource",
        "flow_version",
        "list_pipelines",
        "get_pipeline",
        "pipeline_logs",
        "pipeline_lag",
        "apply_pipeline",
        "reset_pipeline",
        "search_docs",
        "read_doc"
      )
    )
    listed.foreach { t =>
      assert(t.string("description").exists(_.nonEmpty), t.render)
      assertEquals(t("inputSchema").flatMap(_.string("type")), Some("object"), t.render)
      val a           = t("annotations").getOrElse(fail(s"no annotations: ${t.render}"))
      val destructive = Set("apply_pipeline", "reset_pipeline")(t.string("name").get)
      assertEquals(a.boolean("destructiveHint"), Some(destructive), t.string("name").get)
      assertEquals(a.boolean("readOnlyHint"), Some(!destructive), t.string("name").get)
    }
  }

  // ── the pure tools ────────────────────────────────────────────────────────

  private def flowOut(args: String*): CliFixtures.Result =
    CliFixtures.Driver.InProcess.run(args.toList, None)
  private val cart       = CliFixtures.cart
  private def q(p: Path) = p.toString

  test("verify_blueprint answers what flow verify prints, and refuses as it refuses") {
    val expected = flowOut(
      "verify",
      q(cart.resolve("blueprint.conf")),
      "--descriptors",
      q(cart.resolve("descriptors"))
    )
    val result = call(
      server(None),
      "verify_blueprint",
      s"""{"blueprint":"${q(cart.resolve("blueprint.conf"))}","descriptors":"${q(
          cart.resolve("descriptors")
        )}"}"""
    )
    assert(!isError(result))
    assertEquals(text(result), expected.out + expected.err)
    val broken = CliFixtures.variant(blueprint = _.replace("router.in", "router.nope"))
    val refused = call(
      server(None),
      "verify_blueprint",
      s"""{"blueprint":"${q(broken.resolve("blueprint.conf"))}","descriptors":"${q(
          broken.resolve("descriptors")
        )}"}"""
    )
    assert(isError(refused))
    assertEquals(
      text(refused) + "\n",
      flowOut(
        "verify",
        q(broken.resolve("blueprint.conf")),
        "--descriptors",
        q(broken.resolve("descriptors"))
      ).err
    )
  }

  test("generate_resource answers the YAML flow generate writes, and applies nothing") {
    val images = Files
      .readString(cart.resolve("images.conf"))
      .linesIterator
      .map(_.split("=").map(_.trim.stripPrefix("\"").stripSuffix("\"")))
      .map(a => a(0) -> a(1))
      .toMap
    val expected = flowOut(
      Seq(
        "generate",
        q(cart.resolve("blueprint.conf")),
        "--descriptors",
        q(cart.resolve("descriptors")),
        "--version",
        "t",
        "-n",
        "shop"
      ) ++ images.flatMap((k, v) => Seq("--image", s"$k=$v"))*
    )
    assertEquals(expected.code, 0, expected.err)
    val result = call(
      server(None),
      "generate_resource",
      s"""{"blueprint":"${q(cart.resolve("blueprint.conf"))}","descriptors":"${q(
          cart.resolve("descriptors")
        )}","version":"t","namespace":"shop","images":${Json
          .Obj(images.toVector.map((k, v) => k -> str(v)))
          .render}}"""
    )
    assert(!isError(result), text(result))
    assertEquals(text(result), expected.out + expected.err)
    assert(client.resources(classOf[AnkkaFlow]).inNamespace("shop").list().getItems.isEmpty)
  }

  test(
    "flow_version answers flow version's line; a missing blueprint is an error result naming it"
  ) {
    assertEquals(text(call(server(None), "flow_version", "{}")), flowOut("version").out)
    val r = call(server(None), "verify_blueprint", """{"blueprint":"/nowhere/blueprint.conf"}""")
    assert(isError(r) && text(r).contains("/nowhere/blueprint.conf"), text(r))
  }

  test(
    "every documentation page is a resource, search finds one by title, and read_doc returns it"
  ) {
    val srv = server(None)
    val listed = exchange(srv, """{"jsonrpc":"2.0","id":5,"method":"resources/list"}""")
      .head("result")
      .flatMap(_("resources")) match
      case Some(Json.Arr(xs)) => xs.flatMap(_.string("uri"))
      case other              => fail(s"no resources: $other")
    val pages = Files
      .walk(CliFixtures.repoRoot.resolve("docs"))
      .iterator
      .asScala
      .filter(_.toString.endsWith(".md"))
      .map(p => "ankka-flow://docs/" + CliFixtures.repoRoot.resolve("docs").relativize(p))
      .toSet
    assertEquals(listed.toSet, pages)
    val found = text(call(srv, "search_docs", """{"query":"Write a blueprint"}"""))
    assert(found.linesIterator.exists(_.startsWith("build/blueprints.md")), found)
    val page = text(call(srv, "read_doc", """{"path":"build/blueprints.md"}"""))
    assertEquals(page, Files.readString(CliFixtures.repoRoot.resolve("docs/build/blueprints.md")))
    val read = exchange(
      srv,
      """{"jsonrpc":"2.0","id":6,"method":"resources/read","params":{"uri":"ankka-flow://docs/build/blueprints.md"}}"""
    ).head("result")
    assertEquals(
      read.flatMap(_("contents")).collect { case Json.Arr(xs) => xs.head.string("text").get },
      Some(page)
    )
  }

  // ── the cluster tools ─────────────────────────────────────────────────────

  Seq(
    "list_pipelines" -> "{}",
    "get_pipeline"   -> """{"name":"cart"}""",
    "pipeline_logs"  -> """{"name":"cart","streamlet":"router"}""",
    "pipeline_lag"   -> """{"name":"cart"}""",
    "apply_pipeline" -> s"""{"blueprint":"${q(cart.resolve("blueprint.conf"))}"}""",
    "reset_pipeline" -> """{"name":"cart"}"""
  )
    .foreach { (tool, args) =>
      test(s"$tool refuses with no cluster named, saying what to write in flow.toml") {
        val r = call(server(None), tool, args)
        assert(isError(r), text(r))
        assert(
          text(r).contains("flow.toml") && text(r).contains("context") && text(r).contains(
            "namespace"
          ),
          text(r)
        )
      }
    }

  test("list_pipelines and get_pipeline read the named namespace: phases, status and events") {
    install(pipeline("cart", "Ready")); install(pipeline("audit", "Degraded"))
    client.v1.events
      .inNamespace("shop")
      .resource(
        new EventBuilder()
          .withMetadata(new ObjectMetaBuilder().withName("cart.1").withNamespace("shop").build())
          .withInvolvedObject(
            new ObjectReferenceBuilder().withKind("AnkkaFlow").withName("cart").build()
          )
          .withType("Normal")
          .withReason("TopicCreated")
          .withMessage("topic 't1' created")
          .withLastTimestamp("2026-10-07T10:00:00Z")
          .build()
      )
      .createOr(_.update())
    val srv  = server(Some(named))
    val list = text(call(srv, "list_pipelines", "{}"))
    assert(list.contains("audit  Degraded") && list.contains("cart  Ready"), list)
    val got = text(call(srv, "get_pipeline", """{"name":"cart"}"""))
    assert(
      got.contains("phase: Ready") && got.contains("router: image ghcr.io/x/router:1") && got
        .contains("TopicCreated"),
      got
    )
    val missing = call(srv, "get_pipeline", """{"name":"ghost"}""")
    assert(isError(missing) && text(missing).contains("no pipeline 'ghost'"), text(missing))
  }

  test("pipeline_logs reads a streamlet's process container, or its sidecar") {
    install(pipeline("cart", "Ready"))
    val name = pod("cart", "router")
    server
      .expect()
      .get()
      .withPath(
        s"/api/v1/namespaces/shop/pods/$name/log?pretty=false&container=process&tailLines=3"
      )
      .andReturn(200, "p1\np2\np3\n")
      .always()
    server
      .expect()
      .get()
      .withPath(
        s"/api/v1/namespaces/shop/pods/$name/log?pretty=false&container=sidecar&tailLines=3"
      )
      .andReturn(200, "s1\n")
      .always()
    val srv = server(Some(named))
    val process =
      text(call(srv, "pipeline_logs", """{"name":"cart","streamlet":"router","lines":3}"""))
    assert(process.contains(s"--- $name [process]") && process.contains("p2"), process)
    val sidecar = text(
      call(
        srv,
        "pipeline_logs",
        """{"name":"cart","streamlet":"router","container":"sidecar","lines":3}"""
      )
    )
    assert(sidecar.contains("s1"), sidecar)
    val unknown = call(srv, "pipeline_logs", """{"name":"cart","streamlet":"nope"}""")
    assert(isError(unknown) && text(unknown).contains("no streamlet 'nope'"), text(unknown))
  }

  test(
    "apply_pipeline creates or updates the resource on the named cluster, and sends nothing for a blueprint that does not verify"
  ) {
    val srv = server(Some(named))
    val imagesText = Json
      .Obj(Vector("router" -> str("ghcr.io/x/router:9"), "sink" -> str("ghcr.io/x/sink:9")))
      .render
    val r = call(
      srv,
      "apply_pipeline",
      s"""{"blueprint":"${q(cart.resolve("blueprint.conf"))}","descriptors":"${q(
          cart.resolve("descriptors")
        )}","version":"t","images":$imagesText}"""
    )
    assert(!isError(r), text(r))
    val applied = client.resources(classOf[AnkkaFlow]).inNamespace("shop").withName("cart").get()
    assert(
      applied != null && applied.getSpec.streamlets.exists(_.image == "ghcr.io/x/router:9"),
      text(r)
    )
    val again = call(
      srv,
      "apply_pipeline",
      s"""{"blueprint":"${q(cart.resolve("blueprint.conf"))}","descriptors":"${q(
          cart.resolve("descriptors")
        )}","version":"t2","images":$imagesText}"""
    )
    assert(!isError(again), text(again))
    assertEquals(
      client
        .resources(classOf[AnkkaFlow])
        .inNamespace("shop")
        .withName("cart")
        .get()
        .getSpec
        .version,
      "t2"
    )
    val broken = CliFixtures.variant(blueprint = _.replace("router.in", "router.nope"))
    val before = client.resources(classOf[AnkkaFlow]).inNamespace("shop").list().getItems.size
    val refused = call(
      srv,
      "apply_pipeline",
      s"""{"blueprint":"${q(broken.resolve("blueprint.conf"))}","descriptors":"${q(
          broken.resolve("descriptors")
        )}","images":$imagesText}"""
    )
    assert(isError(refused), text(refused))
    assertEquals(
      client.resources(classOf[AnkkaFlow]).inNamespace("shop").list().getItems.size,
      before
    )
  }

  test(
    "reset_pipeline requests the reset as flow reset does, and refuses a streamlet still running"
  ) {
    install(
      pipeline("orders", "Ready", routerReplicas = 0)
    ) // its own pipeline: no pods from other cases
    val srv = server(Some(named))
    val r   = call(srv, "reset_pipeline", """{"name":"orders"}""")
    assert(!isError(r), text(r))
    val request = ResetRequest
      .request(client.resources(classOf[AnkkaFlow]).inNamespace("shop").withName("orders").get())
      .getOrElse(fail("no reset annotation"))
    assert(text(r).contains(request.id), text(r))
    install(pipeline("orders", "Ready", routerReplicas = 2))
    val refused = call(srv, "reset_pipeline", """{"name":"orders","streamlets":["router"]}""")
    assert(isError(refused) && text(refused).contains("[router] is not scaled to 0"), text(refused))
  }

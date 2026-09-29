package com.thinkmorestupidless.ankka.flow.blueprint

import com.thinkmorestupidless.ankka.flow.protocol.Builtins

import Builders.*

/**
 * A blueprint names a built-in descriptor as `builtin/<name>`: it resolves only by that name, a
 * descriptor file of the same bare name neither shadows it nor is shadowed by it, and an unknown
 * built-in names the ones that exist.
 */
class BuiltinRefSuite extends munit.FunSuite:

  private val sink = StreamletDescriptor(Builtins.neo4jMergeSink.getStreamlet, builtin = true)

  private val mapper = descriptor(
    name = "mapper",
    outlets = Seq("deltas" -> json(Builtins.GraphDeltaSchema))
  )

  private def blueprint(graphDescriptor: String, descriptors: Vector[StreamletDescriptor]) =
    Blueprint
      .parseString(
        s"""blueprint {
         |  streamlets {
         |    mapper = mapper
         |    graph  = $graphDescriptor
         |  }
         |  topics {
         |    deltas { producers = [mapper.deltas], consumers = [graph.in] }
         |  }
         |}
         |""".stripMargin,
        descriptors
      )
      .verify

  test("builtin/<name> resolves to the built-in and its inlet is verified like any port") {
    val b = blueprint("builtin/neo4j-merge-sink", Vector(mapper, sink))
    assertEquals(b.problems, Vector.empty)
    assert(b.streamlets.find(_.name == "graph").flatMap(_.verified).exists(_.descriptor.builtin))
  }

  test("a built-in is not found by its bare name") {
    val b = blueprint("neo4j-merge-sink", Vector(mapper, sink))
    assertEquals(
      b.problems.collect { case p: StreamletDescriptorNotFound => p },
      Vector(StreamletDescriptorNotFound("graph", "neo4j-merge-sink"))
    )
  }

  test("a descriptor file with the built-in's name neither shadows it nor is matched by builtin/") {
    val impostor = descriptor(name = "neo4j-merge-sink", inlets = Seq("in" -> json("other.v1")))
    val b        = blueprint("builtin/neo4j-merge-sink", Vector(mapper, impostor, sink))
    assertEquals(b.problems, Vector.empty)
    assert(b.streamlets.find(_.name == "graph").flatMap(_.verified).exists(_.descriptor.builtin))
    val bare = blueprint("neo4j-merge-sink", Vector(mapper, impostor, sink))
    assert(bare.problems.exists(_.isInstanceOf[IncompatibleSchema]), bare.problems.toString)
  }

  test("an unknown built-in lists the built-ins that exist") {
    val b       = blueprint("builtin/nope", Vector(mapper, sink))
    val unknown = b.problems.collect { case p: UnknownBuiltin => p }
    assertEquals(
      unknown,
      Vector(UnknownBuiltin("graph", "builtin/nope", Vector("neo4j-merge-sink")))
    )
    assertEquals(
      BlueprintProblem.toMessage(unknown.head),
      "Streamlet 'graph' names built-in descriptor 'builtin/nope', which this version does not have; the built-ins are: neo4j-merge-sink."
    )
  }

  test("a blueprint whose only streamlet is built in verifies with no descriptor files") {
    val b = Blueprint
      .parseString(
        """blueprint {
          |  streamlets { graph = builtin/neo4j-merge-sink }
          |  topics {
          |    deltas { managed = false, bootstrap.servers = "kafka:9092", consumers = [graph.in] }
          |  }
          |}
          |""".stripMargin,
        Vector(sink)
      )
      .verify
    assertEquals(b.problems, Vector.empty)
  }

  test("an image for a built-in streamlet has its own message") {
    assertEquals(
      BlueprintProblem.toMessage(BuiltinHasImage("graph")),
      "Streamlet 'graph' is built in and takes no image."
    )
  }

package com.thinkmorestupidless.ankka.flow.blueprint

import com.thinkmorestupidless.ankka.flow.protocol.Builtins
import com.typesafe.config.ConfigFactory

import Builders.*
import DeltaTopics.Decision

/**
 * Every row of the delta topic decision table (specs/003-compacted-delta-topics/data-model.md): a
 * topic with a port of the graph delta contract is compacted by default when the pipeline owns it,
 * keeps a policy it was given, and is left to its owner when it is not managed.
 */
class DeltaTopicsSuite extends munit.FunSuite:

  private val mapper = descriptor(
    name = "mapper",
    inlets = Seq("in" -> json("notices.v1")),
    outlets = Seq("deltas" -> json(Builtins.GraphDeltaSchema))
  )
  private val sink = StreamletDescriptor(Builtins.neo4jMergeSink.getStreamlet, builtin = true)

  /** The verified topic `deltas` of a blueprint whose topic block has `settings` in it. */
  private def topic(settings: String, consumers: String = "consumers = [graph.in]") =
    Blueprint
      .parseString(
        s"""blueprint {
           |  streamlets {
           |    mapper = mapper
           |    graph  = builtin/neo4j-merge-sink
           |  }
           |  topics {
           |    notices { managed = false, bootstrap.servers = "kafka:9092", consumers = [mapper.in] }
           |    deltas { $consumers, $settings }
           |  }
           |}
           |""".stripMargin,
        Vector(mapper, sink)
      )
      .verified
      .fold(ps => fail(ps.map(BlueprintProblem.toMessage).mkString("; ")), identity)
      .topics
      .find(_.id == "deltas")
      .get

  private def decide(t: VerifiedTopic) =
    DeltaTopics.decide(t, TopicSettings.fromConfig(t.kafkaConfig).topicConfig)

  private def noted(t: VerifiedTopic) = decide(t).map(DeltaTopics.note(t, _))

  test("a managed delta topic with no cleanup policy is compacted, and says so") {
    val t = topic("producers = [mapper.deltas]")
    assertEquals(decide(t), Some(Decision.Compacted))
    assertEquals(
      noted(t),
      Some("Topic 'deltas' carries graph deltas and is compacted (cleanup.policy = compact).")
    )
  }

  test("a producing port alone makes a delta topic: its sink may be in another pipeline") {
    val t = Blueprint
      .parseString(
        """blueprint {
          |  streamlets { mapper = mapper }
          |  topics {
          |    notices { managed = false, bootstrap.servers = "kafka:9092", consumers = [mapper.in] }
          |    deltas { producers = [mapper.deltas] }
          |  }
          |}
          |""".stripMargin,
        Vector(mapper)
      )
      .verified
      .toOption
      .get
      .topics
      .find(_.id == "deltas")
      .get
    assertEquals(t.consumers, Vector.empty)
    assertEquals(decide(t), Some(Decision.Compacted))
  }

  test("a consuming port alone makes a delta topic") {
    val t = topic("partitions = 3")
    assertEquals(t.producers, Vector.empty)
    assertEquals(decide(t), Some(Decision.Compacted))
  }

  test("a blueprint that sets compact itself is kept, with the same note") {
    val t = topic("producers = [mapper.deltas], topic { cleanup.policy = compact }")
    assertEquals(decide(t), Some(Decision.Kept("compact")))
    assertEquals(
      noted(t),
      Some("Topic 'deltas' carries graph deltas and is compacted (cleanup.policy = compact).")
    )
  }

  test("compact,delete is kept, and the note says what a rebuild loses") {
    val t = topic("""producers = [mapper.deltas], topic { cleanup.policy = "compact,delete" }""")
    assertEquals(decide(t), Some(Decision.Kept("compact,delete")))
    assertEquals(
      noted(t),
      Some(
        "Topic 'deltas' carries graph deltas and sets cleanup.policy = compact,delete; records older than its retention are gone from a rebuild."
      )
    )
    // any order, any spacing: the policy is printed as set
    val spaced =
      topic("""producers = [mapper.deltas], topic { cleanup.policy = "delete, compact" }""")
    assertEquals(
      noted(spaced),
      Some(
        "Topic 'deltas' carries graph deltas and sets cleanup.policy = delete, compact; records older than its retention are gone from a rebuild."
      )
    )
  }

  test("a policy without compact is kept, and the note says the topic cannot rebuild the graph") {
    val t = topic("producers = [mapper.deltas], topic { cleanup.policy = delete }")
    assertEquals(decide(t), Some(Decision.Kept("delete")))
    assertEquals(
      noted(t),
      Some(
        "Topic 'deltas' carries graph deltas and sets cleanup.policy = delete; it will not hold the whole graph and cannot be relied on to rebuild it."
      )
    )
  }

  test("an unmanaged delta topic is its owner's, whatever it sets") {
    val t = topic("""managed = false, bootstrap.servers = "kafka:9092"""")
    assertEquals(decide(t), Some(Decision.NotOurs))
    assertEquals(
      noted(t),
      Some(
        "Topic 'deltas' carries graph deltas and is not managed; whether it is compacted is its owner's."
      )
    )
  }

  test("a topic with no port of the delta contract is not decided at all") {
    val (source, drain) = (ingress(), egress())
    val plain = Draft()
      .define(source, drain)
      .use("source", source)
      .use("drain", drain)
      .connect("foos", "source.out", "drain.in")
      .blueprint
      .verified
      .toOption
      .get
      .topics
      .head
    assertEquals(plain.connections.size, 2)
    assertEquals(DeltaTopics.decide(plain, Map("cleanup.policy" -> "compact")), None)
    assertEquals(DeltaTopics.decide(plain, Map.empty), None)
    val unconnected = VerifiedTopic("unconnected", Vector.empty, None, ConfigFactory.empty())
    assertEquals(DeltaTopics.decide(unconnected, Map.empty), None)
  }

  test("the deploy-time policy is the one decided on") {
    val t = topic("producers = [mapper.deltas]")
    assertEquals(
      DeltaTopics.decide(t, Map("cleanup.policy" -> "delete")),
      Some(Decision.Kept("delete"))
    )
  }

  test("policies are read from a comma-separated value") {
    assertEquals(DeltaTopics.policies(" compact , delete "), Set("compact", "delete"))
    assert(DeltaTopics.compacts("delete,compact"))
    assert(!DeltaTopics.compacts("delete"))
    assert(!DeltaTopics.compacts(""))
  }

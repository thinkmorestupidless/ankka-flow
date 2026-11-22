package com.thinkmorestupidless.ankka.flow.operator

import com.thinkmorestupidless.ankka.flow.crd.*
import com.thinkmorestupidless.ankka.flow.descriptor.{Delivery, Settings as FlowSettings}

import Fixtures.*

/**
 * The cart pipeline with the cleaner reading, and the counter writing, ankka topics on a machine's
 * cluster: never created or deleted, every inlet's group under the machine's prefix in the
 * settings, the status and a reset, and the Flink outlet at least once.
 */
class AnkkaTopicRenderingSuite extends munit.FunSuite:

  private val Prefix = "ankka.machine.affiliates.network."

  private val machine =
    KafkaCluster(
      "pipelines",
      "broker.ankka.svc:9092",
      machine = Some(Machine("affiliates", "network"))
    )

  private val o = flinkObserved.copy(clusters = flinkObserved.clusters + ("pipelines" -> machine))

  private def onAnkka(replicas: Int = 1, delete: Boolean = false): AnkkaFlow =
    val r = cartWithCounter(counterReplicas = replicas)
    r.setSpec(
      r.getSpec.copy(
        onDelete = if delete then OnDelete(OnDelete.Delete) else r.getSpec.onDelete,
        streamlets =
          if replicas == 0 then r.getSpec.streamlets.map(_.copy(replicas = 0))
          else r.getSpec.streamlets,
        topics = r.getSpec.topics.map {
          case t if t.id == "cart-events" =>
            TopicSpec(
              id = t.id,
              name = "checkout.cart-checkouts",
              managed = false,
              ankka = Some(AnkkaRef("checkout", "cart-checkouts")),
              cluster = Some("pipelines")
            )
          case t if t.id == "cart-counts" =>
            TopicSpec(
              id = t.id,
              name = "checkout.cart-counts",
              managed = false,
              ankka = Some(AnkkaRef("checkout", "cart-counts")),
              cluster = Some("pipelines")
            )
          case t => t
        }
      )
    )
    r

  private def render(r: AnkkaFlow) = Rendering.render(r, settings, o, "t")

  private def settingsOf(r: Rendering.Rendered, streamlet: String): FlowSettings =
    val secret = r.actions.collect {
      case Action.EnsureSecret(s) if s.getMetadata.getName == s"flow-cart-$streamlet" => s
    }.head
    FlowSettings.parse(secret.getStringData.get(StreamletFiles.Key)).toOption.get

  test("an ankka topic is never created, and never deleted with the pipeline") {
    val created = render(onAnkka()).actions.collect { case Action.EnsureTopic(t) => t.name }
    assertEquals(created.toSet, Set("cart.clean-carts"))
    val gone = onAnkka(delete = true)
    gone.getMetadata.setDeletionTimestamp("2026-11-22T00:00:00Z")
    val deleted = render(gone).actions.collect { case Action.DeleteTopic(t) => t.name }
    assertEquals(deleted.toSet, Set("cart.clean-carts"))
  }

  test("an inlet on the machine's cluster reads with the machine's group; others without") {
    val r = render(onAnkka())
    assertEquals(
      settingsOf(r, "cleaner").inlets.map(i => (i.topic, i.group)),
      Vector(("checkout.cart-checkouts", s"${Prefix}cart.cleaner.in"))
    )
    assertEquals(settingsOf(r, "counter").inlets.map(_.group), Vector("cart.counter.in"))
  }

  test("the status names the machine's group for that inlet") {
    val r      = render(onAnkka())
    val status = r.actions.collect { case Action.SetStatus(s) => s }.head
    assertEquals(
      status.streamlets.find(_.name == "cleaner").toList.flatMap(_.inlets).map(_.group),
      List(s"${Prefix}cart.cleaner.in")
    )
  }

  test("a reset moves the machine's group") {
    val stopped =
      ResetRequest.withRequest(onAnkka(replicas = 0), ResetRequest.Request("a-1", List("cleaner")))
    val groups = render(stopped).actions.collect { case Action.ResetGroup(t) => t.groupId }
    assertEquals(groups, Vector(s"${Prefix}cart.cleaner.in"))
  }

  test("a Flink streamlet writes an ankka topic at least once, with no transactional id") {
    val Vector(out) = settingsOf(render(onAnkka()), "counter").outlets: @unchecked
    assertEquals(
      (out.topic, out.delivery, out.transactionalIdPrefix),
      ("checkout.cart-counts", Delivery.AtLeastOnce, None)
    )
  }

  // ── Grants ────────────────────────────────────────────────────────────────────────────────

  private def withGrants(g: (String, Grant)*) = o.copy(grants = g.toMap)

  private def statusOf(r: Rendering.Rendered) = r.actions.collect { case Action.SetStatus(s) =>
    s
  }.head
  private def eventsOf(r: Rendering.Rendered) = r.actions.collect { case e: Action.RecordEvent =>
    e
  }

  private val readMessage =
    "machine affiliates/network holds no grant to read topic checkout.cart-checkouts: an owner of project checkout runs `ankka projects grants make machine:affiliates/network topic cart-checkouts consume -p checkout`"
  private val writeMessage =
    "machine affiliates/network holds no grant to write topic checkout.cart-counts: an owner of project checkout runs `ankka projects grants make machine:affiliates/network topic cart-counts produce -p checkout`"

  test(
    "a topic the machine may not read: TopicNotGranted, reason Read, naming the command; Degraded"
  ) {
    val r = Rendering.render(
      onAnkka(),
      settings,
      withGrants("cart-events" -> Grant.Lacks(Grant.Right.Read), "cart-counts" -> Grant.Granted),
      "t"
    )
    val c = statusOf(r).conditions.find(_.`type` == Condition.TopicNotGranted).get
    assertEquals((c.status, c.reason, c.message), (Condition.True, "Read", readMessage))
    assertEquals(statusOf(r).phase, AnkkaFlowStatus.Degraded)
    assert(eventsOf(r).exists(e => e.reason == Condition.TopicNotGranted && e.note == readMessage))
  }

  test(
    "a topic the machine may not write: reason Write, and every topic it holds for in one message"
  ) {
    val r = Rendering.render(
      onAnkka(),
      settings,
      withGrants(
        "cart-events" -> Grant.Lacks(Grant.Right.Read),
        "cart-counts" -> Grant.Lacks(Grant.Right.Write)
      ),
      "t"
    )
    val c = statusOf(r).conditions.find(_.`type` == Condition.TopicNotGranted).get
    assert(c.message.contains(readMessage) && c.message.contains(writeMessage), c.message)
  }

  test("refused credentials: MachineCredentialRefused, Degraded, with the broker's reason") {
    val r = Rendering.render(
      onAnkka(),
      settings,
      withGrants("cart-events" -> Grant.CredentialRefused("Authentication failed: invalid_client")),
      "t"
    )
    val c = statusOf(r).conditions.find(_.`type` == Condition.MachineCredentialRefused).get
    assertEquals(
      c.message,
      "ankka refuses the credentials of machine affiliates/network: Authentication failed: invalid_client"
    )
    assertEquals(statusOf(r).phase, AnkkaFlowStatus.Degraded)
    assert(eventsOf(r).exists(_.reason == Condition.MachineCredentialRefused))
  }

  test("the streamlets are rendered regardless of a missing grant") {
    val r = Rendering.render(
      onAnkka(),
      settings,
      withGrants("cart-events" -> Grant.Lacks(Grant.Right.Read)),
      "t"
    )
    assert(r.actions.exists { case Action.ApplyDeployment(_) => true; case _ => false })
    assert(r.actions.exists { case Action.ApplyFlinkDeployment(_) => true; case _ => false })
  }

  test(
    "the condition clears on the next pass that is granted, and an unknown answer concludes nothing"
  ) {
    val lacking = onAnkka()
    lacking.setStatus(
      statusOf(
        Rendering.render(
          lacking,
          settings,
          withGrants("cart-events" -> Grant.Lacks(Grant.Right.Read)),
          "t"
        )
      )
    )
    val again = Rendering.render(
      lacking,
      settings,
      withGrants("cart-events" -> Grant.Lacks(Grant.Right.Read)),
      "t2"
    )
    assert(!eventsOf(again).exists(_.reason == Condition.TopicNotGranted), "recorded once")
    for g <- Seq(Grant.Granted, Grant.Unknown("timed out")) do
      val next = Rendering.render(lacking, settings, withGrants("cart-events" -> g), "t3")
      assertEquals(
        statusOf(next).conditions.map(_.`type`).filter(_ != Condition.Ready),
        Nil,
        g.toString
      )
  }

package com.thinkmorestupidless.ankka.flow.operator

import com.thinkmorestupidless.ankka.flow.crd.*

import Fixtures.*

/** A pending reset, through Rendering (FR-023, S4.1-S4.3). */
class ResetRenderingSuite extends munit.FunSuite:

  private def requested(
      r: AnkkaFlow,
      req: ResetRequest.Request,
      done: Option[String] = None
  ): AnkkaFlow =
    val f = ResetRequest.withRequest(r, req)
    done.foreach(id => f.getMetadata.getAnnotations.put(ResetRequest.DoneAnnotation, id))
    f

  private def stopped = cart(cleanerReplicas = 0).tap(f =>
    f.setSpec(f.getSpec.copy(streamlets = f.getSpec.streamlets.map(_.copy(replicas = 0))))
  )

  extension [A](a: A) private def tap(f: A => Unit): A = { f(a); a }

  private def render(r: AnkkaFlow, o: Observed = observed) =
    Rendering.render(r, settings, o, "t").actions

  test(
    "streamlets scaled to zero with no pods: one group reset per inlet over its own connection, then the done marker"
  ) {
    val actions = render(requested(stopped, ResetRequest.Request("r1")))
    val resets  = actions.collect { case Action.ResetGroup(t) => t }
    assertEquals(resets.map(_.groupId).toSet, Set("cart.cleaner.in", "cart.sink.in"))
    assertEquals(
      resets.find(_.groupId == "cart.cleaner.in").get.topic.bootstrapServers,
      "shop-kafka:9092"
    )
    assert(
      actions.indexOf(Action.MarkResetDone("r1")) > actions.lastIndexWhere(
        _.isInstanceOf[Action.ResetGroup]
      )
    )
  }

  test("a named streamlet only") {
    val resets = render(requested(stopped, ResetRequest.Request("r1", List("cleaner")))).collect {
      case Action.ResetGroup(t) => t.groupId
    }
    assertEquals(resets, Vector("cart.cleaner.in"))
  }

  test("a streamlet still running refuses the reset and leaves it pending (S4.2)") {
    val running = render(requested(cart(), ResetRequest.Request("r1")))
    assert(running.exists {
      case Action.RecordEvent("ResetRefused", _, note) => note.contains("not scaled to 0");
      case _                                           => false
    })
    assert(
      !running.exists(_.isInstanceOf[Action.ResetGroup]) && !running.contains(
        Action.MarkResetDone("r1")
      )
    )
    val pods = render(
      requested(stopped, ResetRequest.Request("r1")),
      observed.copy(pods = Map("cleaner" -> 1))
    )
    assert(pods.exists {
      case Action.RecordEvent("ResetRefused", _, note) => note.contains("still has 1 pod");
      case _                                           => false
    })
  }

  test("a request already carried out is never repeated (S4.3)") {
    val again = render(requested(stopped, ResetRequest.Request("r1"), done = Some("r1")))
    assert(!again.exists(_.isInstanceOf[Action.ResetGroup]))
  }

  // ── A Flink streamlet: its state goes with its offsets (R12) ─────────────────────────────────

  private def suspendedJob =
    FlinkState("ghcr.io/example/cart-counter:0.6.0", "h", "suspended", Some("SUSPENDED"))
  private def runningJob = FlinkState(
    "ghcr.io/example/cart-counter:0.6.0",
    "h",
    "running",
    Some("STABLE"),
    Some("RUNNING")
  )

  /** Only the counter is named, so the cleaner's replicas do not matter. */
  private def counterReset(replicas: Int) =
    requested(
      cartWithCounter(counterReplicas = replicas),
      ResetRequest.Request("r2", List("counter"))
    )

  test("a reset of a running Flink streamlet is refused and stays pending") {
    for (replicas, job) <- Seq(1 -> runningJob, 0 -> runningJob, 1 -> suspendedJob) do
      val actions =
        render(counterReset(replicas), flinkObserved.copy(flinkDeployments = Map("counter" -> job)))
      assert(
        actions.exists {
          case Action.RecordEvent("ResetRefused", _, note) => note.contains("counter")
          case _                                           => false
        },
        s"$replicas $job: $actions"
      )
      assert(
        !actions.exists {
          case _: Action.ResetGroup | _: Action.DeleteFlinkDeployment |
              _: Action.AdvanceStateEpoch | _: Action.MarkResetDone =>
            true
          case _ => false
        },
        actions
      )
  }

  test("a reset of a suspended Flink streamlet discards its state, then resets, then is done") {
    val actions =
      render(counterReset(0), flinkObserved.copy(flinkDeployments = Map("counter" -> suspendedJob)))
    val reset = actions.filter {
      case _: Action.DeleteFlinkDeployment | _: Action.AdvanceStateEpoch | _: Action.ResetGroup |
          _: Action.MarkResetDone =>
        true
      case Action.RecordEvent("StateDiscarded", _, _) => true
      case _                                          => false
    }
    assertEquals(
      reset.map {
        case Action.DeleteFlinkDeployment(ns, n) => s"delete $ns/$n"
        case Action.AdvanceStateEpoch(s)         => s"epoch $s"
        case Action.ResetGroup(t)                => s"group ${t.groupId}"
        case Action.RecordEvent(r, _, _)         => r
        case Action.MarkResetDone(id)            => s"done $id"
        case other                               => other.describe
      },
      Vector(
        "delete shop/flow-cart-counter",
        "epoch counter",
        "group cart.counter.in",
        "StateDiscarded",
        "done r2"
      )
    )
    val Some(Action.RecordEvent(_, _, note)) = actions.find {
      case Action.RecordEvent("StateDiscarded", _, _) => true
      case _                                          => false
    }: @unchecked
    assert(
      note.contains("epoch 2") && note.contains("s3://flow-state/dev/shop/cart/counter/1"),
      note
    )
    // the deployment it deletes is not applied again in the same pass
    assert(!actions.exists(_.isInstanceOf[Action.ApplyFlinkDeployment]), actions)
  }

  test("a suspended streamlet whose FlinkDeployment is already gone is reset the same way") {
    val actions = render(counterReset(0), flinkObserved)
    assert(actions.contains(Action.AdvanceStateEpoch("counter")), actions)
    assert(actions.contains(Action.MarkResetDone("r2")), actions)
  }

  test("the next render uses epoch 2 in every path and in the transactional id prefix") {
    val r         = cartWithCounter(counterReplicas = 0)
    val o         = flinkObserved.copy(stateEpochs = Map("counter" -> 2))
    val actions   = render(r, o)
    val Vector(d) = actions.collect { case Action.ApplyFlinkDeployment(d) => d }: @unchecked
    val config = d.getAdditionalProperties
      .get("spec")
      .asInstanceOf[java.util.Map[String, AnyRef]]
      .get("flinkConfiguration")
      .asInstanceOf[java.util.Map[String, String]]
    Seq(
      "execution.checkpointing.dir",
      "execution.checkpointing.savepoint-dir",
      "high-availability.storageDir"
    )
      .foreach(k => assert(config.get(k).contains("/counter/2/"), config.get(k)))
    val secret = actions.collect {
      case Action.EnsureSecret(s) if s.getMetadata.getName == "flow-cart-counter" => s
    }.head
    assert(
      secret.getStringData
        .get("streamlet.properties")
        .contains("transactional-id-prefix=flow.shop.cart.counter.out.e2"),
      secret.getStringData
    )
  }

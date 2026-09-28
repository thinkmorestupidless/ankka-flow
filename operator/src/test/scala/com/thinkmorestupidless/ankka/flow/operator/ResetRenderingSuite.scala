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

  private def stopped = cart(routerReplicas = 0).tap(f =>
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
    assertEquals(resets.map(_.groupId).toSet, Set("cart.router.in", "cart.sink.in"))
    assertEquals(
      resets.find(_.groupId == "cart.router.in").get.topic.bootstrapServers,
      "shop-kafka:9092"
    )
    assert(
      actions.indexOf(Action.MarkResetDone("r1")) > actions.lastIndexWhere(
        _.isInstanceOf[Action.ResetGroup]
      )
    )
  }

  test("a named streamlet only") {
    val resets = render(requested(stopped, ResetRequest.Request("r1", List("router")))).collect {
      case Action.ResetGroup(t) => t.groupId
    }
    assertEquals(resets, Vector("cart.router.in"))
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
      observed.copy(pods = Map("router" -> 1))
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

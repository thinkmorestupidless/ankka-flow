package com.thinkmorestupidless.ankka.flow.operator

import com.thinkmorestupidless.ankka.flow.crd.*

import Fixtures.*

/** Each row of data-model's Flink lifecycle table, and the phase and conditions they give. */
class LifecycleRulesSuite extends munit.FunSuite:

  private val resource = cartWithCounter()
  private val image    = "ghcr.io/example/cart-counter:0.6.0"

  private def hashes(o: Observed) =
    Rendering.render(resource, settings, o, "t").hashes

  private val rendered = hashes(flinkObserved)
  private val cleanerUp =
    DeploymentState(rendered("cleaner"), "ghcr.io/example/cart-cleaner:0.6.0", 1, 1, 1, 1, 1)

  private def status(job: Option[FlinkState], r: AnkkaFlow = resource) =
    val o = flinkObserved.copy(
      deployments = Map("cleaner" -> cleanerUp),
      flinkDeployments = job.map("counter" -> _).toMap,
      topics = Map("shop.cart-events.v1" -> TopicState.Exists(3, 1, Map.empty))
    )
    val topics =
      r.getSpec.topics.flatMap(t => TopicResolution.resolve(t, o.clusters).toOption).toVector
    LifecycleRules.status(r, hashes(flinkObserved), topics, o, "2026-11-22T10:00:00Z")

  private def job(
      lifecycle: String,
      jobState: Option[String] = None,
      jm: Option[String] = None,
      error: Option[String] = None,
      jobImage: String = image,
      lastStable: Option[String] = None
  ) = FlinkState(
    jobImage,
    rendered("counter"),
    "running",
    Some(lifecycle),
    jobState,
    jm,
    error,
    lastStable
  )

  private def counter(s: AnkkaFlowStatus) = s.streamlets.find(_.name == "counter").get

  private def conditionTypes(s: AnkkaFlowStatus) =
    s.conditions.filter(_.status == Condition.True).map(_.`type`)

  test("absent is rolling out, and the pipeline is pending") {
    val s = status(None)
    assertEquals((counter(s).ready, counter(s).detail), (0, "rolling out"))
    assertEquals(s.phase, AnkkaFlowStatus.Pending)
  }

  for lifecycle <- Seq("CREATED", "UPGRADING", "DEPLOYED") do
    test(s"lifecycle $lifecycle is rolling out") {
      val s = status(Some(job(lifecycle)))
      assertEquals(counter(s).detail, "rolling out")
      assertEquals(s.phase, AnkkaFlowStatus.Pending)
    }

  for jm <- Seq("DEPLOYING", "DEPLOYED_NOT_READY") do
    test(s"job manager $jm is rolling out") {
      val s = status(Some(job("DEPLOYED", jm = Some(jm))))
      assertEquals(counter(s).detail, "rolling out")
    }

  private val CounterInlet = InletStatus("in", "cart.clean-carts", "cart.counter.in", None)

  test("STABLE with its job RUNNING is running, ready 1, and the pipeline Ready") {
    val s = status(Some(job("STABLE", Some("RUNNING"), Some("READY"))))
    assertEquals(
      counter(s),
      StreamletStatus("counter", "flink", 1, 1, "running", List(CounterInlet))
    )
    assertEquals(s.phase, AnkkaFlowStatus.Ready)
    assertEquals(s.conditions.find(_.`type` == Condition.Ready).map(_.status), Some(Condition.True))
    assertEquals(s.detail, "")
  }

  test("STABLE with its job not yet RUNNING is still rolling out") {
    assertEquals(status(Some(job("STABLE", Some("CREATED")))).phase, AnkkaFlowStatus.Pending)
  }

  test("an older spec still running is rolling out to the new one") {
    val s = status(Some(job("STABLE", Some("RUNNING"), jobImage = "old")))
    assertEquals(counter(s).detail, "rolling out")
  }

  test("SUSPENDED is stopped, desired 0, and counts as running at that count") {
    val stopped = cartWithCounter(counterReplicas = 0)
    val o       = flinkObserved
    val h       = Rendering.render(stopped, settings, o, "t").hashes
    val f       = FlinkState(image, h("counter"), "suspended", Some("SUSPENDED"))
    val s = LifecycleRules.status(
      stopped,
      h,
      Vector.empty,
      o.copy(
        deployments = Map("cleaner" -> cleanerUp.copy(configHash = h("cleaner"))),
        flinkDeployments = Map("counter" -> f)
      ),
      "t"
    )
    assertEquals(
      counter(s),
      StreamletStatus(
        "counter",
        "flink",
        0,
        0,
        "suspended",
        List(CounterInlet.copy(topic = "clean-carts"))
      )
    )
    assertEquals(s.phase, AnkkaFlowStatus.Ready)
  }

  test("FAILED is failed with status.error as its detail, and the pipeline Degraded") {
    val s = status(Some(job("FAILED", error = Some("java.lang.OutOfMemoryError: Java heap space"))))
    assertEquals(counter(s).detail, "java.lang.OutOfMemoryError: Java heap space")
    assertEquals(s.phase, AnkkaFlowStatus.Degraded)
    assertEquals(conditionTypes(s), Nil)
    assertEquals(
      s.conditions.find(_.`type` == Condition.Ready).map(_.status),
      Some(Condition.False)
    )
  }

  test("job manager ERROR is failed") {
    val s = status(Some(job("DEPLOYED", jm = Some("ERROR"), error = Some("pods crash"))))
    assertEquals((counter(s).detail, s.phase), ("pods crash", AnkkaFlowStatus.Degraded))
  }

  for text <- LifecycleRules.StateRestoreFailures do
    test(s"failed after a new image with '$text' is StateUnreadable") {
      val s = status(
        Some(
          job(
            "FAILED",
            error = Some(s"org.apache.flink.util.FlinkException: $text: boom"),
            lastStable = Some("old")
          )
        )
      )
      assertEquals(conditionTypes(s), List(Condition.StateUnreadable))
      assertEquals(counter(s).detail, LifecycleRules.stateUnreadableMessage("counter"))
      assert(counter(s).detail.contains("deploy the previous image, or reset"))
      assertEquals(s.phase, AnkkaFlowStatus.Degraded)
    }

  test("a restore failure with the same image is not StateUnreadable") {
    val s =
      status(Some(job("FAILED", error = Some("StateMigrationException"), lastStable = Some(image))))
    assertEquals(conditionTypes(s), Nil)
  }

  // V6: what a FlinkDeployment shows when its new image cannot restore the state. The deployment
  // stays DEPLOYED, the job restarts without end, status.error is empty, and the Flink operator
  // records JobException events whose message is the top of the trace.
  private val restoreException =
    "java.lang.Exception: Exception while creating StreamOperatorStateContext.\n\tat org.apache.flink.streaming.api.operators.StreamTaskStateInitializerImpl.streamOperatorStateContext"

  test("a new image whose job restarts on a state restore exception is StateUnreadable") {
    for jobState <- Seq("RESTARTING", "RUNNING", "CREATED") do
      val restarting = job("DEPLOYED", Some(jobState), Some("READY"), lastStable = Some("old"))
        .copy(jobException = Some(restoreException))
      val s = status(Some(restarting))
      assertEquals(conditionTypes(s), List(Condition.StateUnreadable), jobState)
      assertEquals(counter(s).detail, LifecycleRules.stateUnreadableMessage("counter"))
      assertEquals(s.phase, AnkkaFlowStatus.Degraded)
  }

  test("the same exception with the image that last ran is a failure, not StateUnreadable") {
    val s = status(
      Some(
        job("DEPLOYED", Some("RESTARTING"), Some("READY"), lastStable = Some(image))
          .copy(jobException = Some(restoreException))
      )
    )
    assertEquals(conditionTypes(s), Nil)
  }

  test("another job exception after a new image is still rolling out") {
    val s = status(
      Some(
        job("DEPLOYED", Some("RESTARTING"), Some("READY"), lastStable = Some("old"))
          .copy(jobException = Some("java.net.ConnectException: Connection refused"))
      )
    )
    assertEquals(conditionTypes(s), Nil)
    assertEquals(s.phase, AnkkaFlowStatus.Pending)
  }

  test("another failure after a new image is not StateUnreadable") {
    val s =
      status(Some(job("FAILED", error = Some("Connection refused"), lastStable = Some("old"))))
    assertEquals(conditionTypes(s), Nil)
    assertEquals(counter(s).detail, "Connection refused")
  }

  test("Ready only with every streamlet running: a rolling Pekko Streams streamlet holds it") {
    val o = flinkObserved.copy(
      deployments = Map("cleaner" -> cleanerUp.copy(updatedReplicas = 0)),
      flinkDeployments = Map("counter" -> job("STABLE", Some("RUNNING"))),
      topics = Map("shop.cart-events.v1" -> TopicState.Exists(3, 1, Map.empty))
    )
    val s = LifecycleRules.status(resource, hashes(flinkObserved), Vector.empty, o, "t")
    assertEquals(s.phase, AnkkaFlowStatus.Pending)
    assertEquals(s.detail, "cleaner: rolling out")
  }

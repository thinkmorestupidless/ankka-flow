package com.thinkmorestupidless.ankka.flow.operator

import java.nio.file.{Files, Paths}

import scala.jdk.CollectionConverters.*

import com.thinkmorestupidless.ankka.flow.crd.*
import com.thinkmorestupidless.ankka.flow.descriptor.{Delivery, Settings as FlowSettings}
import io.fabric8.kubernetes.api.model.GenericKubernetesResource
import io.fabric8.kubernetes.api.model.apps.Deployment

import Fixtures.*

/** A Flink streamlet rendered as a `FlinkDeployment` (research R10, R13). */
class FlinkRenderingSuite extends munit.FunSuite:

  private val expectedFile = Paths
    .get(sys.props.getOrElse("flow.repo.root", "."))
    .resolve("operator/src/test/resources/expected/counter-flinkdeployment.yaml")

  private def render(r: AnkkaFlow = cartWithCounter(), o: Observed = flinkObserved) =
    Rendering.render(
      r,
      settings.copy(flinkServiceAccount = "flink-jobs"),
      o,
      "2026-11-22T00:00:00Z"
    )

  private def flinkDeployment(r: Rendering.Rendered): GenericKubernetesResource =
    val Vector(d) = r.actions.collect { case Action.ApplyFlinkDeployment(d) => d }: @unchecked
    d

  private def yaml(d: GenericKubernetesResource): String =
    FlowSerialization.yamlMapper().writeValueAsString(d)

  private def spec(d: GenericKubernetesResource): Map[String, AnyRef] =
    d.getAdditionalProperties.get("spec").asInstanceOf[java.util.Map[String, AnyRef]].asScala.toMap

  private def settingsOf(r: Rendering.Rendered, streamlet: String): FlowSettings =
    val secret = r.actions.collect {
      case Action.EnsureSecret(s) if s.getMetadata.getName == s"flow-cart-$streamlet" => s
    }.head
    FlowSettings.parse(secret.getStringData.get(StreamletFiles.Key)).toOption.get

  test("the counter's FlinkDeployment is the committed expectation") {
    val actual = yaml(flinkDeployment(render()))
    if sys.props.get("flow.fixtures.regenerate").contains("on") then
      Files.createDirectories(expectedFile.getParent)
      Files.writeString(expectedFile, actual): Unit
    assertEquals(actual, Files.readString(expectedFile))
  }

  test("the Flink streamlet has no Deployment; the Pekko Streams one still has") {
    val r = render()
    val names = r.actions.collect { case Action.ApplyDeployment(d: Deployment) =>
      d.getMetadata.getName
    }
    assertEquals(names, Vector("flow-cart-cleaner"))
    assertEquals(flinkDeployment(r).getMetadata.getName, "flow-cart-counter")
  }

  test("the state store's env is copied to the pipeline's flink-state Secret") {
    val r = render()
    val Vector(env) = r.actions.collect {
      case Action.EnsureSecret(s) if s.getMetadata.getName == "flow-cart-flink-state" => s
    }: @unchecked
    assertEquals(env.getStringData.asScala.toMap, stateStore.env)
    assertEquals(env.getMetadata.getOwnerReferences.asScala.map(_.getUid).toList, List("uid-1"))
  }

  test("replicas 0 suspends the job") {
    val job = spec(flinkDeployment(render(cartWithCounter(counterReplicas = 0))))("job")
      .asInstanceOf[java.util.Map[String, AnyRef]]
    assertEquals(job.get("state"), "suspended")
    assertEquals(job.get("upgradeMode"), "last-state")
  }

  test("a changed image changes only spec.image") {
    val before = yaml(flinkDeployment(render()))
    val after  = yaml(flinkDeployment(render(cartWithCounter(counterImage = "c:2"))))
    val diff   = before.linesIterator.zip(after.linesIterator).filter(_ != _).toVector
    assertEquals(
      diff,
      Vector(("  image: ghcr.io/example/cart-counter:0.6.0", "  image: c:2"))
    )
  }

  test("an outlet on a managed topic is exactly once, under a prefix of the epoch") {
    val out = settingsOf(render(), "counter").outlet("out").get
    assertEquals(out.delivery, Delivery.ExactlyOnce)
    assertEquals(out.transactionalIdPrefix, Some("flow.shop.cart.counter.out.e1"))
    assertEquals(out.kafka.get("transaction.timeout.ms"), Some("900000"))
    val later =
      settingsOf(render(o = flinkObserved.copy(stateEpochs = Map("counter" -> 3))), "counter")
    assertEquals(
      later.outlet("out").get.transactionalIdPrefix,
      Some("flow.shop.cart.counter.out.e3")
    )
  }

  test("an outlet on an unmanaged topic is at least once with no prefix") {
    val out =
      settingsOf(render(cartWithCounter(countsManaged = false)), "counter").outlet("out").get
    assertEquals(out.delivery, Delivery.AtLeastOnce)
    assertEquals(out.transactionalIdPrefix, None)
    assertEquals(out.kafka.get("transaction.timeout.ms"), None)
  }

  test("a Pekko Streams outlet on a managed topic stays at least once") {
    val out = settingsOf(render(), "cleaner").outlet("out").get
    assertEquals(out.delivery, Delivery.AtLeastOnce)
  }

  test("every inlet on a managed topic reads committed records only") {
    val in = settingsOf(render(), "counter").inlet("in").get
    assertEquals(in.kafka.get("isolation.level"), Some("read_committed"))
  }

  test("the epoch is in every state path") {
    val config = spec(
      flinkDeployment(render(o = flinkObserved.copy(stateEpochs = Map("counter" -> 2))))
    )("flinkConfiguration").asInstanceOf[java.util.Map[String, String]].asScala
    Seq(
      "execution.checkpointing.dir",
      "execution.checkpointing.savepoint-dir",
      "high-availability.storageDir"
    )
      .foreach(k =>
        assert(config(k).startsWith("s3://flow-state/dev/shop/cart/counter/2/"), config(k))
      )
  }

  test("a topic whose cluster names a token endpoint gives the task managers the allowed urls") {
    val endpoint = "https://auth.example/realms/ankka/protocol/openid-connect/token"
    val o = flinkObserved.copy(clusters =
      flinkObserved.clusters.updated(
        "default",
        defaultCluster.copy(connectionConfig =
          defaultCluster.connectionConfig + ("sasl.oauthbearer.token.endpoint.url" -> endpoint)
        )
      )
    )
    val config = spec(flinkDeployment(render(o = o)))("flinkConfiguration")
      .asInstanceOf[java.util.Map[String, String]]
    assertEquals(
      config.get("env.java.opts.taskmanager"),
      s"-Dorg.apache.kafka.sasl.oauthbearer.allowed.urls=$endpoint"
    )
    assertEquals(
      spec(flinkDeployment(render()))("flinkConfiguration")
        .asInstanceOf[java.util.Map[String, String]]
        .containsKey("env.java.opts.taskmanager"),
      false
    )
  }

  test("a Flink streamlet no longer in the spec has its FlinkDeployment and Secret deleted") {
    val o = flinkObserved.copy(flinkDeployments = Map("old" -> FlinkState("i", "h")))
    val r = render(o = o)
    assert(r.actions.contains(Action.DeleteFlinkDeployment("shop", "flow-cart-old")), r.actions)
    assert(r.actions.contains(Action.DeleteSecret("shop", "flow-cart-old")), r.actions)
  }

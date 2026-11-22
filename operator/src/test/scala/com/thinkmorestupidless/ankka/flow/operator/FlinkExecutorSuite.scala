package com.thinkmorestupidless.ankka.flow.operator

import scala.jdk.CollectionConverters.*

import com.thinkmorestupidless.ankka.flow.crd.*
import io.fabric8.kubernetes.api.model.{GenericKubernetesResource, ObjectMetaBuilder, SecretBuilder}
import io.fabric8.kubernetes.api.model.apiextensions.v1.{
  CustomResourceDefinitionBuilder,
  CustomResourceDefinitionNamesBuilder,
  CustomResourceDefinitionSpecBuilder
}
import io.fabric8.kubernetes.client.{KubernetesClient, KubernetesClientBuilder}
import io.fabric8.kubernetes.client.server.mock.{KubernetesCrudDispatcher, KubernetesMockServer}
import io.fabric8.mockwebserver.{Context, MockWebServer}

import Fixtures.*

/** The executor's Flink side against a mock API server. */
class FlinkExecutorSuite extends munit.FunSuite:

  private val server = new KubernetesMockServer(
    new Context(),
    new MockWebServer(),
    new java.util.HashMap(),
    new KubernetesCrudDispatcher(),
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

  private def executor = new Fabric8Executor(client, settings)

  private def flinkDeployments(ns: String) =
    client.genericKubernetesResources(Fabric8Executor.FlinkDeploymentContext).inNamespace(ns)

  private def created(r: AnkkaFlow): AnkkaFlow =
    client.resources(classOf[AnkkaFlow]).inNamespace("shop").resource(r).create()

  private def installCrd(): Unit =
    client.apiextensions.v1.customResourceDefinitions
      .resource(
        new CustomResourceDefinitionBuilder()
          .withMetadata(new ObjectMetaBuilder().withName(FlinkRendering.Crd).build())
          .withSpec(
            new CustomResourceDefinitionSpecBuilder()
              .withGroup("flink.apache.org")
              .withNames(
                new CustomResourceDefinitionNamesBuilder()
                  .withKind("FlinkDeployment")
                  .withPlural("flinkdeployments")
                  .build()
              )
              .withScope("Namespaced")
              .build()
          )
          .build()
      )
      .create(): Unit

  private def secret(name: String, data: Map[String, String]): Unit =
    client.secrets
      .inNamespace("ankka-flow")
      .resource(
        new SecretBuilder()
          .withMetadata(new ObjectMetaBuilder().withName(name).build())
          .withStringData(data.asJava)
          .build()
      )
      .create(): Unit

  private def installClusters(): Unit =
    secret(
      "kafka-cluster-default",
      Map(
        "bootstrap.servers" -> "kafka.kafka.svc:9092",
        "partitions"        -> "3",
        "replicas"          -> "1"
      )
    )
    secret("kafka-cluster-shop", Map("bootstrap.servers" -> "shop-kafka:9092"))

  private def installStateStore(): Unit =
    client.secrets
      .inNamespace("ankka-flow")
      .resource(
        new SecretBuilder()
          .withMetadata(new ObjectMetaBuilder().withName(StateStore.SecretName).build())
          .withStringData(
            Map(
              "dir"          -> "s3://flow-state/dev",
              "flink-config" -> "s3.endpoint=http://minio.minio:9000",
              "env"          -> "AWS_ACCESS_KEY_ID=minio"
            ).asJava
          )
          .build()
      )
      .create(): Unit

  test("without the Flink operator's CRD or flink-state, neither is observed") {
    val o = executor.observe(created(cartWithCounter()))
    assertEquals((o.flinkCrd, o.stateStore, o.flinkDeployments), (false, None, Map.empty))
    client.resources(classOf[AnkkaFlow]).inNamespace("shop").withName("cart").delete(): Unit
  }

  test("the CRD, the state store, and an applied FlinkDeployment with its status are observed") {
    installCrd()
    installStateStore()
    installClusters()
    val resource = created(cartWithCounter())
    val o        = executor.observe(resource)
    assert(o.flinkCrd)
    assertEquals(
      o.stateStore,
      Some(
        StateStore(
          "s3://flow-state/dev",
          Map("s3.endpoint"       -> "http://minio.minio:9000"),
          Map("AWS_ACCESS_KEY_ID" -> "minio")
        )
      )
    )
    val rendered = Rendering.render(resource, settings, o, "t")
    val Vector(apply) = rendered.actions.collect { case a: Action.ApplyFlinkDeployment =>
      a
    }: @unchecked
    // The mock's CRUD dispatcher has no server-side apply (415), so the rendered objects are
    // created as they are; the executor's applies run against an API server in FlowClusterSuite.
    flinkDeployments("shop").resource(apply.deployment).create(): Unit
    val env = rendered.actions.collect {
      case a @ Action.EnsureSecret(s) if s.getMetadata.getName == "flow-cart-flink-state" => a
    }
    env.foreach(a => client.resource(a.secret).create())
    assertEquals(
      client.secrets
        .inNamespace("shop")
        .withName("flow-cart-flink-state")
        .get()
        .getStringData
        .asScala
        .toMap,
      Map("AWS_ACCESS_KEY_ID" -> "minio")
    )

    // the Flink operator reports on it
    val live = flinkDeployments("shop").withName("flow-cart-counter").get()
    live.getAdditionalProperties.put(
      "status",
      Map[String, AnyRef](
        "lifecycleState"             -> "STABLE",
        "jobManagerDeploymentStatus" -> "READY",
        "jobStatus"                  -> Map[String, AnyRef]("state" -> "RUNNING").asJava,
        "reconciliationStatus" -> Map[String, AnyRef](
          // As the Flink operator writes it: the spec under `spec`, beside `resource_metadata`.
          "lastStableSpec" -> """{"spec":{"image":"ghcr.io/example/cart-counter:0.5.0","job":{"state":"running"}},"resource_metadata":{"apiVersion":"flink.apache.org/v1beta1","firstDeployment":false}}"""
        ).asJava
      ).asJava
    )
    flinkDeployments("shop").resource(live).update(): Unit

    val seen = executor.observe(resource).flinkDeployments
    assertEquals(
      seen,
      Map(
        "counter" -> FlinkState(
          image = "ghcr.io/example/cart-counter:0.6.0",
          configHash = rendered.hashes("counter"),
          desiredState = "running",
          lifecycleState = Some("STABLE"),
          jobState = Some("RUNNING"),
          jobManagerStatus = Some("READY"),
          error = None,
          lastStableImage = Some("ghcr.io/example/cart-counter:0.5.0")
        )
      )
    )
  }

  test("the latest JobException of the job running now is observed with the FlinkDeployment") {
    val resource = client.resources(classOf[AnkkaFlow]).inNamespace("shop").withName("cart").get()
    val live     = flinkDeployments("shop").withName("flow-cart-counter").get()
    val started  = java.time.Instant.parse("2026-11-22T17:00:00Z")
    def status(jobStarted: java.time.Instant) =
      val st = live.getAdditionalProperties.get("status") match
        case m: java.util.Map[?, ?] =>
          new java.util.HashMap[String, AnyRef](m.asInstanceOf[java.util.Map[String, AnyRef]])
        case m: scala.collection.Map[?, ?] =>
          new java.util.HashMap[String, AnyRef](
            m.asInstanceOf[scala.collection.Map[String, AnyRef]].asJava
          )
        case _ => new java.util.HashMap[String, AnyRef]()
      st.put(
        "jobStatus",
        Map[String, AnyRef](
          "state"     -> "RESTARTING",
          "startTime" -> jobStarted.toEpochMilli.toString
        ).asJava
      )
      live.getAdditionalProperties.put("status", st)
      flinkDeployments("shop").resource(live).update(): Unit
    def event(name: String, at: java.time.Instant, message: String) =
      client.v1.events
        .inNamespace("shop")
        .resource(
          new io.fabric8.kubernetes.api.model.EventBuilder()
            .withMetadata(new ObjectMetaBuilder().withName(name).withNamespace("shop").build())
            .withInvolvedObject(
              new io.fabric8.kubernetes.api.model.ObjectReferenceBuilder()
                .withKind(FlinkRendering.Kind)
                .withName("flow-cart-counter")
                .withUid(live.getMetadata.getUid)
                .build()
            )
            .withReason("JobException")
            .withType("Warning")
            .withMessage(message)
            .withLastTimestamp(at.toString)
            .build()
        )
        .create(): Unit
    status(started)
    event("old", started.minusSeconds(600), "an exception of the job before")
    assertEquals(executor.observe(resource).flinkDeployments("counter").jobException, None)
    event("new", started.plusSeconds(20), "Exception while creating StreamOperatorStateContext.")
    event(
      "newer",
      started.plusSeconds(40),
      "Exception while creating StreamOperatorStateContext. again"
    )
    assertEquals(
      executor.observe(resource).flinkDeployments("counter").jobException,
      Some("Exception while creating StreamOperatorStateContext. again")
    )
  }

  test("a FlinkDeployment of another resource with the same labels is not observed") {
    val other = new GenericKubernetesResource()
    other.setApiVersion(FlinkRendering.ApiVersion)
    other.setKind(FlinkRendering.Kind)
    other.setMetadata(
      new ObjectMetaBuilder()
        .withName("stray")
        .withNamespace("shop")
        .withLabels(Labels.streamlet("cart", "stray").asJava)
        .build()
    )
    flinkDeployments("shop").resource(other).create(): Unit
    val resource = client.resources(classOf[AnkkaFlow]).inNamespace("shop").withName("cart").get()
    assertEquals(executor.observe(resource).flinkDeployments.keySet, Set("counter"))
  }

  test("a FlinkDeployment is deleted") {
    val resource = client.resources(classOf[AnkkaFlow]).inNamespace("shop").withName("cart").get()
    executor.execute(resource, Action.DeleteFlinkDeployment("shop", "flow-cart-counter"))
    assertEquals(flinkDeployments("shop").withName("flow-cart-counter").get(), null)
  }

  test("each AdvanceStateEpoch moves one streamlet's epoch on by one in the annotation") {
    def resource = client.resources(classOf[AnkkaFlow]).inNamespace("shop").withName("cart").get()
    executor.execute(resource, Action.AdvanceStateEpoch("counter"))
    assertEquals(Fabric8Executor.stateEpochs(resource), Map("counter" -> 2))
    executor.execute(resource, Action.AdvanceStateEpoch("counter"))
    executor.execute(resource, Action.AdvanceStateEpoch("other"))
    assertEquals(Fabric8Executor.stateEpochs(resource), Map("counter" -> 3, "other" -> 2))
    assertEquals(
      resource.getMetadata.getAnnotations.get(Labels.StateEpochs),
      """{"counter":3,"other":2}"""
    )
    assertEquals(executor.observe(resource).stateEpochs, Map("counter" -> 3, "other" -> 2))
  }

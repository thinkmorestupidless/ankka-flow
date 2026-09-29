package com.thinkmorestupidless.ankka.flow.operator

import scala.jdk.CollectionConverters.*

import com.thinkmorestupidless.ankka.flow.crd.*
import io.fabric8.kubernetes.api.model.apps.Deployment

import Fixtures.*

/** Rendering, pure: what the operator does for a resource and an observation (US3). */
class RenderingSuite extends munit.FunSuite:

  private def render(r: AnkkaFlow = cart(), o: Observed = observed, s: Settings = settings) =
    Rendering.render(r, s, o, "2026-09-28T10:00:00Z")

  private def deployments(r: Rendering.Rendered) = r.actions.collect {
    case Action.ApplyDeployment(d) => d
  }
  private def events(r: Rendering.Rendered) = r.actions.collect { case e: Action.RecordEvent => e }
  private def status(r: Rendering.Rendered) = r.actions.collect { case Action.SetStatus(s) =>
    s
  }.last
  private def router(r: Rendering.Rendered): Deployment =
    deployments(r).find(_.getMetadata.getName == "flow-cart-router").get

  test(
    "two containers: the sidecar with env, config mount, token, metrics port and exec probes (S3.2)"
  ) {
    val pod     = router(render()).getSpec.getTemplate.getSpec
    val sidecar = pod.getContainers.asScala.find(_.getName == "sidecar").get
    assertEquals(sidecar.getImage, "ankka-flow-sidecar:test")
    val env = sidecar.getEnv.asScala.map(e => e.getName -> Option(e.getValue)).toMap
    assertEquals(env("FLOW_PROCESS_ADDRESS"), Some("127.0.0.1:9010"))
    assertEquals(env("FLOW_CONFIG_DIR"), Some("/etc/flow/config"))
    assert(env.contains("FLOW_POD_NAME") && env.contains("FLOW_POD_NAMESPACE"))
    assertEquals(
      sidecar.getPorts.asScala.map(p => p.getName -> p.getContainerPort.intValue).toList,
      List("metrics" -> 2050)
    )
    assertEquals(sidecar.getVolumeMounts.asScala.map(_.getName).toSet, Set("config", "api-token"))
    assert(sidecar.getVolumeMounts.asScala.forall(_.getReadOnly))
    assertEquals(
      sidecar.getReadinessProbe.getExec.getCommand.asScala.toList,
      List("test", "-f", "/tmp/flow/ready")
    )
    assert(sidecar.getLivenessProbe.getExec.getCommand.asScala.last.contains("/tmp/flow/alive"))
  }

  test(
    "the process container has its port variable and nothing else: no ports, probes, mounts or secrets (S3.2, FR-021)"
  ) {
    val pod     = router(render()).getSpec.getTemplate.getSpec
    val process = pod.getContainers.asScala.find(_.getName == "process").get
    assertEquals(process.getImage, "ghcr.io/example/cart-router:0.3.1")
    assertEquals(
      process.getEnv.asScala.map(e => e.getName -> e.getValue).toList,
      List("FLOW_PROCESS_PORT" -> "9010")
    )
    assert(
      process.getPorts.isEmpty && process.getReadinessProbe == null && process.getLivenessProbe == null
    )
    assert(process.getVolumeMounts.isEmpty && process.getEnvFrom.isEmpty)
    assertEquals(
      pod.getAutomountServiceAccountToken,
      java.lang.Boolean.FALSE,
      "the token would be mounted into every container"
    )
  }

  test(
    "a rolling update that never drops below the desired count, owned by the resource, labelled for its pipeline"
  ) {
    val d = router(render(cart(routerReplicas = 3)))
    assertEquals(d.getSpec.getReplicas.intValue, 3)
    assertEquals(d.getSpec.getStrategy.getRollingUpdate.getMaxUnavailable.getIntVal.intValue, 0)
    assertEquals(d.getMetadata.getOwnerReferences.asScala.map(_.getUid).toList, List("uid-1"))
    assertEquals(d.getMetadata.getLabels.get(Labels.PipelineKey), "cart")
    assertEquals(
      d.getSpec.getSelector.getMatchLabels.asScala.toMap,
      Map(Labels.PipelineKey -> "cart", Labels.StreamletKey -> "router")
    )
    assertEquals(d.getSpec.getTemplate.getMetadata.getAnnotations.get("prometheus.io/port"), "2050")
  }

  test("nothing about the sidecar image comes from the resource (FR-020)") {
    val other = render(s = settings.copy(sidecarImage = Some("registry/sidecar:9")))
    assert(
      deployments(other).forall(
        _.getSpec.getTemplate.getSpec.getContainers.asScala
          .find(_.getName == "sidecar")
          .get
          .getImage == "registry/sidecar:9"
      )
    )
  }

  test("no sidecar image: refused with that reason, one event, nothing created (S3.6, FR-024)") {
    val r = render(s = settings.copy(sidecarImage = None))
    assertEquals(r.actions.size, 2)
    assertEquals(events(r).map(_.reason), Vector("SidecarImageMissing"))
    assertEquals(status(r).phase, AnkkaFlowStatus.Failed)
  }

  test(
    "a changed parameter changes only that streamlet's config hash; nothing else changes (FR-024a)"
  ) {
    def hash(r: Rendering.Rendered, name: String) =
      deployments(r)
        .find(_.getMetadata.getName == name)
        .get
        .getSpec
        .getTemplate
        .getMetadata
        .getAnnotations
        .get(Labels.ConfigHash)
    val before = render()
    val after  = render(cart(threshold = 250))
    assertNotEquals(hash(before, "flow-cart-router"), hash(after, "flow-cart-router"))
    assertEquals(hash(before, "flow-cart-sink"), hash(after, "flow-cart-sink"))
    assertEquals(
      hash(render(), "flow-cart-router"),
      hash(before, "flow-cart-router"),
      "rendering is deterministic"
    )
    assertEquals(
      hash(render(cart(routerReplicas = 5)), "flow-cart-router"),
      hash(before, "flow-cart-router"),
      "replicas do not roll"
    )
  }

  test("a streamlet whose rendering changed is recorded as rolled; an unchanged one is not") {
    val current = Rendering.render(cart(), settings, observed, "t").hashes
    val seen = observed.copy(
      deployments = Map(
        "router" -> DeploymentState(
          current("router"),
          "ghcr.io/example/cart-router:0.3.1",
          1,
          1,
          1,
          1,
          1
        ),
        "sink" -> DeploymentState(current("sink"), "ghcr.io/example/cart-sink:0.3.1", 1, 1, 1, 1, 1)
      ),
      labelledStreamlets = Set("router", "sink")
    )
    assertEquals(
      events(render(cart(threshold = 250), seen))
        .filter(_.reason == "StreamletRolled")
        .map(_.note.contains("'router'")),
      Vector(true)
    )
    assertEquals(events(render(cart(), seen)).filter(_.reason == "StreamletRolled"), Vector.empty)
  }

  test("a streamlet removed from the spec loses its Deployment and Secret") {
    val seen = observed.copy(labelledStreamlets = Set("router", "sink", "auditor"))
    val r    = render(cart(), seen)
    assert(r.actions.contains(Action.DeleteDeployment("shop", "flow-cart-auditor")))
    assert(r.actions.contains(Action.DeleteSecret("shop", "flow-cart-auditor")))
    assert(events(r).exists(e => e.reason == "StreamletRemoved" && e.note.contains("auditor")))
  }

  test(
    "managed topics are ensured before any Deployment; an unmanaged one never is (FR-019, FR-005)"
  ) {
    val r      = render()
    val ensure = r.actions.indexWhere(_.isInstanceOf[Action.EnsureTopic])
    val deploy = r.actions.indexWhere(_.isInstanceOf[Action.ApplyDeployment])
    assert(ensure >= 0 && ensure < deploy)
    assertEquals(
      r.actions.collect { case Action.EnsureTopic(t) => t.name }.toSet,
      Set("cart.valid-carts", "cart.review-carts")
    )
  }

  test(
    "an existing topic with other partitions is kept and reported; changed settings are not applied"
  ) {
    val seen = observed.copy(topics =
      Map(
        "cart.valid-carts"    -> TopicState.Exists(5, 1, Map("retention.ms" -> "1000")),
        "cart.review-carts"   -> TopicState.Exists(3, 1, Map.empty),
        "shop.cart-events.v1" -> TopicState.Exists(3, 1, Map.empty)
      )
    )
    val r = render(cart(), seen)
    assertEquals(r.actions.collect { case Action.EnsureTopic(t) => t.name }, Vector.empty)
    assertEquals(events(r).map(_.reason).toSet, Set("TopicDiffers", "TopicSettingsIgnored"))
  }

  test("a missing unmanaged topic is a warning and leaves the pipeline Degraded") {
    val r = render(cart(), observed.copy(topics = Map("shop.cart-events.v1" -> TopicState.Missing)))
    assert(events(r).exists(_.reason == "TopicMissing"))
    assertEquals(status(r).phase, AnkkaFlowStatus.Degraded)
  }

  test("topic settings resolve from the resource, then the cluster (S3.4, FR-021)") {
    val r      = render()
    val review = r.resolved.find(_.id == "review-carts").get
    assertEquals(
      (review.partitions, review.replicas, review.bootstrapServers),
      (Some(3), Some(1), "kafka.kafka.svc:9092")
    )
    val valid = r.resolved.find(_.id == "valid-carts").get
    assertEquals(valid.partitions, Some(6))
    val events = r.resolved.find(_.id == "cart-events").get
    assertEquals(events.bootstrapServers, "shop-kafka:9092")
    assertEquals(events.connectionConfig, Map("sasl.mechanism" -> "PLAIN"))
  }

  test("a topic naming a cluster with no Secret is refused") {
    val r = render(cart(), observed.copy(clusters = Map("default" -> defaultCluster)))
    assertEquals(status(r).phase, AnkkaFlowStatus.Failed)
    assert(status(r).detail.contains("kafka-cluster-shop"), status(r).detail)
  }

  test("a binding that is not the descriptor's port is refused") {
    val bad = cart()
    val r0  = bad.getSpec.streamlets.head
    bad.setSpec(
      bad.getSpec.copy(streamlets =
        List(r0.copy(outlets = r0.outlets - "review" + ("audit" -> "review-carts")))
      )
    )
    val detail = status(render(bad)).detail
    assert(detail.contains("binds outlet 'audit', which the descriptor does not declare"), detail)
    val unbound = cart()
    val r1      = unbound.getSpec.streamlets.head
    unbound.setSpec(unbound.getSpec.copy(streamlets = List(r1.copy(inlets = Map.empty))))
    assert(
      status(render(unbound)).detail.contains("inlet 'in' is not bound"),
      status(render(unbound)).detail
    )
  }

  test("the Secret holds the descriptor and streamlet.conf, owned by the resource") {
    val secret = render().actions.collect {
      case Action.EnsureSecret(s) if s.getMetadata.getName == "flow-cart-router" => s
    }.head
    assertEquals(
      secret.getStringData.keySet.asScala.toSet,
      Set("descriptor.json", "streamlet.conf")
    )
    assertEquals(secret.getMetadata.getOwnerReferences.asScala.head.getName, "cart")
  }

  test("the sidecars' service account may create events, and nothing else") {
    val role = render().actions.collect { case Action.EnsureRole(r) => r }.head
    assertEquals(
      role.getRules.asScala
        .map(r =>
          (r.getApiGroups.asScala.toList, r.getResources.asScala.toList, r.getVerbs.asScala.toList)
        )
        .toList,
      List((List("events.k8s.io"), List("events"), List("create")))
    )
  }

  test("onDelete Delete adds a finalizer; deletion deletes managed topics only, then releases it") {
    val keep = render()
    assert(!keep.actions.contains(Action.EnsureFinalizer))
    val del = cart()
    del.setSpec(del.getSpec.copy(onDelete = OnDelete(OnDelete.Delete)))
    assert(render(del).actions.contains(Action.EnsureFinalizer))
    del.getMetadata.setDeletionTimestamp("2026-09-28T10:00:00Z")
    val gone = render(del).actions
    assertEquals(
      gone.collect { case Action.DeleteTopic(t) => t.name }.toSet,
      Set("cart.valid-carts", "cart.review-carts")
    )
    assertEquals(gone.last, Action.RemoveFinalizer)
  }

  test("status: Pending while rolling, Ready when every streamlet is ready and settled (S3.3)") {
    val external = Map("shop.cart-events.v1" -> TopicState.Exists(3, 1, Map.empty))
    assertEquals(
      status(render(cart(), observed.copy(topics = external))).phase,
      AnkkaFlowStatus.Pending
    )
    val hashes = render().hashes
    val settled = observed.copy(
      deployments = Map(
        "router" -> DeploymentState(
          hashes("router"),
          "ghcr.io/example/cart-router:0.3.1",
          1,
          1,
          1,
          2,
          2
        ),
        "sink" -> DeploymentState(hashes("sink"), "ghcr.io/example/cart-sink:0.3.1", 1, 1, 1, 2, 2)
      ),
      topics = Map("shop.cart-events.v1" -> TopicState.Exists(3, 1, Map.empty))
    )
    assertEquals(status(render(cart(), settled)).phase, AnkkaFlowStatus.Ready)
    val short = settled.copy(deployments =
      settled.deployments.updated("sink", settled.deployments("sink").copy(readyReplicas = 0))
    )
    assertEquals(status(render(cart(), short)).phase, AnkkaFlowStatus.Degraded)
  }

  // ── Built-in streamlets (feature 002) ─────────────────────────────────────────────────────────

  private def sinkOf(r: Rendering.Rendered): Deployment =
    deployments(r).find(_.getMetadata.getName == "flow-checkouts-graph").get

  private def refusals(r: Rendering.Rendered): Vector[String] =
    events(r).filter(_.reason == "Refused").map(_.note)

  test(
    "a built-in streamlet's pod has only the sidecar, with the Secret mounted and no process address"
  ) {
    val r   = render(graph(), graphObserved)
    val pod = sinkOf(r).getSpec.getTemplate.getSpec
    assertEquals(pod.getContainers.asScala.map(_.getName).toList, List("sidecar"))
    val sidecar = pod.getContainers.get(0)
    val env     = sidecar.getEnv.asScala.map(_.getName).toSet
    assert(!env.contains("FLOW_PROCESS_ADDRESS"), env.toString)
    assert(env.contains("FLOW_CONFIG_DIR") && env.contains("FLOW_POD_NAME"), env.toString)
    val mounts = sidecar.getVolumeMounts.asScala.map(m => m.getName -> m.getMountPath).toMap
    assertEquals(mounts.keySet, Set("config", "api-token", "neo4j"))
    assertEquals(mounts("neo4j"), "/etc/flow/neo4j")
    assert(sidecar.getVolumeMounts.asScala.forall(_.getReadOnly))
    val volume = pod.getVolumes.asScala.find(_.getName == "neo4j").get
    assertEquals(volume.getSecret.getSecretName, "neo4j-shop")
    assertEquals(volume.getSecret.getDefaultMode.intValue, Integer.parseInt("440", 8))
    assert(sidecar.getReadinessProbe != null && sidecar.getLivenessProbe != null)
  }

  test("the mapper beside a built-in keeps its two containers and no stage Secret") {
    val r      = render(graph(), graphObserved)
    val mapper = deployments(r).find(_.getMetadata.getName == "flow-checkouts-mapper").get
    val pod    = mapper.getSpec.getTemplate.getSpec
    assertEquals(pod.getContainers.asScala.map(_.getName).toList, List("sidecar", "process"))
    assert(!pod.getVolumes.asScala.exists(_.getName == "neo4j"))
    assert(!events(r).exists(_.reason == "Refused"), events(r).toString)
  }

  test("the stage's config names the mounted directory and never the password") {
    val r = render(graph(), graphObserved)
    val conf = r.actions.collect {
      case Action.EnsureSecret(s) if s.getMetadata.getName == "flow-checkouts-graph" =>
        s.getStringData.get("streamlet.conf")
    }.head
    assert(conf.contains("stage {"), conf)
    assert(conf.contains("name = \"neo4j-merge-sink\""), conf)
    assert(conf.contains("credentials-dir = \"/etc/flow/neo4j\""), conf)
    assert(!conf.contains("password"), conf)
  }

  test("a missing Secret is refused with nothing applied") {
    val r = render(graph(), observed)
    assertEquals(r.actions.size, 2)
    assertEquals(
      refusals(r),
      Vector(
        "streamlet 'graph' names Secret 'neo4j-shop', which does not exist in namespace 'shop'"
      )
    )
    assertEquals(status(r).phase, AnkkaFlowStatus.Failed)
  }

  test("a Secret without a required key is refused, one message per missing key") {
    val r = render(
      graph(),
      observed.copy(secrets = Map("neo4j-shop" -> SecretState("1", Set("uri"))))
    )
    assertEquals(r.actions.size, 3)
    assertEquals(
      refusals(r),
      Vector(
        "streamlet 'graph': Secret 'neo4j-shop' has no key 'username'",
        "streamlet 'graph': Secret 'neo4j-shop' has no key 'password'"
      )
    )
  }

  test("an image on a built-in streamlet, no secret parameter, or an unknown built-in is refused") {
    assertEquals(
      refusals(render(graph(sinkImage = "registry/x:1"), graphObserved)),
      Vector("streamlet 'graph' is built in and takes no image")
    )
    assertEquals(
      refusals(render(graph(sinkConfig = Map.empty), graphObserved)),
      Vector("streamlet 'graph' is built in and names no Secret in its 'secret' parameter")
    )
    val renamed = neo4jMergeSink.deepCopy[com.fasterxml.jackson.databind.node.ObjectNode]()
    renamed.put("name", "graph-writer")
    assertEquals(
      refusals(render(graph(sinkDescriptor = renamed), graphObserved)),
      Vector("streamlet 'graph' names built-in 'graph-writer', which this operator does not know")
    )
  }

  test("a new version of the stage's Secret changes only the sink's config hash") {
    val before = render(graph(), graphObserved).hashes
    val after = render(
      graph(),
      observed.copy(secrets = Map("neo4j-shop" -> neo4jSecret.copy(resourceVersion = "42")))
    ).hashes
    assertNotEquals(before("graph"), after("graph"))
    assertEquals(before("mapper"), after("mapper"))
  }

  test("a built-in streamlet settles with an empty image on both sides") {
    val hashes = render(graph(), graphObserved).hashes
    val settled = graphObserved.copy(
      deployments = Map(
        "mapper" -> DeploymentState(
          hashes("mapper"),
          "ghcr.io/example/checkout-graph:0.1.0",
          1,
          1,
          1,
          2,
          2
        ),
        "graph" -> DeploymentState(hashes("graph"), "", 1, 1, 1, 2, 2)
      ),
      topics = Map("cart-checkouts" -> TopicState.Exists(3, 1, Map.empty))
    )
    val r = render(graph(), settled)
    assertEquals(status(r).phase, AnkkaFlowStatus.Ready)
    assert(!events(r).exists(_.reason == "StreamletRolled"), events(r).toString)
  }

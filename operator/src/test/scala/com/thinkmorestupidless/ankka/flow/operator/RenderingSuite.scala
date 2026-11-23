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
  private def cleaner(r: Rendering.Rendered): Deployment =
    deployments(r).find(_.getMetadata.getName == "flow-cart-cleaner").get

  test("one container, the streamlet's image, its settings mounted 0440, file probes, no token") {
    val pod = cleaner(render()).getSpec.getTemplate.getSpec
    assertEquals(pod.getContainers.asScala.map(_.getName).toList, List("streamlet"))
    val c = pod.getContainers.get(0)
    assertEquals(c.getImage, "ghcr.io/example/cart-cleaner:0.6.0")
    assertEquals(
      c.getEnv.asScala.map(e => e.getName -> e.getValue).toList,
      List("FLOW_CONFIG_DIR" -> "/etc/flow/config")
    )
    assertEquals(
      c.getVolumeMounts.asScala
        .map(m => (m.getName, m.getMountPath, m.getReadOnly.booleanValue))
        .toList,
      List(("config", "/etc/flow/config", true))
    )
    val volume = pod.getVolumes.asScala.find(_.getName == "config").get.getSecret
    assertEquals(volume.getSecretName, "flow-cart-cleaner")
    assertEquals(volume.getDefaultMode.intValue, Rendering.SecretFileMode)
    assertEquals(pod.getVolumes.size, 1, "no token volume")
    assertEquals(
      c.getReadinessProbe.getExec.getCommand.asScala.toList,
      List("test", "-f", "/tmp/flow/ready")
    )
    assert(c.getLivenessProbe.getExec.getCommand.asScala.last.contains("/tmp/flow/alive"))
    assert(c.getPorts.isEmpty && c.getEnvFrom.isEmpty)
    assertEquals(
      pod.getAutomountServiceAccountToken,
      java.lang.Boolean.FALSE,
      "a Pekko Streams pod gets no service account token"
    )
  }

  private def refusedFor(o: Observed, kind: String, message: String) =
    val rendered = render(cartWithCounter(), o)
    val st       = status(rendered)
    assertEquals(st.phase, AnkkaFlowStatus.Failed)
    val c = st.conditions.find(_.`type` == kind).getOrElse(fail(s"no $kind in ${st.conditions}"))
    assertEquals((c.status, c.message), (Condition.True, message))
    assertEquals(
      st.conditions.find(_.`type` == Condition.Ready).map(_.status),
      Some(Condition.False)
    )
    assert(events(rendered).exists(e => e.reason == kind && e.note == message), events(rendered))
    // none of its streamlets, of either engine, is started; nothing else is touched
    assertEquals(
      rendered.actions.filterNot(a =>
        a.isInstanceOf[Action.RecordEvent] || a.isInstanceOf[Action.SetStatus]
      ),
      Vector.empty
    )

  test("a pipeline with a Flink streamlet is refused where the cluster lacks the Flink operator") {
    refusedFor(
      flinkObserved.copy(flinkCrd = false),
      Condition.FlinkOperatorMissing,
      Conditions.FlinkOperatorMissingMessage
    )
  }

  test("a pipeline with a Flink streamlet is refused where the cluster lacks a state store") {
    refusedFor(
      flinkObserved.copy(stateStore = None),
      Condition.StateStoreMissing,
      "create the Secret flink-state in namespace ankka-flow with a dir key"
    )
  }

  test("lacking both, both conditions are listed") {
    val st = status(render(cartWithCounter(), observed))
    assertEquals(
      st.conditions.map(_.`type`),
      List(Condition.FlinkOperatorMissing, Condition.StateStoreMissing, Condition.Ready)
    )
  }

  test(
    "with the Flink operator and a state store, neither condition; without Flink, neither needed"
  ) {
    val withBoth = status(render(cartWithCounter(), flinkObserved))
    assert(
      !withBoth.conditions.exists(c =>
        c.`type` == Condition.FlinkOperatorMissing || c.`type` == Condition.StateStoreMissing
      ),
      withBoth.conditions
    )
    assertNotEquals(withBoth.phase, AnkkaFlowStatus.Failed)
    assertNotEquals(status(render(cart(), observed)).phase, AnkkaFlowStatus.Failed)
  }

  test("a condition that still holds keeps the time it was first reported") {
    val o     = flinkObserved.copy(flinkCrd = false)
    val first = status(Rendering.render(cartWithCounter(), settings, o, "2026-11-22T10:00:00Z"))
    val r     = cartWithCounter()
    r.setStatus(first)
    val second = status(Rendering.render(r, settings, o, "2026-11-22T10:05:00Z"))
    assertEquals(
      second.conditions.map(_.lastTransitionTime).distinct,
      List("2026-11-22T10:00:00Z")
    )
  }

  test(
    "a rolling update that never drops below the desired count, owned by the resource, labelled for its pipeline"
  ) {
    val d = cleaner(render(cart(cleanerReplicas = 3)))
    assertEquals(d.getSpec.getReplicas.intValue, 3)
    assertEquals(d.getSpec.getStrategy.getRollingUpdate.getMaxUnavailable.getIntVal.intValue, 0)
    assertEquals(d.getMetadata.getOwnerReferences.asScala.map(_.getUid).toList, List("uid-1"))
    assertEquals(d.getMetadata.getLabels.get(Labels.PipelineKey), "cart")
    assertEquals(
      d.getSpec.getSelector.getMatchLabels.asScala.toMap,
      Map(Labels.PipelineKey -> "cart", Labels.StreamletKey -> "cleaner")
    )
    assert(!d.getSpec.getTemplate.getMetadata.getAnnotations.containsKey("prometheus.io/port"))
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
    val after  = render(cart(maxTotal = 250))
    assertNotEquals(hash(before, "flow-cart-cleaner"), hash(after, "flow-cart-cleaner"))
    assertEquals(hash(before, "flow-cart-sink"), hash(after, "flow-cart-sink"))
    assertEquals(
      hash(render(), "flow-cart-cleaner"),
      hash(before, "flow-cart-cleaner"),
      "rendering is deterministic"
    )
    assertEquals(
      hash(render(cart(cleanerReplicas = 5)), "flow-cart-cleaner"),
      hash(before, "flow-cart-cleaner"),
      "replicas do not roll"
    )
  }

  test("a streamlet whose rendering changed is recorded as rolled; an unchanged one is not") {
    val current = Rendering.render(cart(), settings, observed, "t").hashes
    val seen = observed.copy(
      deployments = Map(
        "cleaner" -> DeploymentState(
          current("cleaner"),
          "ghcr.io/example/cart-cleaner:0.6.0",
          1,
          1,
          1,
          1,
          1
        ),
        "sink" -> DeploymentState(current("sink"), "ghcr.io/example/cart-sink:0.6.0", 1, 1, 1, 1, 1)
      ),
      labelledStreamlets = Set("cleaner", "sink")
    )
    assertEquals(
      events(render(cart(maxTotal = 250), seen))
        .filter(_.reason == "StreamletRolled")
        .map(_.note.contains("'cleaner'")),
      Vector(true)
    )
    assertEquals(events(render(cart(), seen)).filter(_.reason == "StreamletRolled"), Vector.empty)
  }

  test("a streamlet removed from the spec loses its Deployment and Secret") {
    val seen = observed.copy(labelledStreamlets = Set("cleaner", "sink", "auditor"))
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
      Set("cart.clean-carts", "cart.cart-counts")
    )
  }

  test(
    "an existing topic with other partitions is kept and reported; changed settings are not applied"
  ) {
    val seen = observed.copy(topics =
      Map(
        "cart.clean-carts"    -> TopicState.Exists(5, 1, Map("retention.ms" -> "1000")),
        "cart.cart-counts"    -> TopicState.Exists(3, 1, Map.empty),
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
    val review = r.resolved.find(_.id == "cart-counts").get
    assertEquals(
      (review.partitions, review.replicas, review.bootstrapServers),
      (Some(3), Some(1), "kafka.kafka.svc:9092")
    )
    val valid = r.resolved.find(_.id == "clean-carts").get
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
        List(r0.copy(outlets = r0.outlets - "out" + ("audit" -> "clean-carts")))
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

  test("the Secret holds only streamlet.properties, owned by the resource") {
    val secret = render().actions.collect {
      case Action.EnsureSecret(s) if s.getMetadata.getName == "flow-cart-cleaner" => s
    }.head
    assertEquals(
      secret.getStringData.keySet.asScala.toSet,
      Set("streamlet.properties")
    )
    assertEquals(secret.getMetadata.getOwnerReferences.asScala.head.getName, "cart")
  }

  test("the pipeline's service account may create events, and nothing else") {
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
      Set("cart.clean-carts", "cart.cart-counts")
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
        "cleaner" -> DeploymentState(
          hashes("cleaner"),
          "ghcr.io/example/cart-cleaner:0.6.0",
          1,
          1,
          1,
          2,
          2
        ),
        "sink" -> DeploymentState(hashes("sink"), "ghcr.io/example/cart-sink:0.6.0", 1, 1, 1, 2, 2)
      ),
      topics = Map("shop.cart-events.v1" -> TopicState.Exists(3, 1, Map.empty))
    )
    assertEquals(status(render(cart(), settled)).phase, AnkkaFlowStatus.Ready)
    val short = settled.copy(deployments =
      settled.deployments.updated("sink", settled.deployments("sink").copy(readyReplicas = 0))
    )
    assertEquals(status(render(cart(), short)).phase, AnkkaFlowStatus.Degraded)
  }

  // ── engines ───────────────────────────────────────────────────────────────────────────────

  private def withDescriptor(edit: String => String): AnkkaFlow =
    val r       = cart()
    val cleaner = r.getSpec.streamlets.head
    val edited  = FlowSerialization.mapper().readTree(edit(cleaner.descriptor.toString))
    r.setSpec(
      r.getSpec.copy(streamlets = cleaner.copy(descriptor = edited) :: r.getSpec.streamlets.tail)
    )
    r

  private def refusedWith(r: Rendering.Rendered, fragment: String): Unit =
    assertEquals(status(r).phase, AnkkaFlowStatus.Failed)
    assert(
      events(r).exists(e => e.reason == "Refused" && e.note.contains(fragment)),
      events(r).toString
    )
    assertEquals(deployments(r), Vector.empty)

  test("a descriptor that names no engine is refused, saying it must name one") {
    refusedWith(
      render(withDescriptor(_.replace("\"engine\":\"pekko-streams\",", ""))),
      "streamlet 'cleaner': a descriptor must name an engine (pekko-streams, flink)"
    )
  }

  test("a descriptor naming an engine flow does not know is refused, naming it and the known ones") {
    refusedWith(
      render(withDescriptor(_.replace("\"engine\":\"pekko-streams\"", "\"engine\":\"spark\""))),
      "streamlet 'cleaner': engine 'spark' is not one flow knows (pekko-streams, flink)"
    )
  }

  test("a resource whose engine differs from its descriptor's is refused") {
    val r       = cart()
    val cleaner = r.getSpec.streamlets.head
    r.setSpec(
      r.getSpec.copy(streamlets = cleaner.copy(engine = "flink") :: r.getSpec.streamlets.tail)
    )
    refusedWith(render(r), "the resource says engine 'flink' and its descriptor 'pekko-streams'")
  }

  // ── Lag on the status, whatever the engine ───────────────────────────────────────────────

  test("each streamlet's inlets carry their topic, group and lag on the status, for both engines") {
    val o  = flinkObserved.copy(lag = Map("cart.cleaner.in" -> 7L, "cart.counter.in" -> 12L))
    val st = status(Rendering.render(cartWithCounter(), settings, o, "t"))
    assertEquals(
      st.streamlets.map(s => s.name -> s.inlets).toMap,
      Map(
        "cleaner" -> List(InletStatus("in", "shop.cart-events.v1", "cart.cleaner.in", Some(7L))),
        "counter" -> List(InletStatus("in", "cart.clean-carts", "cart.counter.in", Some(12L)))
      )
    )
    val unknown = status(Rendering.render(cartWithCounter(), settings, flinkObserved, "t"))
    assertEquals(unknown.streamlets.flatMap(_.inlets).map(_.lag), List(None, None))
  }

  test("a changed lag alone is a different report, so the status is written again") {
    def at(lag: Long) =
      status(
        Rendering.render(
          cartWithCounter(),
          settings,
          flinkObserved.copy(lag = Map("cart.counter.in" -> lag)),
          "t"
        )
      )
    assert(at(3L).sameReport(at(3L)))
    assert(!at(3L).sameReport(at(4L)))
  }

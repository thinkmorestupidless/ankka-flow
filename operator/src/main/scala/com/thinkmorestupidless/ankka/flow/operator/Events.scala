package com.thinkmorestupidless.ankka.flow.operator

import java.nio.charset.StandardCharsets.UTF_8
import java.security.MessageDigest
import java.time.Instant

import com.thinkmorestupidless.ankka.flow.crd.{AnkkaFlow, AnkkaFlowDefinition}
import io.fabric8.kubernetes.api.model.{MicroTime, ObjectMetaBuilder, ObjectReferenceBuilder}
import io.fabric8.kubernetes.api.model.events.v1.{Event, EventBuilder}

/**
 * Kubernetes Events on the pipeline (`events.k8s.io/v1`, FR-022). Named by the pipeline, the reason
 * and the note, so the same event is written once however often a reconcile repeats it.
 */
object Events:

  val Normal  = "Normal"
  val Warning = "Warning"

  val ReportingController = s"${AnkkaFlowDefinition.domain}/operator"

  /** RFC 3339 with microseconds, as `MicroTime` requires. */
  private val MicroFormat =
    java.time.format.DateTimeFormatter
      .ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'")
      .withZone(java.time.ZoneOffset.UTC)

  def build(
      resource: AnkkaFlow,
      reason: String,
      eventType: String,
      note: String,
      instance: String
  ): Event =
    val meta = resource.getMetadata
    val digest = MessageDigest
      .getInstance("SHA-256")
      .digest(s"$reason\n$note".getBytes(UTF_8))
      .take(6)
      .map(b => f"$b%02x")
      .mkString
    new EventBuilder()
      .withMetadata(
        new ObjectMetaBuilder()
          .withName(s"${meta.getName}.${reason.toLowerCase}.$digest")
          .withNamespace(meta.getNamespace)
          .build()
      )
      .withRegarding(
        new ObjectReferenceBuilder()
          .withApiVersion(AnkkaFlowDefinition.apiVersion)
          .withKind(AnkkaFlowDefinition.kind)
          .withName(meta.getName)
          .withNamespace(meta.getNamespace)
          .withUid(meta.getUid)
          .build()
      )
      .withReason(reason)
      .withType(eventType)
      .withNote(note.take(1024))
      .withAction("Reconcile")
      .withEventTime(new MicroTime(MicroFormat.format(Instant.now())))
      .withReportingController(ReportingController)
      .withReportingInstance(instance.take(128))
      .build()

package com.thinkmorestupidless.ankka.flow.sidecar

import java.io.ByteArrayInputStream
import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.file.{Files, Path, Paths}
import java.security.KeyStore
import java.security.cert.CertificateFactory
import java.time.{Duration, Instant, ZoneOffset}
import java.time.format.DateTimeFormatter
import java.util.UUID
import javax.net.ssl.{SSLContext, TrustManagerFactory}

import scala.util.Try

import com.thinkmorestupidless.ankka.flow.protocol.Json
import org.slf4j.LoggerFactory

object EventSinks:
  /** In a pod, Kubernetes Events; elsewhere, log lines (research R9). */
  def forSettings(settings: Settings): EventSink =
    settings.pod.fold[EventSink](new LogEventSink)(pod => new KubernetesEventSink(pod))

/**
 * A `Warning` Event regarding the sidecar's own pod, posted to the API server with the pod's
 * projected service-account token: one HTTP call, no client library. Any failure falls back to a
 * log line, so a missing permission never hides the warning.
 */
final class KubernetesEventSink(
    pod: Settings.Pod,
    tokenDir: Path = Paths.get("/var/run/secrets/kubernetes.io/serviceaccount")
) extends EventSink:

  private val log      = LoggerFactory.getLogger(classOf[KubernetesEventSink])
  private val fallback = new LogEventSink

  private val MicroTime =
    DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'").withZone(ZoneOffset.UTC)

  private lazy val client: HttpClient =
    val ca    = Files.readAllBytes(tokenDir.resolve("ca.crt"))
    val store = KeyStore.getInstance(KeyStore.getDefaultType)
    store.load(null, null)
    CertificateFactory
      .getInstance("X.509")
      .generateCertificates(new ByteArrayInputStream(ca))
      .forEach { c =>
        store.setCertificateEntry(UUID.randomUUID().toString, c)
      }
    val tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
    tmf.init(store)
    val ssl = SSLContext.getInstance("TLS")
    ssl.init(null, tmf.getTrustManagers, null)
    HttpClient.newBuilder().sslContext(ssl).connectTimeout(Duration.ofSeconds(5)).build()

  def body(reason: String, note: String, now: Instant): String =
    import Json.*
    Json.compact(
      Obj(
        Vector(
          "apiVersion" -> Str("events.k8s.io/v1"),
          "kind"       -> Str("Event"),
          "metadata" -> Obj(
            Vector("generateName" -> Str(s"${pod.name}."), "namespace" -> Str(pod.namespace))
          ),
          "regarding" -> Obj(
            Vector(
              "apiVersion" -> Str("v1"),
              "kind"       -> Str("Pod"),
              "name"       -> Str(pod.name),
              "namespace"  -> Str(pod.namespace)
            )
          ),
          "reason"              -> Str(reason),
          "type"                -> Str("Warning"),
          "note"                -> Str(note.take(1024)),
          "action"              -> Str("Consume"),
          "eventTime"           -> Str(MicroTime.format(now)),
          "reportingController" -> Str("flow.ankka.thinkmorestupidless.com/sidecar"),
          "reportingInstance"   -> Str(pod.name)
        )
      )
    )

  def warning(reason: String, note: String): Unit =
    fallback.warning(reason, note)
    Try {
      val token = new String(Files.readAllBytes(tokenDir.resolve("token")), "UTF-8").trim
      val request = HttpRequest
        .newBuilder(
          URI.create(
            s"https://${pod.apiHost}:${pod.apiPort}/apis/events.k8s.io/v1/namespaces/${pod.namespace}/events"
          )
        )
        .header("Authorization", s"Bearer $token")
        .header("Content-Type", "application/json")
        .timeout(Duration.ofSeconds(10))
        .POST(HttpRequest.BodyPublishers.ofString(body(reason, note, Instant.now())))
        .build()
      client.send(request, HttpResponse.BodyHandlers.ofString())
    }.fold(
      e => log.warn("could not record the warning as a Kubernetes Event: {}", e.getMessage),
      r =>
        if r.statusCode / 100 != 2 then
          log.warn("the API server refused the event: {} {}", r.statusCode, r.body.take(300))
    )

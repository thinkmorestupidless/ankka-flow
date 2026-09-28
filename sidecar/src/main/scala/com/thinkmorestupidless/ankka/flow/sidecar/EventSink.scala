package com.thinkmorestupidless.ankka.flow.sidecar

import org.slf4j.LoggerFactory

/** Where the sidecar's warnings go: a Kubernetes Event in a pod, a log line elsewhere (R9). */
trait EventSink:
  def warning(reason: String, note: String): Unit

final class LogEventSink extends EventSink:
  private val log = LoggerFactory.getLogger("com.thinkmorestupidless.ankka.flow.sidecar.events")
  def warning(reason: String, note: String): Unit = log.warn("{}: {}", reason, note)

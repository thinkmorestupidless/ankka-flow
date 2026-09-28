package com.thinkmorestupidless.ankka.flow.sidecar

import java.nio.file.{Files, Path}
import java.nio.file.attribute.FileTime
import java.time.Instant

import scala.util.Try

/**
 * Readiness and liveness as files the container's exec probes read (research R7). `ready` exists
 * only while the conversation runs and every inlet is subscribed; `alive` is touched every second
 * while the sidecar's loop runs.
 */
final class Probes(dir: Path):

  private val readyFile = dir.resolve("ready")
  private val aliveFile = dir.resolve("alive")

  Files.createDirectories(dir)

  @volatile private var isReady = false

  def ready(): Unit =
    isReady = true
    Try(Files.write(readyFile, Array.emptyByteArray)): Unit

  def notReady(): Unit =
    isReady = false
    Try(Files.deleteIfExists(readyFile)): Unit

  def readyNow: Boolean = isReady

  def alive(): Unit =
    Try {
      if !Files.exists(aliveFile) then Files.write(aliveFile, Array.emptyByteArray): Unit
      Files.setLastModifiedTime(aliveFile, FileTime.from(Instant.now()))
    }: Unit

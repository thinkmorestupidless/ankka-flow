package com.thinkmorestupidless.ankka.flow.sidecar

import java.nio.file.{Files, Path}
import java.util.concurrent.ConcurrentLinkedQueue

import scala.concurrent.{Await, Future, Promise}
import scala.concurrent.duration.*

import ankka.flow.v1.discovery.Spec
import com.thinkmorestupidless.ankka.flow.protocol.DescriptorJson
import org.apache.pekko.actor.ActorSystem

/** A captured warning, for suites that assert on them. */
final class CapturingEventSink extends EventSink:
  val warnings                                    = new ConcurrentLinkedQueue[(String, String)]()
  def warning(reason: String, note: String): Unit = warnings.add(reason -> note): Unit

/**
 * One whole sidecar, in-process: the real Supervisor, Discovery, Conversation and inlet graphs,
 * configured from files in a temporary directory exactly as in a pod.
 */
final class SidecarRun(
    deployed: Spec,
    streamletConf: String,
    processPort: Int,
    stallAfter: FiniteDuration = 5.minutes
)(using system: ActorSystem):

  val dir: Path      = Files.createTempDirectory("flow-sidecar")
  val stateDir: Path = dir.resolve("state")
  Files.write(dir.resolve("descriptor.json"), DescriptorJson.write(deployed).getBytes("UTF-8"))
  Files.write(dir.resolve("streamlet.conf"), streamletConf.getBytes("UTF-8"))

  val events = new CapturingEventSink
  val settings = Settings(
    processHost = "127.0.0.1",
    processPort = processPort,
    configDir = dir,
    stateDir = stateDir,
    metricsPort = 0,
    discoveryTimeout = 5.seconds,
    stallWarningAfter = stallAfter,
    reconnectMaxBackoff = 2.seconds,
    pod = None
  )
  val descriptor = Descriptor
    .load(dir.resolve("descriptor.json"))
    .fold(e => throw new IllegalStateException(e.mkString), identity)
  val config = StreamletConfig
    .load(dir.resolve("streamlet.conf"))
    .fold(e => throw new IllegalStateException(e.mkString), identity)
  val probes     = new Probes(stateDir)
  val stalls     = new Stalls(stallAfter, events)
  val supervisor = new Supervisor(settings, descriptor, config, probes, stalls)

  private val exitCode = Promise[Int]()
  private val thread =
    new Thread(() => exitCode.complete(scala.util.Try(supervisor.run())): Unit, "supervisor")
  thread.setDaemon(true)
  thread.start()

  private val checker = system.scheduler.scheduleAtFixedRate(1.second, 200.millis)(() =>
    stalls.check((_, _) => s"${config.pipeline}.${config.streamlet}")
  )(using system.dispatcher)

  def ready: Boolean = Files.exists(stateDir.resolve("ready"))

  def exited: Future[Int] = exitCode.future

  def stop(): Int =
    supervisor.requestStop()
    checker.cancel(): Unit
    Await.result(exitCode.future, 40.seconds)

object SidecarRun:
  def conf(
      pipeline: String,
      streamlet: String,
      bootstrap: String,
      inlets: Seq[(String, String)],
      outlets: Seq[(String, String)],
      maxRecords: Int = 100,
      config: String = ""
  ): String =
    val in = inlets.map { (port, topic) =>
      s"""  "$port" { topic = "$topic", bootstrap.servers = "$bootstrap", batch { max-records = $maxRecords } }"""
    }
    val out = outlets.map { (port, topic) =>
      s"""  "$port" { topic = "$topic", bootstrap.servers = "$bootstrap" }"""
    }
    s"""flow {
       |  pipeline = "$pipeline"
       |  streamlet = "$streamlet"
       |  config { $config }
       |  inlets {
       |${in.mkString("\n")}
       |  }
       |  outlets {
       |${out.mkString("\n")}
       |  }
       |}""".stripMargin

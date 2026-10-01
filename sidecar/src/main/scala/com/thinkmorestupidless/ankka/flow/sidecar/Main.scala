package com.thinkmorestupidless.ankka.flow.sidecar

import java.util.concurrent.{CountDownLatch, TimeUnit}

import scala.concurrent.Await
import scala.concurrent.duration.*
import scala.util.Try

import org.apache.pekko.actor.ActorSystem
import org.slf4j.LoggerFactory

/** The sidecar: owns Kafka for one streamlet and speaks the protocol to its process. */
object Main:

  private val log = LoggerFactory.getLogger("com.thinkmorestupidless.ankka.flow.sidecar.Main")

  @volatile private var shuttingDown = false

  def main(args: Array[String]): Unit =
    val code = run()
    if !shuttingDown then sys.exit(code)

  def run(): Int =
    val loaded = for
      settings   <- Settings.load().left.map(Vector(_))
      descriptor <- Descriptor.load(settings.configDir.resolve("descriptor.json"))
      config     <- StreamletConfig.load(settings.configDir.resolve("streamlet.conf"))
      _ <- config.check(descriptor.streamlet) match
        case Vector() => Right(())
        case problems => Left(problems)
    yield (settings, descriptor, config)

    loaded match
      case Left(problems) =>
        log.error("refusing to start:")
        problems.foreach(p => log.error("  {}", p))
        2
      case Right((settings, descriptor, config)) =>
        config.stage match
          case Some(stage) =>
            log.info(
              "ankka-flow sidecar {} for {}.{}: stage '{}', no process",
              BuildInfo.version,
              config.pipeline,
              config.streamlet,
              stage.name
            )
          case None =>
            log.info(
              "ankka-flow sidecar {} for {}.{} (streamlet '{}'), process at {}",
              BuildInfo.version,
              config.pipeline,
              config.streamlet,
              descriptor.streamlet.name,
              settings.processAddress
            )
        given system: ActorSystem = ActorSystem("flow-sidecar")
        val probes                = new Probes(settings.stateDir)
        val events                = EventSinks.forSettings(settings)
        val stalls                = new Stalls(settings.stallWarningAfter, events)
        val metrics               = new Metrics(stalls)
        import system.dispatcher
        system.scheduler.scheduleAtFixedRate(Duration.Zero, 1.second)(() => probes.alive())
        system.scheduler.scheduleAtFixedRate(1.second, 1.second) { () =>
          stalls.check((_, _) => s"${config.pipeline}.${config.streamlet}")
          metrics.refresh()
        }
        val stage: Stage = config.stage match
          case Some(_) => new Neo4jMergeStage(descriptor, config, events, metrics.stage)
          case None    => new ProcessStage(settings, descriptor, config)
        val supervisor = new Supervisor(settings, config, probes, stalls, stage)
        val finished   = new CountDownLatch(1)
        sys.addShutdownHook {
          shuttingDown = true
          log.info("shutting down")
          supervisor.requestStop()
          finished.await(25, TimeUnit.SECONDS): Unit
        }
        val code =
          try supervisor.run()
          finally
            probes.notReady()
            Try(Await.ready(system.terminate(), 10.seconds))
            finished.countDown()
        log.info("exiting with {}", code)
        code

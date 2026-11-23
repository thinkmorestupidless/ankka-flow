package com.thinkmorestupidless.ankka.flow.pekko

import java.nio.file.Paths

import scala.concurrent.Await
import scala.concurrent.duration.*
import scala.util.control.NonFatal

import com.thinkmorestupidless.ankka.flow.descriptor.Settings
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.actor.typed.scaladsl.Behaviors

/**
 * The main of a Pekko Streams streamlet's image:
 * {{{
 * object Main:
 *   def main(args: Array[String]): Unit = PekkoStreamlets.run(Cleaner, args)
 * }}}
 * `--descriptor <path>` writes its descriptor, `--descriptor-check <path>` compares it, and no
 * argument runs it from the `streamlet.properties` in the directory `FLOW_CONFIG_DIR` names.
 */
object PekkoStreamlets:

  def run(streamlet: PekkoStreamlet, args: Array[String]): Unit =
    DescriptorFiles.handle(streamlet, args.toList) match
      case Some(code) => sys.exit(code)
      case None =>
        val started =
          try
            val s = settings(sys.env).fold(e => throw new IllegalArgumentException(e), identity)
            val system = ActorSystem(Behaviors.empty[Nothing], s"flow-${s.streamlet}")
            try
              val running = Runner.start(streamlet, s, system, new Probes(Probes.Directory))
              Right((system, running))
            catch
              case NonFatal(e) =>
                system.terminate()
                throw e
          catch case NonFatal(e) => Left(e.getMessage)
        started match
          case Left(e) =>
            System.err.println(s"${streamlet.name}: $e")
            sys.exit(1)
          case Right((system, running)) =>
            sys.addShutdownHook {
              scala.util.Try(Await.ready(running.stop(), 30.seconds))
              system.terminate()
              scala.util.Try(Await.ready(system.whenTerminated, 30.seconds)): Unit
            }: Unit
            Await.ready(running.done, Duration.Inf)
            system.terminate()
            Await.ready(system.whenTerminated, Duration.Inf): Unit

  /** The settings in the directory `FLOW_CONFIG_DIR` names in `env`. */
  def settings(env: Map[String, String]): Either[String, Settings] =
    env
      .get(Settings.DirectoryVariable)
      .toRight(
        s"${Settings.DirectoryVariable} is not set; it names the directory of ${Settings.FileName}"
      )
      .flatMap(dir => Settings.read(Paths.get(dir)))

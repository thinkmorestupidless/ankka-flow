package com.thinkmorestupidless.ankka.flow.sdk

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path, Paths}

import scala.util.control.NonFatal

import ankka.flow.v1.discovery.{SdkInfo, Spec}
import com.thinkmorestupidless.ankka.flow.protocol.{
  DescriptorJson,
  DescriptorValidation,
  ProtocolVersion
}

/**
 * The descriptor: what a streamlet declares, as the protocol's canonical JSON. A blueprint is
 * verified against it, and the sidecar compares it with the running process before it starts.
 */
object Descriptor:
  val SdkName = "ankka-flow-scala"

  def sdk: SdkInfo = SdkInfo(SdkName, SdkBuildInfo.version)

  /** The discovery `Spec` the streamlet declares: ports sorted by name, parameters by key. */
  def spec(streamlet: Streamlet, sdk: SdkInfo = Descriptor.sdk): Spec =
    Spec(ProtocolVersion.Current.toString, Some(sdk), Some(streamlet.descriptor))

  /** Every rule of the protocol the declaration breaks; empty when it is a valid descriptor. */
  def validate(streamlet: Streamlet): Vector[String] =
    DescriptorValidation.validate(spec(streamlet))

  /**
   * Canonical JSON: the bytes every SDK writes for the same declaration. Refuses an invalid one.
   */
  def write(streamlet: Streamlet, sdk: SdkInfo = Descriptor.sdk): String =
    Streamlet.refuse(validate(streamlet))
    DescriptorJson.write(spec(streamlet, sdk))

  /** A streamlet by class name, constructed with no arguments. */
  def instantiate(className: String): Streamlet =
    Class.forName(className).getDeclaredConstructor().newInstance() match
      case s: Streamlet => s
      case other =>
        throw new IllegalArgumentException(
          s"$className is not a Streamlet (${other.getClass.getName})"
        )

  /**
   * `<streamlet class> <path> [--check]`: write the descriptor to `path`, or with `--check` exit 1
   * when the file there is missing or differs. Exit 2 when the streamlet cannot be constructed.
   */
  def main(args: Array[String]): Unit = sys.exit(run(args.toList))

  def run(
      args: List[String],
      out: java.io.PrintStream = System.out,
      err: java.io.PrintStream = System.err
  ): Int =
    val check = args.contains("--check")
    args.filterNot(_ == "--check") match
      case className :: path :: Nil =>
        val file = Paths.get(path)
        try
          val text = write(instantiate(className))
          if check then
            if Files.exists(file) && new String(Files.readAllBytes(file), UTF_8) == text then
              out.println(s"$path is current"); 0
            else
              err.println(
                s"$path differs from what $className declares; write it with the descriptor command"
              )
              1
          else
            writeFile(file, text)
            out.println(s"wrote $path"); 0
        catch
          case NonFatal(e) =>
            val cause = Option(e.getCause)
              .filter(_ => e.isInstanceOf[java.lang.reflect.InvocationTargetException])
              .getOrElse(e)
            err.println(s"$className: ${cause.getClass.getSimpleName}: ${cause.getMessage}")
            2
      case _ =>
        err.println("usage: Descriptor <streamlet class> <path> [--check]")
        2

  private def writeFile(file: Path, text: String): Unit =
    Option(file.toAbsolutePath.getParent).foreach(Files.createDirectories(_))
    Files.write(file, text.getBytes(UTF_8)): Unit

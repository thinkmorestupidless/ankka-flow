package com.thinkmorestupidless.ankka.flow.sdk

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.FiniteDuration

import ankka.flow.v1.discovery.{
  ConfigParameter,
  ConfigType,
  Contract,
  Port as PortSpec,
  StreamletDescriptor
}
import com.thinkmorestupidless.ankka.flow.protocol.DescriptorValidation

/**
 * A streamlet: declare its ports and parameters as `val`s with the factories below, and implement
 * `process`.
 *
 * {{{
 * final class CartRouter extends Streamlet("cart-router", "Routes cart events."):
 *   val in     = inlet("in", schemaName = "cart-events.v1")
 *   val valid  = outlet("valid", schemaName = "cart-events.v1")
 *   val limit  = parameter.integer("review-threshold", default = 100)
 *   def process(batch: Batch): Iterable[Emit] = ...
 * }}}
 *
 * Each factory registers what it returns, so the declaration is the construction of the object:
 * nothing is discovered by scanning. A port name or parameter key declared twice, or one the
 * descriptor rules refuse, throws `IllegalArgumentException` where it is declared.
 *
 * `process` is called once per batch on a worker thread. The sidecar keeps one batch in flight per
 * inlet partition, so it never runs twice at once for one partition, but it may run concurrently
 * for different partitions. Returning acknowledges the batch; throwing fails it; emitting nothing
 * for a record skips it. The SDK keeps nothing between batches.
 */
abstract class Streamlet(val name: String, val description: String = ""):
  private val declaredInlets                    = ArrayBuffer.empty[JsonInlet]
  private val declaredOutlets                   = ArrayBuffer.empty[Outlet]
  private val declaredParameters                = ArrayBuffer.empty[Parameter[?]]
  @volatile private var resolved: Config | Null = null

  Streamlet.refuse(
    DescriptorValidation
      .validateStreamlet(StreamletDescriptor(name = name))
      .filter(_.startsWith("streamlet name"))
  )

  /** Answer one batch with the emits it produces. */
  def process(batch: Batch): Iterable[Emit]

  final def inlets: Seq[JsonInlet]        = declaredInlets.toVector
  final def outlets: Seq[Outlet]          = declaredOutlets.toVector
  final def parameters: Seq[Parameter[?]] = declaredParameters.toVector

  /** The parameter values: the declared defaults until `configure` applies deployed ones. */
  final def config: Config =
    val c = resolved
    if c != null then c
    else
      val defaults = Config.resolve(parameters, Map.empty)
      resolved = defaults
      defaults

  /** Resolve `values` — each the text the protocol carries — over the declared defaults. */
  final def configure(values: Map[String, String]): Unit =
    resolved = Config.resolve(parameters, values)

  // ── declarations ──────────────────────────────────────────────────────────────────────────────

  /** An inlet carrying JSON values named by `schemaName`. */
  protected final def inlet(name: String, schemaName: String): JsonInlet =
    registerPort(new JsonInlet(name, schemaName), "inlet")(declaredInlets += _)

  /** An outlet carrying JSON values named by `schemaName`. */
  protected final def outlet(name: String, schemaName: String): JsonOutlet =
    registerPort(new JsonOutlet(name, schemaName), "outlet")(declaredOutlets += _)

  /** An outlet of graph deltas (`ankka.graph-delta.v1`), each keyed by its element. */
  protected final def graphDeltaOutlet(name: String): GraphDeltaOutlet =
    registerPort(new GraphDeltaOutlet(name), "outlet")(declaredOutlets += _)

  /**
   * Typed parameters. A default is the value (`100`, `0.5`, `true`, `100.millis`) or its text as
   * the protocol carries it (`"100 ms"`, `"1 MiB"`); with none, the parameter must be set at deploy
   * time.
   */
  protected object parameter:
    def string(key: String, default: String = "", description: String = ""): Parameter[String] =
      register(key, ConfigType.STRING, default, description, Parameter.string)

    def integer(
        key: String,
        default: Long | Int | String | Null = null,
        description: String = ""
    ): Parameter[Long] =
      register(key, ConfigType.INTEGER, render(default), description, Parameter.integer)

    def double(
        key: String,
        default: Double | String | Null = null,
        description: String = ""
    ): Parameter[Double] =
      register(key, ConfigType.DOUBLE, render(default), description, Parameter.double)

    def boolean(
        key: String,
        default: Boolean | String | Null = null,
        description: String = ""
    ): Parameter[Boolean] =
      register(key, ConfigType.BOOLEAN, render(default), description, Parameter.boolean)

    def duration(
        key: String,
        default: FiniteDuration | String | Null = null,
        description: String = ""
    ): Parameter[FiniteDuration] =
      val text = default match
        case d: FiniteDuration => Parameter.renderDuration(d)
        case other             => render(other)
      register(key, ConfigType.DURATION, text, description, Parameter.duration)

    /** A size in bytes. */
    def memorySize(
        key: String,
        default: Long | Int | String | Null = null,
        description: String = ""
    ): Parameter[Long] =
      register(key, ConfigType.MEMORY_SIZE, render(default), description, Parameter.memorySize)

    private def render(default: Any): String = default match
      case null  => ""
      case other => other.toString

    private def register[T](
        key: String,
        t: ConfigType,
        default: String,
        description: String,
        parse: String => Either[String, T]
    ): Parameter[T] =
      val p = new Parameter[T](key, t, default, description, parse)
      val problems = DescriptorValidation
        .validateStreamlet(
          StreamletDescriptor(name = "x", configParameters = Seq(Streamlet.paramSpec(p)))
        )
      Streamlet.refuse(problems)
      if declaredParameters.exists(_.key == key) then
        Streamlet.refuse(Vector(s"parameter '$key' is declared more than once"))
      declaredParameters += p
      p

  private def registerPort[P <: Port](p: P, kind: String)(add: P => Unit): P =
    Streamlet.refuse(
      DescriptorValidation
        .validateStreamlet(StreamletDescriptor(name = "x", inlets = Seq(Streamlet.portSpec(p))))
        .map(_.replaceFirst("^inlet", kind))
    )
    if (declaredInlets ++ declaredOutlets).exists(_.name == p.name) then
      Streamlet.refuse(Vector(s"port '${p.name}' is declared more than once"))
    add(p)
    p

  /** The streamlet as the protocol describes it: ports sorted by name, parameters by key. */
  private[sdk] final def descriptor: StreamletDescriptor =
    StreamletDescriptor(
      name = name,
      description = description,
      inlets = inlets.sortBy(_.name).map(Streamlet.portSpec),
      outlets = outlets.sortBy(_.name).map(Streamlet.portSpec),
      configParameters = parameters.sortBy(_.key).map(Streamlet.paramSpec)
    )

object Streamlet:

  private[sdk] def refuse(problems: Seq[String]): Unit =
    if problems.nonEmpty then throw new IllegalArgumentException(problems.mkString("; "))

  private[sdk] def portSpec(p: Port): PortSpec =
    PortSpec(p.name, Some(Contract(p.format, p.schemaName, p.fingerprint)))

  private[sdk] def paramSpec(p: Parameter[?]): ConfigParameter =
    ConfigParameter(p.key, p.description, p.configType, p.defaultValue)

  /**
   * Run `process` over one batch, refusing an emit to an outlet the streamlet does not declare.
   * Shared by the server and the harness, so both fail a batch the same way.
   */
  def runBatch(streamlet: Streamlet, batch: Batch): Iterator[Emit] =
    val declared = streamlet.outlets.map(_.name).toSet
    streamlet.process(batch).iterator.map { e =>
      if !declared(e.outlet) then throw new UndeclaredOutlet(e.outlet)
      e
    }

/** An emit to an outlet the streamlet does not declare: the batch fails. */
final class UndeclaredOutlet(val outlet: String)
    extends RuntimeException(s"emit to undeclared outlet '$outlet'")

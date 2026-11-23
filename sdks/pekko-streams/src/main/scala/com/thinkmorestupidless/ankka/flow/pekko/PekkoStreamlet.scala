package com.thinkmorestupidless.ankka.flow.pekko

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.FiniteDuration

import com.thinkmorestupidless.ankka.flow.descriptor.{
  Descriptor,
  DescriptorValidation,
  Engine,
  Parameter as DeclaredParameter,
  ParameterType,
  Port,
  SdkInfo,
  StreamletDescriptor
}
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.FlowWithContext

/**
 * A streamlet on Pekko Streams: a name, its inlets, outlets and parameters declared as values, and
 * its logic, one stage per inlet. The main names it: `PekkoStreamlets.run(Cleaner, args)`.
 *
 * {{{
 * object Cleaner extends PekkoStreamlet("cart-cleaner"):
 *   val in       = inlet("in", Contract("cart-events.v1"), Codec.json[CartEvent])
 *   val out      = outlet("out", Contract("clean-carts.v1"), Codec.json[CleanCart])
 *   val maxTotal = intParameter("max-total", default = 10000)
 *
 *   def logic(ctx: Context) = from(in)(_.filter(_.value.total <= ctx(maxTotal)).map(r => out(r.key.get, clean(r.value))))
 * }}}
 */
abstract class PekkoStreamlet(val name: String, val description: String = ""):

  private val declaredInlets     = ListBuffer.empty[Inlet[?]]
  private val declaredOutlets    = ListBuffer.empty[Outlet[?]]
  private val declaredParameters = ListBuffer.empty[Parameter[?]]

  final def inlets: Vector[Inlet[?]]         = declaredInlets.toVector
  final def outlets: Vector[Outlet[?]]       = declaredOutlets.toVector
  final def parameters: Vector[Parameter[?]] = declaredParameters.toVector

  protected def inlet[A](name: String, contract: Contract, codec: Codec[A]): Inlet[A] =
    val i = new Inlet(name, contract, codec)
    declaredInlets += i
    i

  protected def outlet[A](name: String, contract: Contract, codec: Codec[A]): Outlet[A] =
    val o = new Outlet(name, contract, codec)
    declaredOutlets += o
    o

  private def declare[A](p: Parameter[A]): Parameter[A] =
    declaredParameters += p
    p

  private def text[A](default: A | Null): Option[String] =
    Option(default).map(_.toString)

  protected def stringParameter(
      key: String,
      default: String | Null = null,
      description: String = ""
  ): Parameter[String] =
    declare(
      Parameter.declare(key, ParameterType.String, text(default), description, Parameter.string)
    )

  protected def intParameter(
      key: String,
      default: Int | Null = null,
      description: String = ""
  ): Parameter[Int] =
    declare(
      Parameter.declare(key, ParameterType.Integer, text(default), description, Parameter.int)
    )

  protected def doubleParameter(
      key: String,
      default: Double | Null = null,
      description: String = ""
  ): Parameter[Double] =
    declare(
      Parameter.declare(key, ParameterType.Double, text(default), description, Parameter.double)
    )

  protected def booleanParameter(
      key: String,
      default: Boolean | Null = null,
      description: String = ""
  ): Parameter[Boolean] =
    declare(
      Parameter.declare(key, ParameterType.Boolean, text(default), description, Parameter.boolean)
    )

  /** A duration, its default as text: `100 ms`, `1m`, `30s`. */
  protected def durationParameter(
      key: String,
      default: String | Null = null,
      description: String = ""
  ): Parameter[FiniteDuration] =
    declare(
      Parameter.declare(key, ParameterType.Duration, text(default), description, Parameter.duration)
    )

  /** A size in bytes, its default as text: `1 MiB`, `512k`. */
  protected def memorySizeParameter(
      key: String,
      default: String | Null = null,
      description: String = ""
  ): Parameter[Long] =
    declare(
      Parameter.declare(
        key,
        ParameterType.MemorySize,
        text(default),
        description,
        Parameter.memorySize
      )
    )

  /**
   * The stage for `inlet`: given its records with their offsets, what to write. The elements of
   * what it returns are an `Emit`, a `Seq` of them (`Nil` writes nothing) or an `Option`.
   */
  protected def from[A](inlet: Inlet[A])(flow: Stage[A] => Stage.Out): Logic =
    val out = flow(FlowWithContext[Record[A], Offset])
      .asInstanceOf[FlowWithContext[Record[
        A
      ], Offset, Emit | Seq[Emit] | Option[Emit], Offset, Any]]
      .map(Stage.emits)
      .asInstanceOf[FlowWithContext[Record[A], Offset, Seq[Emit], Offset, NotUsed]]
    new Logic(Vector(Logic.Part(inlet, out)))

  /** What the streamlet does with its inlets, given its parameters. */
  def logic(ctx: Context): Logic

  /**
   * The descriptor this declaration writes, refused with every problem when it is not a valid one.
   * `sdk` is this SDK unless a test names another.
   */
  final def descriptor(sdk: SdkInfo = PekkoStreamlet.Sdk): Descriptor =
    val loader = Option(getClass.getClassLoader).getOrElse(ClassLoader.getSystemClassLoader)
    def ports(ps: Vector[(String, Contract)]) = ps.map { (n, c) =>
      c.declared(loader).map(Port(n, _))
    }
    val in       = ports(inlets.map(i => i.name -> i.contract))
    val out      = ports(outlets.map(o => o.name -> o.contract))
    val schemata = (in ++ out).collect { case Left(e) => e }
    val d = Descriptor(
      Descriptor.Version,
      Some(sdk),
      StreamletDescriptor(
        name,
        description,
        Engine.PekkoStreams,
        in.collect { case Right(p) => p },
        out.collect { case Right(p) => p },
        parameters.map(p => DeclaredParameter(p.key, p.description, p.kind, p.default)),
        None
      )
    )
    val problems = schemata ++ DescriptorValidation.validate(d)
    if problems.nonEmpty then
      throw new IllegalArgumentException(
        s"streamlet '$name' is not a valid declaration: ${problems.mkString("; ")}"
      )
    d

object PekkoStreamlet:
  val SdkName = "ankka-flow-pekko-streams"

  val Sdk: SdkInfo = SdkInfo(SdkName, PekkoSdkBuildInfo.version)

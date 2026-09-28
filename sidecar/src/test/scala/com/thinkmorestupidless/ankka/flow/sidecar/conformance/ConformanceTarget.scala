package com.thinkmorestupidless.ankka.flow.sidecar.conformance

import com.thinkmorestupidless.ankka.flow.sidecar.{ProcessDouble, TestSpecs}

/**
 * What the conformance suite drives: the Scala reference in-process (the default), or any process
 * already listening at `-Dflow.conformance.target=host:port`.
 */
sealed trait ConformanceTarget:
  def name: String
  def host: String
  def port: Int
  def external: Boolean
  def close(): Unit

object ConformanceTarget:

  final class InProcess extends ConformanceTarget:
    private val reference = ConformanceReference.start()
    val name              = "in-process Scala reference"
    val host              = "127.0.0.1"
    val port              = reference.port
    val external          = false
    def close(): Unit     = reference.close()

  final class Remote(address: String) extends ConformanceTarget:
    val name          = s"a process at $address"
    val host          = address.substring(0, address.lastIndexOf(':'))
    val port          = address.substring(address.lastIndexOf(':') + 1).toInt
    val external      = true
    def close(): Unit = ()

  def fromProperties(): ConformanceTarget =
    sys.props.get("flow.conformance.target").filter(_.nonEmpty) match
      case Some(address) => new Remote(address)
      case None          => new InProcess

/**
 * The Scala reference streamlet: the `conformance` declaration of protocol/fixtures, behaving by
 * each record's key as contracts/conformance.md says. It is the scriptable double, which already
 * implements every keyed behaviour.
 */
object ConformanceReference:
  def start(): ProcessDouble =
    val double = new ProcessDouble(TestSpecs.fixture("conformance"), ProcessDouble.keyed())
    double.start()
    double

package com.thinkmorestupidless.ankka.flow.sidecar.conformance

import com.thinkmorestupidless.ankka.flow.sdk.Serve
import com.thinkmorestupidless.ankka.flow.sdk.conformance.Conformance
import com.thinkmorestupidless.ankka.flow.sidecar.{ProcessDouble, TestSpecs}

/**
 * What the conformance suite drives: the Scala SDK's reference streamlet in process (the default),
 * or any process already listening at `-Dflow.conformance.target=host:port`. The misbehaviour a
 * correct SDK cannot produce is scripted with the double, inside the suite.
 */
sealed trait ConformanceTarget:
  def name: String
  def host: String
  def port: Int
  def external: Boolean
  def close(): Unit

object ConformanceTarget:

  final class InProcess extends ConformanceTarget:
    private val server = Serve.start(new Conformance, port = 0)
    val name           = "in-process Scala SDK"
    val host           = Serve.Loopback
    val port           = server.port
    val external       = false
    def close(): Unit  = server.close()

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
 * The scriptable double declared as the `conformance` streamlet, for the `violation.*` and
 * `version.*` cases: it produces the misbehaviour a correct SDK cannot.
 */
object ConformanceReference:
  def start(): ProcessDouble =
    val double = new ProcessDouble(TestSpecs.fixture("conformance"), ProcessDouble.keyed())
    double.start()
    double

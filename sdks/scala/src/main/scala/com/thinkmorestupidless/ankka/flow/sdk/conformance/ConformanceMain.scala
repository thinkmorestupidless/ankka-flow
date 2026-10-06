package com.thinkmorestupidless.ankka.flow.sdk.conformance

import com.thinkmorestupidless.ankka.flow.sdk.Serve

/**
 * Serves the reference streamlet on `127.0.0.1:<port>` (default 9010) for the conformance suite's
 * remote mode: `sbt 'sidecar/testOnly *ConformanceSuite' -Dflow.conformance.target=127.0.0.1:9010`.
 */
object ConformanceMain:
  def main(args: Array[String]): Unit =
    val port   = args.headOption.map(_.toInt).getOrElse(Serve.DefaultPort)
    val server = Serve.start(new Conformance, port)
    println(s"conformance reference serving on ${Serve.Loopback}:${server.port}")
    Runtime.getRuntime.addShutdownHook(new Thread(() => server.close()))
    server.awaitTermination()

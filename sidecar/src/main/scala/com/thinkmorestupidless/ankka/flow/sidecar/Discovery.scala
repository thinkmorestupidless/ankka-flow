package com.thinkmorestupidless.ankka.flow.sidecar

import java.util.concurrent.TimeUnit

import scala.concurrent.duration.*
import scala.util.{Failure, Success, Try}

import ankka.flow.v1.discovery.{DiscoveryGrpc, SidecarInfo, Spec}
import ankka.flow.v1.payload.{Problem, Problems}
import com.thinkmorestupidless.ankka.flow.protocol.{DescriptorValidation, ProtocolVersion}
import io.grpc.ManagedChannel
import org.slf4j.LoggerFactory

/**
 * Discovery (contracts/protocol.md): ask the process to describe itself, until it answers, then
 * check the answer against the protocol version, the descriptor rules and the deployed descriptor.
 */
final class Discovery(channel: ManagedChannel, settings: Settings, deployed: Descriptor):

  private val log = LoggerFactory.getLogger(classOf[Discovery])

  private val info =
    SidecarInfo(ProtocolVersion.Current.toString, BuildInfo.version)

  /**
   * Blocks until the process answers (retrying with backoff from 500 ms to 10 s, forever) or
   * `stopped` returns true. `Right(spec)` when the answer is acceptable, `Left(problems)`
   * otherwise.
   */
  def run(stopped: () => Boolean): Option[Either[Vector[String], Spec]] =
    var backoff                                      = 500.millis
    var attempt                                      = 1
    var result: Option[Either[Vector[String], Spec]] = None
    while result.isEmpty && !stopped() do
      once() match
        case Success(spec) =>
          log.info(
            "discovery: the process answered on attempt {} ({} {})",
            attempt,
            spec.getSdk.name,
            spec.getSdk.version
          )
          result = Some(check(spec))
        case Failure(e) =>
          log.info(
            "discovery attempt {} at {}: {}; retrying in {}",
            attempt,
            settings.processAddress,
            e.getMessage,
            backoff
          )
          Thread.sleep(backoff.toMillis)
          backoff = (backoff * 2).min(10.seconds)
          attempt += 1
    result

  def once(): Try[Spec] =
    Try(
      DiscoveryGrpc
        .blockingStub(channel)
        .withDeadlineAfter(settings.discoveryTimeout.toMillis, TimeUnit.MILLISECONDS)
        .discover(info)
    )

  def check(spec: Spec): Either[Vector[String], Spec] =
    ProtocolVersion.compatible(ProtocolVersion.Current, spec.protocolVersion) match
      case Left(version) => Left(Vector(version))
      case Right(()) =>
        val problems = (DescriptorValidation.validate(spec) ++ deployed.compare(spec)).distinct
        if problems.isEmpty then Right(spec) else Left(problems)

  /** Tell the process why it was refused, so the problems appear in its own log. Best effort. */
  def reportError(problems: Vector[String]): Unit =
    Try(
      DiscoveryGrpc
        .blockingStub(channel)
        .withDeadlineAfter(5, TimeUnit.SECONDS)
        .reportError(Problems(problems.map(Problem(_))))
    ).failed.foreach(e => log.warn("could not report the refusal to the process: {}", e.getMessage))

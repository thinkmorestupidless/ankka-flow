package com.thinkmorestupidless.ankka.flow.protocol

/**
 * `MAJOR.MINOR`. A sidecar accepts an SDK of its own major whose minor is not later than its own,
 * and refuses anything else naming both (FR-017).
 */
final case class ProtocolVersion(major: Int, minor: Int):
  override def toString: String = s"$major.$minor"

object ProtocolVersion:

  val Current: ProtocolVersion = ProtocolVersion(1, 0)

  private val Shape = """(\d{1,9})\.(\d{1,9})""".r

  def parse(s: String): Either[String, ProtocolVersion] = s match
    case Shape(a, b) => Right(ProtocolVersion(a.toInt, b.toInt))
    case _           => Left(s"protocol version '$s' is not MAJOR.MINOR")

  def compatible(sidecar: ProtocolVersion, sdk: String): Either[String, Unit] =
    parse(sdk).flatMap { v =>
      if v.major != sidecar.major then
        Left(
          s"the SDK speaks protocol '$v' and this sidecar speaks '$sidecar'; the major versions must match"
        )
      else if v.minor > sidecar.minor then
        Left(
          s"the SDK speaks protocol '$v', later than this sidecar's '$sidecar'; upgrade the platform or pin the SDK"
        )
      else Right(())
    }

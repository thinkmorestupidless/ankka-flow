package com.thinkmorestupidless.ankka.flow.descriptor

import java.nio.charset.StandardCharsets.UTF_8
import java.security.MessageDigest

/**
 * What a port carries: a name and, when the streamlet was declared with a schema document, that
 * document's fingerprint (research R4). The name follows ankka's rule for a contract's name; the
 * fingerprint is computed exactly as ankka computes it, so a port can be checked against an ankka
 * topic's declaration. Nothing checks a record against the schema.
 */
final case class Contract(name: String, fingerprint: Option[String])

object Contract:

  /** ankka's rule for a contract's name: 2 to 100 of a topic's letters. */
  val NameRule: String = "[a-z0-9][a-z0-9._-]{0,98}[a-z0-9]"

  private val NamePattern        = NameRule.r
  private val FingerprintPattern = "sha256:[0-9a-f]{64}".r

  def validName(name: String): Boolean = NamePattern.matches(name)

  def validFingerprint(fingerprint: String): Boolean = FingerprintPattern.matches(fingerprint)

  /** `sha256:` and the lowercase hex SHA-256 of the document under RFC 8785; `Left` if not JSON. */
  def fingerprint(document: Array[Byte]): Either[String, String] =
    Json.parse(new String(document, UTF_8)).map { json =>
      val digest =
        MessageDigest.getInstance("SHA-256").digest(Canonical.render(json).getBytes(UTF_8))
      "sha256:" + digest.map(b => f"$b%02x").mkString
    }

  /**
   * Whether a writer's port and a reader's connect over one topic: equal names and, where both
   * state a fingerprint, equal fingerprints.
   */
  def connects(a: Contract, b: Contract): Boolean =
    a.name == b.name && (a.fingerprint, b.fingerprint).match
      case (Some(x), Some(y)) => x == y
      case _                  => true

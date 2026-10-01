package com.thinkmorestupidless.ankka.flow.blueprint

import ankka.flow.v1.discovery.StreamletDescriptor as Proto
import com.thinkmorestupidless.ankka.flow.protocol.Builtins

/** A port's contract as verification sees it (research R4). */
final case class SchemaDescriptor(name: String, fingerprint: String, format: String):
  override def toString: String = s"$format $name"

final case class PortDescriptor(name: String, schema: SchemaDescriptor, isOutlet: Boolean)

/**
 * A streamlet descriptor as the carried verification reads it: the protocol's descriptor (written
 * by an SDK, or shipped by the platform when `builtin`) viewed through the shape Cloudflow's
 * verification was written against.
 */
final case class StreamletDescriptor(proto: Proto, builtin: Boolean = false):
  def name: String = proto.name

  /**
   * What a blueprint writes to name this descriptor: `builtin/<name>` for a built-in, the bare name
   * otherwise, so neither kind can shadow the other.
   */
  def ref: String = if builtin then Builtins.Prefix + name else name

  val inlets: Vector[PortDescriptor]  = proto.inlets.toVector.map(port(_, isOutlet = false))
  val outlets: Vector[PortDescriptor] = proto.outlets.toVector.map(port(_, isOutlet = true))

  private def port(p: ankka.flow.v1.discovery.Port, isOutlet: Boolean) =
    val c = p.getContract
    PortDescriptor(p.name, SchemaDescriptor(c.schemaName, c.fingerprint, c.format), isOutlet)

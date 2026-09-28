package com.thinkmorestupidless.ankka.flow.blueprint

import ankka.flow.v1.discovery.StreamletDescriptor as Proto

/** A port's contract as verification sees it (research R4). */
final case class SchemaDescriptor(name: String, fingerprint: String, format: String):
  override def toString: String = s"$format $name"

final case class PortDescriptor(name: String, schema: SchemaDescriptor, isOutlet: Boolean)

/**
 * A streamlet descriptor as the carried verification reads it: the protocol's descriptor (written
 * by an SDK) viewed through the shape Cloudflow's verification was written against.
 */
final case class StreamletDescriptor(proto: Proto):
  def name: String = proto.name

  val inlets: Vector[PortDescriptor]  = proto.inlets.toVector.map(port(_, isOutlet = false))
  val outlets: Vector[PortDescriptor] = proto.outlets.toVector.map(port(_, isOutlet = true))

  private def port(p: ankka.flow.v1.discovery.Port, isOutlet: Boolean) =
    val c = p.getContract
    PortDescriptor(p.name, SchemaDescriptor(c.schemaName, c.fingerprint, c.format), isOutlet)

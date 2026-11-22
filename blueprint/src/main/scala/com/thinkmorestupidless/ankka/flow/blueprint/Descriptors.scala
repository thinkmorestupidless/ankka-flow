package com.thinkmorestupidless.ankka.flow.blueprint

import com.thinkmorestupidless.ankka.flow.descriptor.{Contract, Descriptor, Engine}

final case class PortDescriptor(name: String, contract: Contract, isOutlet: Boolean)

object PortDescriptor:

  /** A contract as a message names it: its name, and its fingerprint when it states one. */
  def show(c: Contract): String = c.fingerprint.fold(c.name)(f => s"${c.name} $f")

/**
 * A streamlet descriptor as the carried verification reads it: the descriptor an SDK wrote, viewed
 * through the shape Cloudflow's verification was written against.
 */
final case class StreamletDescriptor(descriptor: Descriptor):
  def name: String   = descriptor.streamlet.name
  def engine: Engine = descriptor.streamlet.engine

  val inlets: Vector[PortDescriptor] =
    descriptor.streamlet.inlets.map(p => PortDescriptor(p.name, p.contract, isOutlet = false))
  val outlets: Vector[PortDescriptor] =
    descriptor.streamlet.outlets.map(p => PortDescriptor(p.name, p.contract, isOutlet = true))

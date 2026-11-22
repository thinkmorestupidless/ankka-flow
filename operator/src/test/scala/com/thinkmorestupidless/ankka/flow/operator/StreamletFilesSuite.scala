package com.thinkmorestupidless.ankka.flow.operator

import com.thinkmorestupidless.ankka.flow.descriptor.{Descriptor, DescriptorJson}

import Fixtures.*

/** The files the operator renders for a streamlet, and the hash that rolls it. */
class StreamletFilesSuite extends munit.FunSuite:

  private val rendered = Rendering.render(cart(maxTotal = 250), settings, observed, "t")
  private val topics   = rendered.resolved.map(t => t.id -> t).toMap
  private val cleaner  = cart(maxTotal = 250).getSpec.streamlets.head
  private val files    = StreamletFiles.render("cart", cleaner, topics).toOption.get

  test("the descriptor file is the resource's descriptor as a version 2 document") {
    val d = DescriptorJson.read(files.descriptorJson).fold(e => fail(e), identity)
    assertEquals(d.descriptorVersion, Descriptor.Version)
    assertEquals(d.streamlet.name, "cart-cleaner")
  }

  test("streamlet.conf names every port's topic, group and client id, and no batching") {
    val c = files.streamletConf
    Seq(
      "topic = \"shop.cart-events.v1\"",
      "group = \"cart.cleaner.in\"",
      "client-id = \"cart.cleaner.out\"",
      "topic = \"cart.clean-carts\""
    ).foreach(f => assert(c.contains(f), s"$f in:\n$c"))
    assert(!c.contains("batch"), c)
  }

  test("the hash is stable and covers both files") {
    assertEquals(StreamletFiles.render("cart", cleaner, topics).toOption.get.hash, files.hash)
    assertNotEquals(files.copy(streamletConf = files.streamletConf + " ").hash, files.hash)
  }

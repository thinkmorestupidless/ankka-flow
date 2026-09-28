package com.thinkmorestupidless.ankka.flow.protocol

class ProtocolVersionSuite extends munit.FunSuite:

  private val sidecar = ProtocolVersion(1, 1)

  test("the same version is compatible") {
    assertEquals(ProtocolVersion.compatible(sidecar, "1.1"), Right(()))
  }

  test("an earlier minor within the major is accepted") {
    assertEquals(ProtocolVersion.compatible(sidecar, "1.0"), Right(()))
  }

  test("a later minor is refused naming both") {
    val Left(msg) = ProtocolVersion.compatible(sidecar, "1.2"): @unchecked
    assert(msg.contains("1.2") && msg.contains("1.1"), msg)
  }

  test("another major is refused naming both") {
    val Left(msg) = ProtocolVersion.compatible(sidecar, "2.0"): @unchecked
    assert(msg.contains("'2.0'") && msg.contains("'1.1'"), msg)
    assert(ProtocolVersion.compatible(sidecar, "0.9").isLeft)
  }

  test("a malformed version is refused") {
    assert(ProtocolVersion.compatible(sidecar, "1").isLeft)
    assert(ProtocolVersion.compatible(sidecar, "v1.0").isLeft)
    assert(ProtocolVersion.compatible(sidecar, "").isLeft)
  }

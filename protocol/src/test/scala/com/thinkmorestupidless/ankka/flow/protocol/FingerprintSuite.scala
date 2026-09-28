package com.thinkmorestupidless.ankka.flow.protocol

/** The fingerprint assertions of Cloudflow's JsonSpec that survive: codecs are no longer ours. */
class FingerprintSuite extends munit.FunSuite:

  test("fingerprint by schema name alone, so equal names connect and different names do not") {
    assertEquals(Fingerprint.Format, "json")
    assertEquals(
      Fingerprint.fingerprint("nakka.cart-events.v1"),
      Fingerprint.fingerprint("nakka.cart-events.v1")
    )
    assertNotEquals(
      Fingerprint.fingerprint("nakka.cart-events.v1"),
      Fingerprint.fingerprint("nakka.cart-events.v2")
    )
  }

  test("the algorithm is standard Base64 of SHA-256 over the UTF-8 name, padded") {
    // printf 'cart-events.v1' | openssl dgst -sha256 -binary | base64
    val fp = Fingerprint.fingerprint("cart-events.v1")
    assertEquals(fp.length, 44)
    assert(fp.endsWith("="))
    val expected = java.util.Base64.getEncoder.encodeToString(
      java.security.MessageDigest.getInstance("SHA-256").digest("cart-events.v1".getBytes("UTF-8"))
    )
    assertEquals(fp, expected)
  }

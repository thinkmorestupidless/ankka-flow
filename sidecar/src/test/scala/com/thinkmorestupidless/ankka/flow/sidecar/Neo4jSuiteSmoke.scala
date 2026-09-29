package com.thinkmorestupidless.ankka.flow.sidecar

class Neo4jSuiteSmoke extends Neo4jSuite:
  test("the container answers a query") {
    assertEquals(query("RETURN 1 AS one").head("one"), java.lang.Long.valueOf(1))
  }

package com.thinkmorestupidless.ankka.flow.operator

class StateStoreSuite extends munit.FunSuite:

  test("dir, flink-config and env are read from the Secret's data") {
    val data = Map(
      "dir"          -> "s3://flow-state/prod/",
      "flink-config" -> "s3.endpoint: http://minio.minio:9000\ns3.path-style-access=true\n",
      "env"          -> "AWS_ACCESS_KEY_ID=minio\nAWS_SECRET_ACCESS_KEY=minio123\n"
    )
    assertEquals(
      StateStore.fromData(data),
      Right(
        Some(
          StateStore(
            "s3://flow-state/prod/",
            Map("s3.endpoint" -> "http://minio.minio:9000", "s3.path-style-access" -> "true"),
            Map("AWS_ACCESS_KEY_ID" -> "minio", "AWS_SECRET_ACCESS_KEY" -> "minio123")
          )
        )
      )
    )
  }

  test("only dir is required") {
    assertEquals(
      StateStore.fromData(Map("dir" -> "file:///state")),
      Right(Some(StateStore("file:///state")))
    )
  }

  test("a Secret without dir, or with an empty one, is no state store") {
    assertEquals(StateStore.fromData(Map.empty), Right(None))
    assertEquals(StateStore.fromData(Map("dir" -> "  ", "env" -> "A=1")), Right(None))
  }

  test("properties text that does not parse is named by its key") {
    val Left(e) = StateStore.fromData(Map("dir" -> "s3://x", "env" -> "A=\\u00zz")): @unchecked
    assert(e.contains("env"), e)
  }

  test("the three directories of a streamlet at an epoch") {
    val paths = StateStore("s3://flow-state/prod/").paths("shop", "carts", "counter", 2)
    assertEquals(
      paths,
      StateStore.Paths(
        "s3://flow-state/prod/shop/carts/counter/2/checkpoints",
        "s3://flow-state/prod/shop/carts/counter/2/savepoints",
        "s3://flow-state/prod/shop/carts/counter/2/ha"
      )
    )
    assertEquals(StateStore.envSecretName("carts"), "flow-carts-flink-state")
  }

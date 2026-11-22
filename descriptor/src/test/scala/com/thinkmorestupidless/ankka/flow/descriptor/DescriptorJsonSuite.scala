package com.thinkmorestupidless.ankka.flow.descriptor

class DescriptorJsonSuite extends munit.FunSuite:

  /** The example of the descriptor's contract (specs/009-engine-streamlets/contracts). */
  private val example =
    """{
      |  "descriptor_version": "2.0",
      |  "sdk": {
      |    "name": "ankka-flow-flink",
      |    "version": "0.6.0"
      |  },
      |  "streamlet": {
      |    "config_parameters": [
      |      {
      |        "default_value": "1m",
      |        "key": "window",
      |        "type": "DURATION"
      |      }
      |    ],
      |    "engine": "flink",
      |    "flink": {
      |      "entry_class": "com.example.CartCounter",
      |      "version": "2.2"
      |    },
      |    "inlets": [
      |      {
      |        "contract": {
      |          "name": "clean-carts.v1"
      |        },
      |        "name": "in"
      |      }
      |    ],
      |    "name": "cart-counter",
      |    "outlets": [
      |      {
      |        "contract": {
      |          "fingerprint": "sha256:9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08",
      |          "name": "cart-counts.v1"
      |        },
      |        "name": "out"
      |      }
      |    ]
      |  }
      |}
      |""".stripMargin

  test("the contract's example reads and is written back byte for byte"):
    val d = DescriptorJson.read(example).fold(e => fail(e), identity)
    assertEquals(d.streamlet.engine, Engine.Flink)
    assertEquals(d.streamlet.flink, Some(FlinkJob("com.example.CartCounter", "2.2")))
    assertEquals(
      d.streamlet.outlets.head.contract.fingerprint,
      Some("sha256:9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08")
    )
    assertEquals(DescriptorJson.write(d), example)

  private val pekko = Descriptor(
    "2.0",
    Some(SdkInfo("fixture", "0.0.0")),
    StreamletDescriptor(
      name = "sorting",
      description = "",
      engine = Engine.PekkoStreams,
      inlets = Vector(Port("b", Contract("x.v1", None)), Port("a", Contract("x.v1", None))),
      outlets = Vector.empty,
      parameters = Vector(
        Parameter("z", "", ParameterType.String, Some("")),
        Parameter("m", "Em.", ParameterType.Integer, Some("1"))
      ),
      flink = None
    )
  )

  test("writing sorts ports and parameters and omits defaults"):
    val text = DescriptorJson.write(pekko)
    assert(text.indexOf("\"a\"") < text.indexOf("\"b\""), text)
    assert(text.indexOf("\"m\"") < text.indexOf("\"z\""), text)
    assert(!text.contains("STRING"), text)
    assert(!text.contains("\"description\": \"\""), text)
    assert(!text.contains("default_value\": \"\""), text)
    assert(!text.contains("outlets"), text)
    assert(!text.contains("flink"), text)
    assert(text.contains("\"engine\": \"pekko-streams\""), text)

  test("reading tolerates whitespace and key order"):
    val compact = Json.compact(Json.parse(example).toOption.get)
    assertEquals(DescriptorJson.read(compact).map(DescriptorJson.write), Right(example))

  test("a value of the wrong JSON type is a problem naming the field"):
    val bad = example.replace("\"name\": \"cart-counter\"", "\"name\": 3")
    assertEquals(DescriptorJson.read(bad).left.map(_.contains("'name'")), Left(true))

  test("an unknown parameter type is a problem naming it"):
    val bad = example.replace("\"DURATION\"", "\"COLOUR\"")
    assertEquals(DescriptorJson.read(bad), Left("unknown config parameter type 'COLOUR'"))

package com.thinkmorestupidless.ankka.flow.descriptor

class DescriptorValidationSuite extends munit.FunSuite:

  private def doc(streamlet: String, version: String = "\"descriptor_version\": \"2.0\",") =
    s"""{ $version "streamlet": { "name": "s", $streamlet
       |  "inlets": [{"name": "in", "contract": {"name": "x.v1"}}] } }""".stripMargin

  private def problems(text: String): Vector[String] =
    DescriptorValidation.check(text).left.getOrElse(Vector.empty)

  // ── The four refusals of the descriptor's contract ──────────────────────

  test("no engine is refused: a descriptor must name an engine"):
    assertEquals(
      problems(doc("")),
      Vector("a descriptor must name an engine (pekko-streams, flink)")
    )

  test("version one's descriptor, with a protocol_version, is refused the same way"):
    val v1 = doc("", "\"protocol_version\": \"1.0\",")
    assertEquals(problems(v1), Vector("a descriptor must name an engine (pekko-streams, flink)"))
    val both = doc("\"engine\": \"pekko-streams\",", "\"protocol_version\": \"1.0\",")
    assertEquals(problems(both), Vector("a descriptor must name an engine (pekko-streams, flink)"))

  test("an unknown engine is refused, naming it and the ones flow knows"):
    assertEquals(
      problems(doc("\"engine\": \"spark\",")),
      Vector("engine 'spark' is not one flow knows (pekko-streams, flink)")
    )

  test("a flink streamlet without its flink section, and another with one, are refused"):
    val message = "a flink streamlet's descriptor has a flink section, and no other has"
    assertEquals(problems(doc("\"engine\": \"flink\",")), Vector(message))
    val pekkoWithFlink =
      doc(
        "\"engine\": \"pekko-streams\", \"flink\": {\"entry_class\": \"a.B\", \"version\": \"2.2\"},"
      )
    assertEquals(problems(pekkoWithFlink), Vector(message))

  test("a major version other than 2 is refused"):
    assertEquals(
      problems(doc("\"engine\": \"flink\",", "\"descriptor_version\": \"3.0\",")),
      Vector("descriptor version 3.0 is not one this flow reads (2.x)")
    )
    assertEquals(
      problems(doc("\"engine\": \"pekko-streams\",", "\"descriptor_version\": \"two\",")),
      Vector("descriptor version two is not one this flow reads (2.x)")
    )

  test("a well-formed descriptor of each engine is accepted"):
    assert(DescriptorValidation.check(doc("\"engine\": \"pekko-streams\",")).isRight)
    val flink =
      doc("\"engine\": \"flink\", \"flink\": {\"entry_class\": \"a.B\", \"version\": \"2.2\"},")
    assert(DescriptorValidation.check(flink).isRight, problems(flink))
    assert(
      DescriptorValidation
        .check(doc("\"engine\": \"pekko-streams\",", "\"descriptor_version\": \"2.7\","))
        .isRight
    )

  test("a flink section names an entry class and a version the operator can run"):
    val noClass =
      doc("\"engine\": \"flink\", \"flink\": {\"entry_class\": \"\", \"version\": \"2.2\"},")
    assert(problems(noClass).exists(_.contains("entry class")), problems(noClass))
    val old =
      doc("\"engine\": \"flink\", \"flink\": {\"entry_class\": \"a.B\", \"version\": \"1.20\"},")
    assertEquals(problems(old), Vector("flink version 1.20 is not one this flow runs (2.2)"))

  // ── Names, ports, contracts and parameters, as version one had them ─────

  private val base = StreamletDescriptor(
    name = "minimal",
    description = "",
    engine = Engine.PekkoStreams,
    inlets = Vector(Port("in", Contract("minimal.v1", None))),
    outlets = Vector(Port("out", Contract("minimal.v1", None))),
    parameters = Vector.empty,
    flink = None
  )
  private def of(d: StreamletDescriptor) = DescriptorValidation.validateStreamlet(d)

  test("streamlet name"):
    assert(of(base.copy(name = "Bad_Name")).exists(_.contains("streamlet name")))
    assert(of(base.copy(name = "-lead")).nonEmpty)
    assert(of(base.copy(name = "a" * 64)).nonEmpty)
    assertEquals(of(base), Vector.empty)

  test("a port declared twice across inlets and outlets"):
    val dup = base.copy(outlets = base.outlets :+ base.inlets.head)
    assert(of(dup).exists(_.contains("port 'in' is declared 2 times")), of(dup).toString)

  test("port names"):
    val bad = base.copy(inlets = Vector(Port("In", Contract("minimal.v1", None))))
    assert(of(bad).exists(_.contains("inlet name 'In'")), of(bad).toString)

  test("a contract's name follows ankka's rule and a fingerprint is sha256 hex"):
    val name = base.copy(inlets = Vector(Port("in", Contract("Minimal", None))))
    assert(of(name).exists(_.contains("contract name 'Minimal'")), of(name).toString)
    val fp = base.copy(inlets = Vector(Port("in", Contract("minimal.v1", Some("abc")))))
    assert(of(fp).exists(_.contains("fingerprint 'abc'")), of(fp).toString)

  test("parameter keys, duplicates and defaults that do not parse as their type"):
    val badKey = base.copy(parameters = Vector(Parameter("Bad", "", ParameterType.String, None)))
    assert(of(badKey).exists(_.contains("parameter key 'Bad'")))
    val p    = Parameter("a-string", "", ParameterType.String, Some("x"))
    val dupe = base.copy(parameters = Vector(p, p))
    assert(of(dupe).exists(_.contains("parameter 'a-string' is declared 2 times")))
    Seq(
      ParameterType.Integer    -> "1.5",
      ParameterType.Double     -> "abc",
      ParameterType.Boolean    -> "yes",
      ParameterType.Duration   -> "soon",
      ParameterType.MemorySize -> "big"
    ).foreach { (t, v) =>
      val d = base.copy(parameters = Vector(Parameter("x", "", t, Some(v))))
      assert(of(d).exists(_.contains(s"'$v' is not a")), s"$t $v: ${of(d)}")
    }

  test("parseValue renders each type"):
    assertEquals(DescriptorValidation.parseValue(ParameterType.Integer, " 7 "), Right(Json.Num(7)))
    assertEquals(
      DescriptorValidation.parseValue(ParameterType.Boolean, "false"),
      Right(Json.Bool(false))
    )
    assertEquals(
      DescriptorValidation.parseValue(ParameterType.Duration, "1m"),
      Right(Json.Str("1m"))
    )
    assertEquals(DescriptorValidation.typeName(ParameterType.MemorySize), "memory size")

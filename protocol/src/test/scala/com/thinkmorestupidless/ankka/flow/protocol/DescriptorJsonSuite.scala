package com.thinkmorestupidless.ankka.flow.protocol

import java.nio.file.Files

import ankka.flow.v1.discovery.*

class DescriptorJsonSuite extends munit.FunSuite:

  private def fixtureFiles =
    Files
      .list(Fixtures.dir.resolve("descriptors"))
      .toArray
      .toVector
      .map(_.asInstanceOf[java.nio.file.Path])

  test("every fixture parses and re-writes byte for byte") {
    val files = fixtureFiles
    assert(files.nonEmpty)
    files.foreach { f =>
      val text = Fixtures.read(f)
      val spec = DescriptorJson.read(text).fold(e => fail(s"$f: $e"), identity)
      assertEquals(DescriptorJson.write(spec), text, f.toString)
    }
  }

  test("writing sorts ports and parameters whatever order they were declared in") {
    val many = Fixtures.byName("many-ports")
    val text = DescriptorJson.write(many)
    assert(text.indexOf("\"in-a\"") < text.indexOf("\"in-b\""))
    assert(text.indexOf("\"out-1\"") < text.indexOf("\"out-5\""))
    val every = DescriptorJson.write(Fixtures.byName("every-type"))
    assert(every.indexOf("\"a-boolean\"") < every.indexOf("\"required\""))
  }

  test("defaults are omitted: STRING type, empty description, no default value") {
    val text = DescriptorJson.write(Fixtures.byName("every-type"))
    assert(!text.contains("\"STRING\""), text)
    assert(!DescriptorJson.write(Fixtures.byName("minimal")).contains("description"))
  }

  test("the canonical form escapes like Python's json.dumps(ensure_ascii=False)") {
    assertEquals(
      Json.canonical(
        Json.Obj(Vector("b" -> Json.Str("q\"\\\n\u0001é✓"), "a" -> Json.Arr(Vector.empty)))
      ),
      "{\n  \"a\": [],\n  \"b\": \"q\\\"\\\\\\n\\u0001é✓\"\n}\n"
    )
  }

  test("parsing tolerates whitespace and key order, as Jackson's compact output") {
    val compact = Json.compact(DescriptorJson.toJson(Fixtures.byName("cart-router")))
    assertEquals(
      DescriptorJson.read(compact).map(DescriptorJson.write),
      Right(DescriptorJson.write(Fixtures.byName("cart-router")))
    )
  }

  private def streamlet(name: String)          = Fixtures.byName(name).getStreamlet
  private def problems(d: StreamletDescriptor) = DescriptorValidation.validateStreamlet(d)

  test("validation: streamlet name") {
    assert(problems(streamlet("minimal").withName("Bad_Name")).exists(_.contains("streamlet name")))
    assert(problems(streamlet("minimal").withName("-lead")).nonEmpty)
    assert(problems(streamlet("minimal").withName("a" * 64)).nonEmpty)
  }

  test("validation: a port declared twice across inlets and outlets") {
    val d   = streamlet("minimal")
    val dup = d.withOutlets(d.outlets :+ d.inlets.head)
    assert(
      problems(dup).exists(_.contains("port 'in' is declared 2 times")),
      problems(dup).toString
    )
  }

  test("validation: port names, format, and a hand-edited fingerprint") {
    val d  = streamlet("minimal")
    val in = d.inlets.head
    assert(problems(d.withInlets(Seq(in.withName("In")))).exists(_.contains("inlet name 'In'")))
    val avro = in.withContract(in.getContract.withFormat("avro"))
    assert(problems(d.withInlets(Seq(avro))).exists(_.contains("format 'avro'")))
    val edited = in.withContract(in.getContract.withSchemaName("minimal.v2"))
    assert(problems(d.withInlets(Seq(edited))).exists(_.contains("fingerprint does not match")))
  }

  test("validation: parameter keys, duplicates and defaults that do not parse as their type") {
    val d      = streamlet("every-type")
    val badKey = d.withConfigParameters(Seq(ConfigParameter("Bad", "", ConfigType.STRING, "")))
    assert(problems(badKey).exists(_.contains("parameter key 'Bad'")))
    val dupe = d.withConfigParameters(d.configParameters :+ d.configParameters.head)
    assert(problems(dupe).exists(_.contains("parameter 'a-string' is declared 2 times")))
    Seq(
      ConfigType.INTEGER     -> "1.5",
      ConfigType.DOUBLE      -> "abc",
      ConfigType.BOOLEAN     -> "yes",
      ConfigType.DURATION    -> "soon",
      ConfigType.MEMORY_SIZE -> "big"
    ).foreach { (t, v) =>
      val p = d.withConfigParameters(Seq(ConfigParameter("x", "", t, v)))
      assert(problems(p).exists(_.contains(s"'$v' is not a")), s"$t $v: ${problems(p)}")
    }
  }

  test("validation: protocol version shape") {
    assert(
      DescriptorValidation.validate(Fixtures.byName("minimal").withProtocolVersion("one")).nonEmpty
    )
  }

  test("compare names a changed fingerprint, a missing outlet and a changed default") {
    val deployed = streamlet("cart-router")
    val valid    = deployed.outlets.find(_.name == "valid").get
    val changed = deployed
      .withOutlets(
        Seq(
          valid.withContract(
            valid.getContract
              .withSchemaName("cart-events.v2")
              .withFingerprint(Fingerprint.fingerprint("cart-events.v2"))
          )
        )
      )
      .withConfigParameters(deployed.configParameters.map(_.withDefaultValue("250")))
    val diffs = DescriptorValidation.compare(deployed, changed)
    assert(
      diffs.exists(_.contains("outlet 'review' is deployed but the process does not declare it")),
      diffs.toString
    )
    assert(
      diffs.exists(d => d.contains("outlet 'valid' contract") && d.contains("cart-events.v2")),
      diffs.toString
    )
    assert(
      diffs.exists(
        _.contains("parameter 'review-threshold' default: deployed '100', process declares '250'")
      ),
      diffs.toString
    )
    assertEquals(DescriptorValidation.compare(deployed, deployed), Vector.empty)
  }

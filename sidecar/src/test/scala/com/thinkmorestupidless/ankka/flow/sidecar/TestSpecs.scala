package com.thinkmorestupidless.ankka.flow.sidecar

import java.nio.file.{Files, Path, Paths}

import ankka.flow.v1.discovery.Spec
import ankka.flow.v1.payload.{Header, Record}
import ankka.flow.v1.streamlet.InputRecord
import com.google.protobuf.ByteString
import com.thinkmorestupidless.ankka.flow.protocol.DescriptorJson

object TestSpecs:

  val repoRoot: Path =
    Paths.get(sys.props.getOrElse("flow.repo.root", ".")).toAbsolutePath.normalize

  def fixture(name: String): Spec =
    val text = new String(
      Files.readAllBytes(repoRoot.resolve(s"protocol/fixtures/descriptors/$name.json")),
      "UTF-8"
    )
    DescriptorJson.read(text).fold(e => throw new IllegalStateException(e), identity)

  def record(key: String, value: String = "{}", headers: Seq[(String, Array[Byte])] = Nil): Record =
    Record(
      key = Some(ByteString.copyFromUtf8(key)),
      headers = headers.map((k, v) => Header(k, ByteString.copyFrom(v))),
      value = ByteString.copyFromUtf8(value)
    )

  def input(
      offset: Long,
      key: String,
      value: String = "{}",
      headers: Seq[(String, Array[Byte])] = Nil
  ) =
    InputRecord(offset, 0L, Some(record(key, value, headers)))

  /** A record with exactly this key (or none) and this value (empty: a record with no value). */
  def keyed(offset: Long, key: Option[String], value: String): InputRecord =
    InputRecord(
      offset,
      0L,
      Some(Record(key = key.map(ByteString.copyFromUtf8), value = ByteString.copyFromUtf8(value)))
    )

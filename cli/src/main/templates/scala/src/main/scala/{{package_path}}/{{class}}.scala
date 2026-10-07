package {{package}}

import java.nio.charset.StandardCharsets.UTF_8

import com.thinkmorestupidless.ankka.flow.protocol.Json
import com.thinkmorestupidless.ankka.flow.sdk.*

/** Adds a greeting to each JSON object it reads, keeping the record's key and headers. */
final class {{class}} extends Streamlet("{{name}}", "Adds a greeting to each JSON object it reads."):
  val in  = inlet("in", schemaName = "{{name}}.v1")
  val out = outlet("out", schemaName = "{{name}}.v1")
  val greeting = parameter.string(
    "greeting",
    default = "hello, ankka-flow",
    description = "The greeting added to each record."
  )

  def process(batch: Batch): Iterable[Emit] =
    val text = config(greeting)
    batch.records.map { record =>
      // The SDK decodes nothing; reading the value as JSON is this streamlet's choice. A value
      // that is not a JSON object fails the batch, and the sidecar delivers it again.
      val fields = Json.parse(record.valueString) match
        case Right(Json.Obj(fs)) => fs
        case _                   => throw new IllegalArgumentException(s"not a JSON object: ${record.valueString}")
      val greeted = Json.Obj(fields.filterNot(_._1 == "greeting") :+ ("greeting" -> Json.Str(text)))
      out.emit(record.copy(value = Json.compact(greeted).getBytes(UTF_8))) // same key, same headers
    }

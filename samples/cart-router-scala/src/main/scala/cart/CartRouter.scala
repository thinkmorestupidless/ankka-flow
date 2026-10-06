package cart

// docs:start router
import com.thinkmorestupidless.ankka.flow.protocol.Json
import com.thinkmorestupidless.ankka.flow.sdk.*

final class CartRouter
    extends Streamlet("cart-router", "Routes cart events to the valid or review outlet."):
  val in     = inlet("in", schemaName = "cart-events.v1")
  val valid  = outlet("valid", schemaName = "cart-events.v1")
  val review = outlet("review", schemaName = "cart-events.v1")
  val threshold = parameter.integer(
    "review-threshold",
    default = 100,
    description = "Carts with a total above this go to the review outlet."
  )

  def process(batch: Batch): Iterable[Emit] =
    val limit = config(threshold)
    batch.records.map { record =>
      // The SDK decodes nothing; this is the router's choice. A value that is not a cart event
      // fails the batch, as the Python router's does.
      val event =
        Json.parse(record.valueString).fold(e => throw new IllegalArgumentException(e), identity)
      val total = event.field("total") match
        case Some(Json.Num(n)) => n
        case _ => throw new IllegalArgumentException(s"no total in ${record.valueString}")
      val outlet = if total > limit then review else valid
      outlet.emit(record) // same key, same headers, same bytes
    }
// docs:end router

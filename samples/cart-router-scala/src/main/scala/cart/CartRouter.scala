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
      // The SDK decodes nothing; reading the total is the router's choice.
      val total = Json.parse(record.valueString).toOption.flatMap(_.field("total"))
      val outlet = total match
        case Some(Json.Num(n)) if n > limit => review
        case _                              => valid
      outlet.emit(record) // same key, same headers, same bytes
    }
// docs:end router

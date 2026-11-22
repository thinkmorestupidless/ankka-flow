package cart.cleaner

// docs:start cleaner
import com.github.plokhotnyuk.jsoniter_scala.core.{JsonValueCodec, readFromArray}
import com.github.plokhotnyuk.jsoniter_scala.macros.JsonCodecMaker
import com.thinkmorestupidless.ankka.flow.pekko.*

import scala.util.Try

/** What the shop publishes: something that happened to a cart. */
final case class CartEvent(cartId: String, seq: Long, total: Long, at: Long)

/** What the cleaner writes: an event that decoded and is within the maximum, keyed by its cart. */
final case class CleanCart(cartId: String, seq: Long, total: Long, at: Long)

object CleanCart:
  given JsonValueCodec[CleanCart] = JsonCodecMaker.make

object CartEvent:
  given JsonValueCodec[CartEvent] = JsonCodecMaker.make

  /** The event, when the bytes are one. */
  def parse(bytes: Array[Byte]): Option[CartEvent] = Try(readFromArray[CartEvent](bytes)).toOption

object Cleaner
    extends PekkoStreamlet(
      "cart-cleaner",
      "Drops cart events that do not decode or whose total is above the maximum, and writes the rest clean."
    ):
  // The inlet reads bytes: a record that is not a cart event is the cleaner's to drop, where a
  // typed codec would fail the stream on it.
  val in  = inlet("in", Contract("cart-events.v1", schema = "cart-events.v1.json"), Codec.bytes)
  val out = outlet("out", Contract("clean-carts.v1"), Codec.json[CleanCart])
  val maxTotal =
    intParameter(
      "max-total",
      default = 10000,
      description = "Events whose total is above this are dropped."
    )

  def logic(ctx: Context): Logic =
    val max = ctx(maxTotal)
    from(in) {
      _.map(r =>
        CartEvent.parse(r.value).filter(_.total <= max).map { e =>
          out(e.cartId, CleanCart(e.cartId, e.seq, e.total, e.at))
        }
      )
    }
// docs:end cleaner

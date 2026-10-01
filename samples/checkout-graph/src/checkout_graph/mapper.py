"""Maps the shopping cart's checkout notices to graph deltas for the built-in Neo4j merge sink."""

# docs:start mapper
import logging
from collections.abc import Iterable
from datetime import UTC, datetime

from ankka_flow import Batch, Emit, GraphDeltaOutlet, JsonInlet, Streamlet, json

log = logging.getLogger(__name__)


class CheckoutGraph(Streamlet):
    name = "checkout-graph"
    description = "Maps checkout notices to graph deltas: a cart, a checkout, and the edge between them."
    notices = JsonInlet("in", schema_name="ankka.checkout-notice.v1")
    deltas = GraphDeltaOutlet("deltas")

    def process(self, batch: Batch) -> Iterable[Emit]:
        for record in batch:
            try:
                notice = json.loads(record.value)
                cart, at = str(notice["cartId"]), int(notice["at"])
            except (ValueError, KeyError, TypeError):
                log.warning("skipping a record at offset %d that is not a checkout notice", record.offset)
                continue
            cart_id, checkout_id = f"cart:{cart}", f"checkout:{cart}:{at}"
            checked_out_at = datetime.fromtimestamp(at / 1000, UTC).isoformat(timespec="milliseconds")
            # Each delta is the element's whole state, versioned by the notice's time. The outlet
            # keys each record by its element (`node:<id>`, `edge:<id>`), so every delta for one
            # element is applied in order and a compacted topic keeps the latest of each.
            yield self.deltas.node(record, id=cart_id, version=at, labels=["Cart"], properties={"cartId": cart})
            yield self.deltas.node(
                record,
                id=checkout_id,
                version=at,
                labels=["Checkout"],
                properties={"cartId": cart, "checkedOutAt": checked_out_at},
            )
            yield self.deltas.edge(
                record,
                id=f"checked-out:{cart}:{at}",
                version=at,
                type="CHECKED_OUT",
                from_id=cart_id,
                to_id=checkout_id,
            )
    # docs:end mapper

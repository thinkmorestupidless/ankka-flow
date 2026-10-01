"""Maps the shopping cart's checkout notices to graph deltas for the built-in Neo4j merge sink."""

# docs:start mapper
import logging
from collections.abc import Iterable
from datetime import UTC, datetime

from ankka_flow import Batch, Emit, JsonInlet, JsonOutlet, Streamlet, json

log = logging.getLogger(__name__)


class CheckoutGraph(Streamlet):
    name = "checkout-graph"
    description = "Maps checkout notices to graph deltas: a cart, a checkout, and the edge between them."
    notices = JsonInlet("in", schema_name="ankka.checkout-notice.v1")
    deltas = JsonOutlet("deltas", schema_name="ankka.graph-delta.v1")

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
            # Each delta is the element's whole state, versioned by the notice's time, and keyed by
            # the element's id so every delta for one element is applied in order.
            for delta in (
                {"kind": "node", "id": cart_id, "version": at, "labels": ["Cart"], "properties": {"cartId": cart}},
                {
                    "kind": "node",
                    "id": checkout_id,
                    "version": at,
                    "labels": ["Checkout"],
                    "properties": {"cartId": cart, "checkedOutAt": checked_out_at},
                },
                {
                    "kind": "edge",
                    "id": f"checked-out:{cart}:{at}",
                    "version": at,
                    "type": "CHECKED_OUT",
                    "from": cart_id,
                    "to": checkout_id,
                    "properties": {},
                },
            ):
                yield self.deltas.emit(record, value=json.dumps(delta), key=delta["id"].encode())
    # docs:end mapper

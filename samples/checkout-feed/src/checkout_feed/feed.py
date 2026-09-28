"""Reads the checkout notices an ankka service publishes and writes a feed of checkouts."""

# docs:start feed
import logging
from collections.abc import Iterable
from datetime import UTC, datetime

from ankka_flow import Batch, Emit, JsonInlet, JsonOutlet, Streamlet, json

log = logging.getLogger(__name__)


class CheckoutFeed(Streamlet):
    name = "checkout-feed"
    description = "Turns the shopping cart's checkout notices into a feed of checkouts."
    notices = JsonInlet("in", schema_name="ankka.checkout-notice.v1")
    checkouts = JsonOutlet("checkouts", schema_name="checkouts.v1")

    def process(self, batch: Batch) -> Iterable[Emit]:
        for record in batch:
            # ankka publishes the notice as a plain JSON body, {"cartId": ..., "at": <epoch ms>},
            # keyed by the cart's id, with the CloudEvents attributes as headers.
            try:
                notice = json.loads(record.value)
                cart, at = str(notice["cartId"]), int(notice["at"])
            except (ValueError, KeyError, TypeError):
                # Not a checkout notice. Acknowledging without emitting skips it; failing the batch
                # would stall the partition on a record that can never succeed.
                log.warning("skipping a record at offset %d that is not a checkout notice", record.offset)
                continue
            checkout = {
                "cartId": cart,
                "checkedOutAt": datetime.fromtimestamp(at / 1000, UTC).isoformat(timespec="milliseconds"),
            }
            # Same key and headers, so each cart's checkouts stay in order and keep ankka's ce-id.
            yield self.checkouts.emit(record, value=json.dumps(checkout))
    # docs:end feed

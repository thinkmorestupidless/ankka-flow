# Read an ankka service's topic

> Build a pipeline on the messages an ankka service publishes — give the service a broker in its descriptor, declare its topic unmanaged in the blueprint, and decode ankka's CloudEvents in a streamlet.

Source: https://flow.ankka.cloud/build/ankka-topics/
An ankka service publishes to Kafka from a consumer that produces: it turns the service's own events
into messages for the outside world and sends them to a topic. A pipeline reads that topic like any
other input. The topic stays the service's: the pipeline declares it **unmanaged**, so the operator
never creates, alters or deletes it, and nothing in the pipeline may produce to it.

This page builds the pipeline in
[`samples/checkout-feed`](https://github.com/thinkmorestupidless/ankka-flow/blob/main/samples/checkout-feed):
ankka's shopping cart sample publishes a notice to `cart-checkouts` whenever a cart is checked out, and
a streamlet turns each notice into an entry in a feed of checkouts.

```text
shopping-cart (ankka) ──► cart-checkouts ──► feed ──► checkouts.checkouts
```

## Give the service a broker

ankka provides no broker of its own. A service reaches one through `ANKKA_KAFKA_BOOTSTRAP_SERVERS` in
its descriptor's `env`: a process-hosted service's sidecar reads it, and a Scala service reads it when
it registers `ProjectionRuntime.fromEnv()`. On a cluster where ankka-flow's development Kafka runs in
the `kafka` namespace, the shopping cart's descriptor is:

```json
{
  "name": "shopping-cart",
  "service": {
    "image": "sample-shopping-cart:latest",
    "env": [
      { "name": "ANKKA_KAFKA_BOOTSTRAP_SERVERS", "value": "kafka.kafka.svc:9092" }
    ]
  }
}
```

```bash
ankka services apply -f shopping-cart.json --project checkout
```

The service and the pipeline must use the same Kafka. Here both use the cluster's development Kafka,
which is also what the `default` Kafka cluster Secret of
[Install the platform](../deploy/install.md) points at.

## What ankka publishes

ankka frames every message as a CloudEvent in binary mode: the body is the message as plain JSON, and
the CloudEvents attributes travel as Kafka headers. The record key is the source entity's id, so every
message about one cart lands on one partition, in order. A checkout notice looks like this:

```text
key      cart-1
headers  ce-subject: cart-1, ce-specversion: 1.0, ce-id: 582b15a1-…, ce-type: message,
         content-type: application/json
value    {"cartId":"cart-1","at":1790627790360}
```

## Decode it in a streamlet

The streamlet decodes the body itself: the sidecar never decodes a record. It keeps the key, so each
cart's checkouts stay in order downstream, and the headers, so the entry carries ankka's `ce-id`. A
record that is not a checkout notice is skipped by acknowledging it without emitting, rather than
failing the batch, which would stall the partition on a record that can never succeed.

```python
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
```

The inlet's schema name, `ankka.checkout-notice.v1`, is a label this pipeline chooses: ankka does not
publish contracts, and the contract check only compares the ports of one blueprint. Name it after the
message so a second streamlet reading the same topic declares the same contract.

## Declare the topic unmanaged

```hocon
blueprint {
  name = checkouts
  streamlets {
    feed = checkout-feed
  }
  topics {
    # Published by the ankka shopping cart's CheckoutNotifier. The platform only reads it: it never
    # creates, alters or deletes it, and nothing in this pipeline may produce to it.
    cart-checkouts {
      managed    = false
      topic.name = "cart-checkouts"
      cluster    = default
      consumers  = [feed.in]
      consumer-config { auto.offset.reset = earliest }
    }
    checkouts {
      producers  = [feed.checkouts]
      partitions = 3
      replicas   = 1
    }
  }
}
```

`managed = false` and `topic.name` say the topic already exists under that name and belongs to
something else. An unmanaged topic must name its brokers or a Kafka cluster; `cluster = default` uses
the `kafka-cluster-default` Secret in the operator's namespace. `auto.offset.reset = earliest` makes a
pipeline deployed after the service read the notices already published.

## Deploy and watch

```bash
flow generate samples/checkout-feed/blueprint.conf --descriptors samples/checkout-feed/flow \
  --image feed=sample-checkout-feed:latest -n shop | kubectl apply -f -
kubectl -n shop get aflow checkouts                  # Ready
curl -s --cacert ~/.ankka/local-ca.crt -X POST \
  https://shopping-cart-checkout.127.0.0.1.sslip.io:8443/carts/cart-1/items \
  -H 'content-type: application/json' -d '{"productId":"p1","name":"Pen","quantity":2}'
curl -s --cacert ~/.ankka/local-ca.crt -X POST \
  https://shopping-cart-checkout.127.0.0.1.sslip.io:8443/carts/cart-1/checkout
kubectl -n kafka exec kafka-0 -- /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic checkouts.checkouts --from-beginning --property print.key=true
```

The feed's entry arrives under the cart's key:

```text
cart-1	{"cartId":"cart-1","checkedOutAt":"2026-09-28T20:36:30.360+00:00"}
```

The events on the resource show `TopicCreated` for `checkouts.checkouts` and nothing for
`cart-checkouts`: the operator left the service's topic alone. If the service's topic does not exist
yet, the resource reports `TopicMissing` and the streamlet stays not ready until the service first
publishes, or until the topic is created.

# The checkout feed

A pipeline beside a running ankka service: ankka's shopping cart sample publishes a checkout notice to
`cart-checkouts` whenever a cart is checked out, and this Python streamlet turns each notice into an
entry in a feed of checkouts. The walkthrough is
[Read an ankka service's topic](https://flow.ankka.cloud/build/ankka-topics/).

| file | what |
|---|---|
| `src/checkout_feed/feed.py` | the streamlet |
| `flow/descriptor.json` | written by `uv run descriptor`; committed; checked by `uv run descriptor --check` |
| `blueprint.conf` | the pipeline: ankka's topic, unmanaged, and one managed outlet topic |

```bash
uv sync
uv run pytest -q                                  # the harness: no Kafka, no sidecar
uv run descriptor --check
docker build -f samples/checkout-feed/Dockerfile -t sample-checkout-feed .   # from the repository root
```

## On a kind cluster beside ankka

ankka (`just up` in its repository, with the shopping cart deployed in project `checkout`) and
ankka-flow (`just up` here) in one kind cluster:

```bash
kind load docker-image --name ankka sample-checkout-feed
kubectl -n kafka exec kafka-0 -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic cart-checkouts --partitions 3
ankka services apply -f shopping-cart.json --project checkout      # ANKKA_KAFKA_BOOTSTRAP_SERVERS=kafka.kafka.svc:9092
flow generate blueprint.conf --descriptors flow --image feed=sample-checkout-feed:latest -n shop | kubectl apply -f -
```

Check a cart out through the shopping cart's HTTP API and read `checkouts.checkouts`.

### Last run on kind (2026-09-28)

ankka at `main` plus `ProjectionRuntime.fromEnv`, the shopping cart redeployed with
`ANKKA_KAFKA_BOOTSTRAP_SERVERS=kafka.kafka.svc:9092`: `CheckoutNotifier` registered and published
three checkouts to `cart-checkouts`; the pipeline was `Ready` 15 s after `kubectl apply`, and all three
arrived on `checkouts.checkouts` under their cart ids with ankka's CloudEvents headers intact. The only
topic event was `TopicCreated` for `checkouts.checkouts`. The run found that the sidecar's keepalive
pings provoked `GOAWAY too_many_pings` from the Python server on a quiet stream; the sidecar no longer
pings, and both streamlets then idled with one conversation each.

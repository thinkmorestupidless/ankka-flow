# The cart router

The spec's first story in one directory: a Python streamlet with one JSON inlet of cart events keyed
by cart id, two outlets, and a function that routes each event by its total.

| file | what |
|---|---|
| `src/cart_router/router.py` | the streamlet |
| `flow/descriptor.json` | written by `uv run descriptor`; committed; checked by `uv run descriptor --check` |
| `flow/streamlet.conf` | the sidecar's configuration for the compose network |
| `blueprint.conf` | the pipeline: an unmanaged input topic and two managed outlet topics |
| `docker-compose.yml` | Kafka and the sidecar; the router runs on the host |
| `produce.py` | fifty CloudEvents over ten carts, with a plain Kafka client; creates the topics |
| `verify.py` | reads everything back and checks the story's promises |

## On a laptop

```bash
(cd ../.. && sbt sidecar/docker:publishLocal)     # the sidecar image, once
uv sync
uv run pytest -q                                  # the harness: no Kafka, no sidecar
uv run descriptor --check
docker compose up -d                              # the sidecar waits for the router
uv run python -m cart_router.main &               # the router on 127.0.0.1:9010
uv run python produce.py
kill %1; sleep 1; uv run python -m cart_router.main &   # kill it mid-stream and start it again
uv run python verify.py                           # exit 0: nothing lost, each cart in order, headers intact
```

`verify.py` reports repeats separately: delivery is at least once, so an event whose emits were
written but whose offsets were not yet committed when the router died is delivered again. A repeat
never reorders a cart.

## The image

```bash
docker build -f samples/cart-router/Dockerfile -t sample-cart-router .   # from the repository root
```

## On a kind cluster

`flow` is not installed by anything: build it with `just cli` from the repository root, which
prints the line that puts it on your PATH. The commands below call it by its staged path instead.

```bash
just up                                            # from the repository root: kind, the CRD, the operator, a dev Kafka
kubectl create namespace shop
kubectl -n kafka exec kafka-0 -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic shop.cart-events.v1 --partitions 3      # the unmanaged input, owned by "someone else"
../../cli/target/universal/stage/bin/flow generate blueprint.conf --descriptors flow \
  --conf k8s/in-cluster.conf --image router=sample-cart-router:latest -n shop | kubectl apply -f -
kubectl -n shop get aflow cart -w                  # Pending, then Ready
kubectl -n shop get events --field-selector involvedObject.kind=AnkkaFlow
kubectl -n shop port-forward deploy/flow-cart-router 2050 & curl -s localhost:2050/metrics | grep records_lag
```

`k8s/in-cluster.conf` points the unmanaged input at the in-cluster Kafka; the blueprint's
`kafka:9092` is the compose network's name. `k8s/kafka-cluster-shop.yaml` shows a named cluster.

### Last run on kind (2026-09-28)

`just up`, then the commands above: the pipeline was `Ready` 8 s after `kubectl apply`; 50 events
produced with the console producer arrived 25 on each outlet; `TopicCreated` events for both managed
topics; lag scraped from the router pod under `client_id="cart.router.in"`. The pod listens on
exactly two sockets: `127.0.0.1:9010` (the router, loopback only) and `:2050` (the sidecar's
metrics). `just down` removes the cluster.

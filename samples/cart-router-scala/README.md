# The cart router, in Scala

The Python cart router's streamlet, written with the Scala SDK: one JSON inlet of cart events keyed
by cart id, two outlets, and a function that routes each event by its total. Its declaration is the
Python one's, so the two descriptors declare the same streamlet and differ only in the SDK that
wrote them; the Python sample's blueprint, compose file and sidecar configuration run it unchanged.

In your own project, depend on the published SDK (Scala 3.3 or later, Java 21 or later):

```scala
libraryDependencies += "com.thinkmorestupidless" %% "ankka-flow-sdk" % "<version>"
```

| file | what |
|---|---|
| `src/main/scala/cart/CartRouter.scala` | the streamlet |
| `src/main/scala/cart/Main.scala` | serves it on `127.0.0.1:$FLOW_PROCESS_PORT` |
| `src/test/scala/cart/CartRouterSuite.scala` | the harness: no Kafka, no sidecar |
| `flow/descriptor.json` | written by `sbt cartRouterScala/descriptor`; committed; checked by `sbt cartRouterScala/descriptorCheck` |

The blueprint, `docker-compose.yml`, `flow/streamlet.conf`, `produce.py` and `verify.py` are the
Python sample's, in `samples/cart-router`.

## On a laptop

From the repository root:

```bash
sbt sidecar/docker:publishLocal                  # the sidecar image, once
sbt cartRouterScala/test cartRouterScala/descriptorCheck
sbt cartRouterScala/stage
(cd samples/cart-router && docker compose up -d) # Kafka and the sidecar; the sidecar waits for the router
samples/cart-router-scala/target/universal/stage/bin/cart-router-scala &   # the router on 127.0.0.1:9010
(cd samples/cart-router && uv sync && uv run python produce.py && uv run python verify.py)
```

`sbt cartRouterScala/run` serves it the same way, from sbt.

## The image

```bash
sbt cartRouterScala/docker:publishLocal          # sample-cart-router-scala:<version> and :latest
```

The image holds the router, the SDK and a Java runtime, and exposes no port: the sidecar in the same
pod dials `127.0.0.1:9010`.

## On a kind cluster

With `just up` run from the repository root, and the input topic created as the Python sample's
README shows:

```bash
kind load docker-image --name ankka sample-cart-router-scala:latest
kubectl create namespace shop
flow generate samples/cart-router/blueprint.conf --descriptors samples/cart-router-scala/flow \
  --conf samples/cart-router/k8s/in-cluster.conf --image router=sample-cart-router-scala:latest -n shop \
  | kubectl apply -f -
kubectl -n shop get aflow cart -w                  # Pending, then Ready
```

# {{name}}

An [ankka-flow](https://flow.ankka.cloud/) streamlet in Python, written by `flow init` from ankka-flow
{{flow_version}}. It reads JSON objects from the topic `{{name}}.in`, adds a `greeting` — the
parameter of the same name, `hello, ankka-flow` unless the pipeline sets it — and writes them to
`{{name}}.out`, keeping each record's key and headers. A value that is not a JSON object fails its
batch, and the sidecar delivers it again.

| file | what |
|---|---|
| `src/{{module}}/streamlet.py` | the streamlet |
| `src/{{module}}/main.py` | serves it on `127.0.0.1:$FLOW_PROCESS_PORT` (9010) |
| `tests/test_streamlet.py` | its tests |
| `flow/descriptor.json` | the descriptor, committed; `uv run descriptor --check` compares it |
| `flow/streamlet.conf` | the sidecar's configuration for the compose network |
| `blueprint.conf` | the pipeline: an input topic something else writes, an output topic it owns |
| `docker-compose.yml` | Kafka and the sidecar, for the laptop |
| `k8s/in-cluster.conf` | deploy-time configuration for a kind cluster |

## Test, check the descriptor, verify the blueprint

```bash
uv sync && uv run pytest -q
uv run descriptor --check
flow verify blueprint.conf --descriptors flow
```

The tests use the SDK's harness: no Kafka, no sidecar. `flow/descriptor.json` is what the
streamlet declares, and what the sidecar compares with the running process before it sends a
record; after changing a port, the parameter or the name, rewrite it with `uv run descriptor`.

## On a laptop

Kafka and the sidecar run in containers; the streamlet runs on the host, where the sidecar dials it
on port 9010. There is no operator on a laptop, so the topics are created by hand.

```bash
docker compose up -d
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic {{name}}.in --partitions 3
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic {{name}}.out --partitions 3
uv run python -m {{module}}.main &
echo 'k-1:{"id": 1}' | docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic {{name}}.in --property parse.key=true --property key.separator=:
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic {{name}}.out --from-beginning --property print.key=true --timeout-ms 10000
```

The consumer prints `k-1` and the object with its greeting. `docker compose down` stops it all.

## The image

```bash
docker build -t {{name}}:0.1.0 .
```

The image holds the streamlet and the SDK and exposes no port: in a cluster the platform runs the
sidecar beside it in the same pod.

## On a cluster

With ankka-flow installed (a kind cluster set up by ankka-flow's `just up` has the operator and a
Kafka), load the image, create the input topic, then generate and apply the pipeline:

```bash
kind load docker-image --name ankka {{name}}:0.1.0
kubectl -n kafka exec kafka-0 -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic {{name}}.in --partitions 3
kubectl create namespace {{name}}
flow generate blueprint.conf --descriptors flow --conf k8s/in-cluster.conf \
  --image {{name}}={{name}}:0.1.0 -n {{name}} | kubectl apply -f -
kubectl -n {{name}} get aflow {{name}} -w            # Pending, then Ready
```

## Versions

The SDK in `pyproject.toml` and the sidecar image in `docker-compose.yml` are ankka-flow {{flow_version}}'s; move them
together. A project written by a `flow` built from source between releases names a version no
registry holds: build it against the ankka-flow repository's SDK, or write it with a released `flow`.

## For a coding agent

`.claude/skills/` holds the ankka-flow skills of this version: the platform, the Python SDK, the
protocol and deploying. Claude Code reads them from here with no configuration.

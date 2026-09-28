# {{name}}

An [ankka-flow](https://github.com/thinkmorestupidless/ankka-flow) streamlet in Python.

## The laptop loop

```bash
uv sync
uv run pytest -q                        # the harness: no Kafka, no sidecar
uv run descriptor                       # writes flow/descriptor.json from the declaration
docker compose up -d                    # Kafka on localhost:9094 and the sidecar, which waits for you
uv run python -m {{module}}.main        # your streamlet on 127.0.0.1:9010
```

Create the topics named in `flow/streamlet.conf` first: the compose Kafka does not auto-create
them, and on a laptop there is no operator to do it. In a cluster, `flow generate` turns
`blueprint.conf` and `flow/descriptor.json` into a pipeline resource and the operator does the rest.

Commit `flow/descriptor.json`. `uv run descriptor --check` fails when it is stale.

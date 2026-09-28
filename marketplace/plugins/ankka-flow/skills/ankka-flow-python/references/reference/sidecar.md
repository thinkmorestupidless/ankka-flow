# Sidecar

> The ankka-flow sidecar's environment variables, the two files it reads, its start-up checks and exit codes, its probes, metrics, stall warnings and logs.

Source: https://flow.ankka.cloud/reference/sidecar/
Image `ankka-flow-sidecar`, published as `ghcr.io/thinkmorestupidless/ankka-flow-sidecar` and built
locally by `sbt sidecar/docker:publishLocal`. It runs beside every streamlet's process and owns
everything Kafka: subscribing, batching, producing, committing after the write, consumer groups, lag and
stall warnings. In a cluster the operator adds it to each pod from its own `FLOW_SIDECAR_IMAGE` setting;
a pipeline never names it. On a laptop it runs in Docker Compose beside a process on the host. How it
behaves is explained in [The sidecar](../concepts/sidecar.md).

## Environment

Each variable can also be given as a system property, the name lower-cased with `_` replaced by `.`
(`flow.process.address`).

| variable | default | meaning |
|---|---|---|
| `FLOW_PROCESS_ADDRESS` | `127.0.0.1:9010` | `host:port` of the streamlet's process. The operator sets `127.0.0.1:9010`; a compose file sets `host.docker.internal:9010` |
| `FLOW_CONFIG_DIR` | `/etc/flow/config` | holds `descriptor.json` and `streamlet.conf` |
| `FLOW_STATE_DIR` | `/tmp/flow` | where the `ready` and `alive` files are written |
| `FLOW_METRICS_PORT` | `2050` | the Prometheus metrics port |
| `FLOW_DISCOVERY_TIMEOUT` | `60s` | the deadline of one discovery call; attempts continue forever |
| `FLOW_STALL_WARNING_AFTER` | `5m` | how long a partition may go without committing before a stall warning |
| `FLOW_RECONNECT_MAX_BACKOFF` | `30s` | the ceiling of the backoff between conversations after a failure |
| `FLOW_POD_NAME`, `FLOW_POD_NAMESPACE` | set by the operator | the pod a stall warning is recorded on |
| `KUBERNETES_SERVICE_HOST`, `KUBERNETES_SERVICE_PORT` | set by Kubernetes | with the pod name and namespace, stall warnings become Events; otherwise they are log lines |

Durations are HOCON durations: `500ms`, `30s`, `5m`. The process container receives
`FLOW_PROCESS_PORT` (9010) and nothing else from the platform.

## `descriptor.json`

The streamlet's descriptor as deployed: in a cluster the resource's `descriptor`, locally the file
`uv run descriptor` wrote. The sidecar compares it with what the process answers at discovery. Its format
is in [the descriptor reference](descriptor.md).

## `streamlet.conf`

HOCON naming the pipeline, the streamlet, its resolved parameters, and one entry per port with that
port's topic and its own Kafka connection. In a cluster the operator renders it into the streamlet's
Secret. On a laptop you write it beside a compose file:

```hocon
# The sidecar's configuration for the compose network. In a cluster the operator renders the same
# file from the pipeline resource. descriptor.json beside it is written by `uv run descriptor`.
flow {
  pipeline  = cart
  streamlet = router
  config    = { review-threshold = 100 }
  inlets {
    in {
      topic             = "shop.cart-events.v1"
      group             = "cart.router.in"
      client-id         = "cart.router.in"
      bootstrap.servers = "kafka:9092"
      connection-config {}
      consumer-config { auto.offset.reset = earliest }
      batch { max-records = 100, max-bytes = 1 MiB }
    }
  }
  outlets {
    valid {
      topic             = "cart.valid-carts"
      client-id         = "cart.router.valid"
      bootstrap.servers = "kafka:9092"
      connection-config {}
      producer-config {}
    }
    review {
      topic             = "cart.review-carts"
      client-id         = "cart.router.review"
      bootstrap.servers = "kafka:9092"
      connection-config {}
      producer-config {}
    }
  }
}
```

| key | required | meaning |
|---|---|---|
| `flow.pipeline`, `flow.streamlet` | yes | the pipeline id and the streamlet's name |
| `flow.config` | no | parameter values; a parameter with a default may be left out. Sent to the process as `Start.config_json` |
| `inlets.<name>.topic`, `outlets.<name>.topic` | yes | the Kafka topic's name |
| `…bootstrap.servers` | yes | the brokers, per port |
| `inlets.<name>.group` | no | the consumer group; default `<pipeline>.<streamlet>.<inlet>` |
| `…client-id` | no | the Kafka client id; default `<pipeline>.<streamlet>.<port>` |
| `…connection-config` | no | client properties for this port: `security.protocol`, `sasl.*`, `ssl.*` |
| `inlets.<name>.consumer-config`, `outlets.<name>.producer-config` | no | consumer or producer properties |
| `inlets.<name>.batch.max-records`, `…max-bytes` | no | the largest batch sent to the process; default `100` and `1 MiB` |

Every inlet reads with `auto.offset.reset = earliest` unless its `consumer-config` says otherwise, so a
new pipeline reads its inputs from the start. `allow.auto.create.topics` is always `false`: reading a
topic never creates it. The inlets and outlets must be exactly the descriptor's ports.

## Start-up and exit codes

1. Read the environment, `descriptor.json` and `streamlet.conf`, and check that the file's ports and
   parameters match the descriptor. Any problem is logged and the sidecar exits with code `2`.
2. Call `Discover` on the process until it answers, retrying from 500 ms, doubling to 10 s, forever.
3. Check the answer: the protocol major version must equal the sidecar's and its minor must not be later;
   the descriptor must be valid and equal to the deployed one. On any problem it sends every problem to
   the process's `ReportError`, logs them, and exits with code `1`.
4. Open a `Run` conversation, subscribe every inlet, and write `ready` once every inlet is subscribed and
   its topic exists.

On a failure after start-up the sidecar never exits: it discards the batches in flight, removes `ready`,
waits (500 ms, doubling to `FLOW_RECONNECT_MAX_BACKOFF`), repeats discovery, and resumes from the last
committed offsets. It exits `0` when it is stopped. The protocol itself is in
[the protocol reference](protocol.md).

## Probes

Rendered by the operator on the sidecar container only; the process container has no probes.

| probe | command | timing |
|---|---|---|
| readiness | `test -f /tmp/flow/ready` | every 5 s |
| liveness | the age of `/tmp/flow/alive` is under 15 s | every 10 s, after 20 s |

`ready` exists while a conversation runs and every inlet is subscribed to a topic that exists. It is
removed the moment the conversation fails or the sidecar is stopping. `alive` is touched every second.

## Metrics

Prometheus text on `:2050/metrics`, from the JMX exporter agent. Every Kafka client's id is
`<pipeline>.<streamlet>.<port>`, so `client_id` attributes lag and throughput to one port of one
streamlet.

| metric | labels | meaning |
|---|---|---|
| `kafka_consumer_consumer_fetch_manager_metrics_records_lag` | `client_id`, `topic`, `partition` | records behind, per inlet partition |
| `kafka_consumer_consumer_fetch_manager_metrics_records_lag_max` | `client_id`, `topic`, `partition` | the maximum lag in the current window |
| `kafka_consumer_consumer_fetch_manager_metrics_records_consumed_rate` | `client_id`, `topic` | records read per second |
| `kafka_producer_producer_metrics_record_send_rate` | `client_id`, `topic` | records written per second, per outlet |
| `ankka_flow_sidecar_in_flight` | `inlet`, `partition` | `1` while a batch of that partition is with the process |
| `ankka_flow_sidecar_stalled_seconds` | `inlet`, `partition` | how long the partition has gone without committing; `0` when nothing is outstanding |

Kafka reports topic names in these labels with dots replaced by underscores:
`shop.cart-events.v1` appears as `shop_cart-events_v1`. The pod template carries
`prometheus.io/scrape: "true"` and `prometheus.io/port: "2050"`.

## Stall warnings

When `ankka_flow_sidecar_stalled_seconds` for a partition first passes `FLOW_STALL_WARNING_AFTER`, the
sidecar records one warning for that stall, reason `PartitionStalled`, with a note naming the pipeline,
streamlet, inlet and partition, how long it has not committed, and the last error. In a pod it is an
`events.k8s.io/v1` Warning Event regarding the pod, posted with the pipeline's service account. Elsewhere,
or if posting fails, it is a `warn` log line. The stall clears when a batch of that partition commits.

## Logs

One line per event on stdout at `info`: the start-up line naming the pipeline, streamlet and process
address, each discovery attempt and its result, each conversation's id, partitions assigned, revoked and
lost, each stream failure with its cause, each reconnect with its backoff, and readiness. A missing input
topic is logged as `inlet '<name>': topic '<topic>' does not exist; not ready until it does`. No record
value is ever logged.

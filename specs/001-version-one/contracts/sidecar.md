# Contract: the sidecar

**Feature**: [spec.md](../spec.md) | **Research**: R2, R7–R10

The sidecar is one Pekko Streams program built at startup from two files. It is published as the
image `ankka-flow-sidecar:<version>`, built by `sbt sidecar/docker:publishLocal`, pushed on a
release tag as `ghcr.io/thinkmorestupidless/ankka-flow-sidecar`. It is never a library. Nothing in
a pipeline resource names it (FR-020); the operator knows it from `FLOW_SIDECAR_IMAGE`.

## Environment

| variable | default | who sets it |
|---|---|---|
| `FLOW_PROCESS_ADDRESS` | `127.0.0.1:9010` | the operator; compose sets `host.docker.internal:9010` |
| `FLOW_CONFIG_DIR` | `/etc/flow/config` | holds `descriptor.json` and `streamlet.conf` (below) |
| `FLOW_STATE_DIR` | `/tmp/flow` | where `ready` and `alive` are written |
| `FLOW_METRICS_PORT` | `2050` | the JMX exporter agent's port; the operator names it `metrics` |
| `FLOW_DISCOVERY_TIMEOUT` | `60s` per attempt, retried forever | |
| `FLOW_STALL_WARNING_AFTER` | `5m` | a partition stalled this long is a warning event |
| `FLOW_RECONNECT_MAX_BACKOFF` | `30s` | reconnect backoff caps here (starts at 500 ms) |
| `KUBERNETES_SERVICE_HOST` | set by Kubernetes | when present, stall warnings are Events; otherwise log lines |
| `FLOW_POD_NAME`, `FLOW_POD_NAMESPACE` | set by the operator from the downward API | the pod a `PartitionStalled` event regards |
| `FLOW_SIDECAR_PORT`, `FLOW_SIDECAR_BIND` | reserved (9011, `127.0.0.1`) | unused in 1.0 |

The process container gets `FLOW_PROCESS_PORT=9010` and nothing else from the platform.

## Files in `FLOW_CONFIG_DIR`

`descriptor.json`: the deployed descriptor (contracts/descriptor.md), compared with discovery.

`streamlet.conf` (HOCON), rendered by the operator into the streamlet's Secret, or written by hand
locally from the template:

```hocon
flow {
  pipeline  = cart
  streamlet = router
  config    = { review-threshold = 100 }          # resolved parameters, becomes Start.config_json
  inlets {
    in {
      topic            = "shop.cart-events.v1"
      group            = "cart.router.in"           # <pipeline>.<streamlet>.<inlet>
      client-id        = "cart.router.in"           # <pipeline>.<streamlet>.<port>
      bootstrap.servers = "kafka:9092"
      connection-config { }                          # security.protocol, sasl.*, ssl.* …
      consumer-config   { auto.offset.reset = earliest }
      batch { max-records = 100, max-bytes = 1 MiB }
    }
  }
  outlets {
    valid  { topic = "cart.valid-carts",  client-id = "cart.router.valid",  bootstrap.servers = "kafka:9092", connection-config {}, producer-config {} }
    review { topic = "cart.review-carts", client-id = "cart.router.review", bootstrap.servers = "kafka:9092", connection-config {}, producer-config {} }
  }
}
```

Every inlet and outlet carries its own connection, so an unmanaged topic on other brokers (FR-005)
needs nothing special. `auto.offset.reset` defaults to `earliest` for every inlet so a new
pipeline reads its inputs from the start.

## Startup

1. Read both files; refuse to start, with exit code 2, on a parse error or a descriptor that fails
   validation.
2. Discovery (protocol.md): dial, compare, validate; on refusal `ReportError`, log, exit 1. The
   channel sends no HTTP/2 keepalive pings: gRPC servers on default settings answer frequent pings
   with `GOAWAY too_many_pings`, and on loopback they detect nothing a reset socket does not.
3. Build one consumer per inlet (`committablePartitionedSource`, group and client id from the
   file), one `SendProducer` per outlet, the `RemoteProcessor` over a new `Run` conversation.
4. When every inlet's consumer has joined its group: write `ready` (FR-014, S3.3).
5. Touch `alive` every second while the supervisor loop runs.

## Readiness and liveness

Exec probes rendered by the operator on the sidecar container only:
`readinessProbe: exec: test -f /tmp/flow/ready` (period 5 s), `livenessProbe: exec: test $(( $(date
+%s) - $(stat -c %Y /tmp/flow/alive) )) -lt 15` (period 10 s, initial delay 20 s). `ready` is removed the moment the
process is unreachable or the stream is reconnecting, and rewritten when the rebuilt graph has
rejoined its groups. The process container has no ports and no probe (S3.2).

## Metrics

`jmx_prometheus_javaagent` in the image at `/opt/flow/jmx_prometheus_javaagent.jar`, started with
`-javaagent:…=${FLOW_METRICS_PORT}:/opt/flow/prometheus.yaml`. The rules file is the one carried
from the Cloudflow fork (records-lag, records-lag-max, records-consumed-rate, record-send-rate,
all labelled `client_id`, and `topic`/`partition` where JMX has them) plus two rules for the
sidecar's own MBeans:

| metric | labels | meaning |
|---|---|---|
| `kafka_consumer_consumer_fetch_manager_metrics_records_lag` | `client_id`, `topic`, `partition` | lag per inlet partition; `client_id` is `<pipeline>.<streamlet>.<inlet>` (S4.4) |
| `kafka_producer_producer_metrics_record_send_rate` | `client_id`, `topic` | emits per second per outlet |
| `ankka_flow_sidecar_in_flight` | `inlet`, `partition` | 1 while a batch is with the process |
| `ankka_flow_sidecar_stalled_seconds` | `inlet`, `partition` | age of the oldest uncommitted batch; 0 when none |

Pod annotations: `prometheus.io/scrape: "true"`, `prometheus.io/port: "2050"`.

## Failure and recovery

As protocol.md's *Failing the stream*: fail in-flight batches, remove `ready`, close, back off,
re-discover, new `Start`, rebuild, resume from committed offsets. Repeats indefinitely (FR-012a).
Stall warning: once per stall per partition, when `stalled_seconds` first passes
`FLOW_STALL_WARNING_AFTER`: in a pod, an `events.k8s.io/v1` Event of type `Warning`, reason
`PartitionStalled`, regarding the pod, note naming pipeline, streamlet, inlet, partition, offset
and the last error; posted to the API server with the pod's service account token (the operator
grants `create` on `events`); elsewhere a `warn` log line with the same fields.

## Logging

Logback to stdout, one line per event at `info`: discovery attempts and result, each conversation
start with its id, each stream failure with its cause and the batches it voided, each reconnect
with the backoff, ready and not-ready transitions, each stall warning. Batches are logged at
`debug` only. No record value is ever logged (FR-009).

## Local run

`docker compose up` from the SDK template starts Kafka (`apache/kafka:3.9.1`, KRaft, listeners
`kafka:9092` inside compose and `localhost:9094` on the host) and the sidecar with
`FLOW_PROCESS_ADDRESS=host.docker.internal:9010`, `extra_hosts: host.docker.internal:host-gateway`,
and `./flow` mounted at `/etc/flow/config`. The developer runs `uv run descriptor` (writes
`flow/descriptor.json`) and starts their process on 9010 before or after; the sidecar waits. The
metrics port is published on `localhost:2050`.

## What the sidecar refuses

- A `Spec` whose protocol major differs or minor is later; a descriptor that differs from the
  deployed one; a descriptor that fails validation: at startup, every problem at once, after
  `ReportError`, then exit 1.
- An emit for an undeclared outlet, an ack for a batch not in flight, an emit after its ack: the
  stream fails (never the process: it is redialled).
- A record over the message limit: the stream fails naming topic, partition and offset.
- A `streamlet.conf` whose inlet or outlet names do not match the descriptor's ports: refused at
  startup, before discovery.

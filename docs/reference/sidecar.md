# The sidecar

Image `ankka-flow-sidecar`. Set on the operator (`FLOW_SIDECAR_IMAGE`), never in a pipeline.

## Environment

| variable | default | |
|---|---|---|
| `FLOW_PROCESS_ADDRESS` | `127.0.0.1:9010` | where your process listens |
| `FLOW_CONFIG_DIR` | `/etc/flow/config` | holds `descriptor.json` and `streamlet.conf` |
| `FLOW_STATE_DIR` | `/tmp/flow` | where the `ready` and `alive` files are written |
| `FLOW_METRICS_PORT` | `2050` | Prometheus metrics |
| `FLOW_DISCOVERY_TIMEOUT` | `60s` | per discovery attempt; attempts continue forever |
| `FLOW_STALL_WARNING_AFTER` | `5m` | a partition stalled this long is a warning |
| `FLOW_RECONNECT_MAX_BACKOFF` | `30s` | the reconnect backoff's ceiling |

Your process receives `FLOW_PROCESS_PORT` (9010) and nothing else from the platform.

## `streamlet.conf`

```hocon
flow {
  pipeline  = cart
  streamlet = router
  config    = { review-threshold = 100 }
  inlets {
    in {
      topic = "shop.cart-events.v1"
      group = "cart.router.in"            # <pipeline>.<streamlet>.<inlet>
      client-id = "cart.router.in"        # <pipeline>.<streamlet>.<port>
      bootstrap.servers = "kafka:9092"
      connection-config { }
      consumer-config { auto.offset.reset = earliest }
      batch { max-records = 100, max-bytes = 1 MiB }
    }
  }
  outlets {
    valid { topic = "cart.valid-carts", bootstrap.servers = "kafka:9092", producer-config { } }
  }
}
```

In a cluster the operator renders this file. On a laptop you write it beside a compose file; the
SDK's project template has one.

## Probes

Readiness is the file `/tmp/flow/ready`: it exists while the conversation runs, every inlet is
subscribed and every inlet's topic exists. Liveness is `/tmp/flow/alive`, touched every second.

## Metrics

| metric | labels |
|---|---|
| `kafka_consumer_consumer_fetch_manager_metrics_records_lag`, `…_records_lag_max` | `client_id`, `topic`, `partition` |
| `kafka_consumer_consumer_fetch_manager_metrics_records_consumed_rate` | `client_id`, `topic` |
| `kafka_producer_producer_metrics_record_send_rate` | `client_id`, `topic` |
| `ankka_flow_sidecar_in_flight` | `inlet`, `partition` |
| `ankka_flow_sidecar_stalled_seconds` | `inlet`, `partition` |

Kafka reports topic names with dots replaced by underscores in its metric tags.

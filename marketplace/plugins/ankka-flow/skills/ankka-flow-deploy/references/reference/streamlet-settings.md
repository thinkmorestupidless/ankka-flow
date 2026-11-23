# Streamlet settings

> The file streamlet.properties a streamlet is given at run time — its names, parameters, and the topic, group, client id, delivery and Kafka properties of each of its ports — and the rules both SDKs read it by.

Source: https://flow.ankka.cloud/reference/streamlet-settings/
A streamlet is given one file at run time, `streamlet.properties`, in the directory the environment
variable `FLOW_CONFIG_DIR` names. The operator writes it in a pod; `flow local` writes it on a
laptop. Both SDKs read it; a streamlet's own code never does.

The file is Java properties, ISO 8859-1 with `\u` escapes, as `java.util.Properties` reads it.

```properties
pipeline=carts
streamlet=counter
namespace=shop
local=false

parameter.window=1m

inlet.in.topic=carts.clean-carts
inlet.in.group=carts.counter.in
inlet.in.client-id=carts.counter.in
inlet.in.kafka.bootstrap.servers=kafka.kafka.svc:9092
inlet.in.kafka.isolation.level=read_committed

outlet.out.topic=carts.cart-counts
outlet.out.client-id=carts.counter.out
outlet.out.delivery=exactly-once
outlet.out.transactional-id-prefix=flow.shop.carts.counter.out.e1
outlet.out.kafka.bootstrap.servers=kafka.kafka.svc:9092
outlet.out.kafka.transaction.timeout.ms=900000
```

| Key | Meaning |
|---|---|
| `pipeline`, `streamlet`, `namespace` | names, for logs and client ids |
| `local` | `true` only from `flow local`: no cluster; a Flink streamlet runs on Flink's local environment |
| `parameter.<key>` | every declared parameter, as text of its type, its default already applied |
| `inlet.<port>.topic` | the Kafka topic |
| `inlet.<port>.group` | the consumer group, used as given |
| `inlet.<port>.client-id`, `outlet.<port>.client-id` | the Kafka client id |
| `inlet.<port>.kafka.<property>` | every Kafka consumer property of the port: the cluster's `connection-config` and `consumer-config`, then the topic's, the later winning |
| `outlet.<port>.topic` | the Kafka topic |
| `outlet.<port>.delivery` | `at-least-once` or `exactly-once`; a Pekko Streams streamlet is only ever given the first |
| `outlet.<port>.transactional-id-prefix` | present with `exactly-once`; it names the state epoch, so a reset streamlet's transactions never meet its earlier ones |
| `outlet.<port>.kafka.<property>` | every Kafka producer property of the port |

## Rules

- Every declared port and parameter has its keys. A missing one fails the streamlet at start-up,
  naming the key, and so does a port or parameter the streamlet does not declare.
- A streamlet's file holds the settings of its own ports, credentials included, and of no other
  streamlet's.
- An SDK sets no Kafka property the file does not give, except what its delivery needs:
  `enable.auto.commit=false`, and for a Pekko Streams streamlet `enable.idempotence=true` and
  `acks=all`.
- `auto.offset.reset` is `earliest` unless the file gives it.
- An inlet on a topic the pipeline manages reads with `isolation.level=read_committed`.
- In a pod the file is the key `streamlet.properties` of the Secret `flow-<pipeline>-<streamlet>`,
  mounted read-only at `/etc/flow/config` with mode 0440.

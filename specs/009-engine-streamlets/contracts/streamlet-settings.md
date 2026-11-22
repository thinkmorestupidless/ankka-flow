# Contract: `streamlet.properties`

What a streamlet is given at run time, by the operator in a pod and by `flow local` on a laptop.
One file, Java properties (ISO 8859-1 with `\u` escapes, as `java.util.Properties` reads), in the
directory the environment variable `FLOW_CONFIG_DIR` names. Both SDKs read it; an author never
does.

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
| `local` | `true` only from `flow local`: no cluster; a Flink streamlet uses its local environment |
| `parameter.<key>` | every declared parameter, as text of its type; the default already applied |
| `inlet.<port>.topic` | the Kafka topic name |
| `inlet.<port>.group` | the consumer group; the SDK uses it as given |
| `inlet.<port>.client-id`, `outlet.<port>.client-id` | the Kafka client id |
| `inlet.<port>.kafka.<property>` | every Kafka consumer property for this port: the cluster's `connection-config` and `consumer-config`, then the topic's, later winning |
| `outlet.<port>.topic` | the Kafka topic name |
| `outlet.<port>.delivery` | `at-least-once` or `exactly-once`; a Pekko Streams streamlet is only ever given the first |
| `outlet.<port>.transactional-id-prefix` | present with `exactly-once` |
| `outlet.<port>.kafka.<property>` | every Kafka producer property for this port |

Rules:

- Every declared port and parameter has its keys. A missing one is the SDK's start-up failure,
  naming the key.
- A streamlet's file holds the settings of its own ports only.
- An SDK sets no Kafka property the file does not give except the ones its guarantee needs:
  `enable.auto.commit=false`; for Pekko Streams `enable.idempotence=true` and `acks=all`.
- `auto.offset.reset` is `earliest` unless the file gives it.
- In a pod the file is the key `streamlet.properties` of the Secret `flow-<pipeline>-<streamlet>`,
  mounted read-only at `/etc/flow/config`, mode 0440.

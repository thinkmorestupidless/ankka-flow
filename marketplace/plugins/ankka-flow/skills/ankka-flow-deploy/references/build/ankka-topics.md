# Read and write ankka topics

> Build a pipeline on an ankka project's topics — name each by project and name, check its contracts against the project's declarations, reach the broker as a registered machine, and see what a missing grant looks like.

Source: https://flow.ankka.cloud/build/ankka-topics/
An ankka project's topics are ankka's: the project declares each one, with its partitions and its
contract, and ankka makes it on the installation's broker. A pipeline can read them and write them,
whatever the engine of the streamlet, by naming each topic by its project and its name. The pipeline
never creates, alters or deletes one, and it reaches the broker the way any system outside the
installation does: as a **registered machine** that the project has granted each topic to.

This page builds the sample in
[`samples/cart-pipeline/ankka`](https://github.com/thinkmorestupidless/ankka-flow/blob/main/samples/cart-pipeline/ankka):
the cleaner, between two topics of the project `checkout`.

```text
checkout/cart-checkouts ──► cleaner ──► checkout/clean-checkouts
```

## Name the topics

A topic with `ankka = "<project>/<name>"` is that project's topic:

```hocon
blueprint {
  name = checkouts
  streamlets {
    cleaner = cart-cleaner
  }
  topics {
    cart-checkouts {
      ankka     = "checkout/cart-checkouts"
      consumers = [cleaner.in]
      consumer-config { auto.offset.reset = earliest }
    }
    clean-checkouts {
      ankka     = "checkout/clean-checkouts"
      producers = [cleaner.out]
    }
  }
}
```

Its Kafka name is `<project>.<name>`, here `checkout.cart-checkouts`. It may have producers, which
another unmanaged topic may not, and it takes neither `managed = true`, `topic.name` nor
`bootstrap.servers`: ankka names the topic and the broker, and each of those is refused.

## Check the contracts against the project

Every port on an ankka topic must carry the contract the project declares for it. Export the
project's declarations and give them to `flow verify`:

```bash
ankka projects topics list -p checkout -o json > checkout-topics.json
flow verify blueprint.conf --descriptors flow --ankka-topics checkout=checkout-topics.json
```

`--ankka-topics` is repeatable, one per project, and `flow generate` and `flow local` take it too.
`flow verify` refuses:

| when | it says |
|---|---|
| a port's contract name differs from the topic's, or both state a fingerprint and they differ | `cleaner.in declares contract … and ankka topic checkout/cart-checkouts declares …` |
| the project does not declare the topic | `ankka topic checkout/<name> is not declared; project checkout declares: …` |
| the topic declares no contract | `ankka topic checkout/<name> declares no contract: declare one with ankka projects topics set <name> --contract … --schema …` |
| a file names a project the blueprint does not | `--ankka-topics names project '<p>', which no topic of the blueprint does` |

Without a file for a project the blueprint names, the blueprint verifies, with the note
`ankka topics of project 'checkout' were not checked: pass --ankka-topics checkout=<file>`.

## Register the machine

An owner of an organization registers the machine the pipeline acts as, once:

```bash
ankka organizations machines register affiliates network
```

It answers the machine's client id, `machine:affiliates/network`, and its secret, shown then and
never again. The machine holds no grant until a project gives it one.

## Give it to the operator

The machine is a Kafka cluster Secret in the operator's namespace, with one more key, `machine`:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: kafka-cluster-ankka
  namespace: ankka-flow
  labels:
    flow.ankka.thinkmorestupidless.com/kafka-cluster: ankka
stringData:
  bootstrap.servers: broker.<base domain>:9094
  machine: affiliates/network
  connection-config: |
    security.protocol=SASL_SSL
    sasl.mechanism=OAUTHBEARER
    sasl.login.callback.handler.class=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler
    sasl.oauthbearer.token.endpoint.url=https://api.<base domain>/oauth/token
    sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required clientId="machine:affiliates/network" clientSecret="<the machine's secret>";
```

`bootstrap.servers` is the installation's broker for machines, and `connection-config` the
properties ankka documents for one: `SASL_SSL`, `OAUTHBEARER`, the control plane's token endpoint and
the machine's client id and secret. The Kafka clients take the machine's token from the control
plane themselves.

An ankka topic is on the cluster named `ankka` unless the topic names another with `cluster`, or
deploy-time configuration names one for every ankka topic:

```hocon
flow.ankka.cluster = "pipelines"
```

On a machine's cluster every consumer group carries the machine's prefix, the only groups ankka lets
it use: the cleaner's inlet reads as `ankka.machine.affiliates.network.checkouts.cleaner.in`. The
status and `flow reset` name the group the same way.

## Grant the topics

An owner of the project grants the machine each topic, to consume what the pipeline reads and to
produce what it writes:

```bash
ankka projects grants make machine:affiliates/network topic cart-checkouts consume -p checkout
ankka projects grants make machine:affiliates/network topic clean-checkouts produce -p checkout
```

The operator asks the broker, as the machine, what it may do with each of the pipeline's ankka topics
every 30 seconds. A topic it may not read or write is the condition `TopicNotGranted`, recorded as an
event too, and the pipeline is `Degraded`:

```text
machine affiliates/network holds no grant to read topic checkout.cart-checkouts: an owner of project
checkout runs `ankka projects grants make machine:affiliates/network topic cart-checkouts consume -p checkout`
```

Credentials ankka does not accept are the condition `MachineCredentialRefused`, with the broker's
reason. The streamlets are started regardless and keep retrying; each condition clears on the first
pass that succeeds, so a grant made after the deploy is picked up within a minute, and one revoked is
reported within a minute.

## Delivery

A streamlet writes an ankka topic at least once, whatever its engine: a record may arrive twice
after a restart, so a reader of the topic must tolerate repeats. Between topics the pipeline manages,
a Flink streamlet writes each result once; see [Delivery and failure](../concepts/delivery.md).

## Limits

- ankka answers a machine's token requests at most twelve times a minute, and caps connections per
  client address. Many replicas starting at once may be told to slow down; they retry.
- A field ankka encrypts in a message stays an envelope: a streamlet receives it as ankka wrote it.
- The machine's byte rates are the installation's defaults unless an owner sets them.

On a laptop, `flow local` writes an ankka topic's settings for the broker `--ankka-bootstrap-servers`
names, such as the one ankka's local platform runs.

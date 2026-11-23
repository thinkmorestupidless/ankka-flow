#!/usr/bin/env bash
# Publishes COUNT cart events to shop.cart-events.v1 (default 50), every tenth not a cart event,
# through the Kafka image's console producer in the root docker-compose.yml. Event i is cart
# i % 10 at START + 6i seconds, so each cart has one event in each minute; START is the current
# minute unless given (in milliseconds since the epoch). Times are recent because a Flink
# streamlet writes a window's own time as its record's timestamp, and the broker's retention
# deletes a record older than a week.
#
#   samples/cart-pipeline/produce.sh localhost:9092 50 [START]
set -euo pipefail
bootstrap="${1:-localhost:9092}"
count="${2:-50}"
start="${3:-$(( $(date +%s) / 60 * 60000 ))}"

for ((i = 0; i < count; i++)); do
  cart="cart-$((i % 10))"
  if ((i % 10 == 9)); then
    echo "$cart|not a cart event $i"
  else
    echo "$cart|{\"cartId\":\"$cart\",\"seq\":$((i + 1)),\"total\":$(((i * 37) % 200)),\"at\":$((start + i * 6000))}"
  fi
done | docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server "$bootstrap" --topic shop.cart-events.v1 \
  --property parse.key=true --property key.separator='|'
echo "published $count cart events to shop.cart-events.v1 from $start"

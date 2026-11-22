#!/usr/bin/env bash
# Publishes COUNT cart events to shop.cart-events.v1 (default 50), every tenth not a cart event,
# through the Kafka image's console producer in the root docker-compose.yml.
#
#   samples/cart-pipeline/produce.sh localhost:9092 50
set -euo pipefail
bootstrap="${1:-localhost:9092}"
count="${2:-50}"
start=1767225600000 # 2026-01-01T00:00:00Z

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
echo "published $count cart events to shop.cart-events.v1"

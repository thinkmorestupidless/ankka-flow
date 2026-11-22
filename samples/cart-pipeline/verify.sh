#!/usr/bin/env bash
# Checks what the pipeline wrote for produce.sh's events: every cart event that decodes is on
# carts.clean-carts, keyed by its cart, and nothing else is. Exits 0 when it all matches.
#
#   samples/cart-pipeline/verify.sh localhost:9092 50
set -euo pipefail
bootstrap="${1:-localhost:9092}"
count="${2:-50}"
expected=$((count - count / 10))

cleaned=$(docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server "$bootstrap" --topic carts.clean-carts --from-beginning \
  --property print.key=true --property key.separator='|' \
  --max-messages "$expected" --timeout-ms 30000 2>/dev/null || true)

got=$(printf '%s\n' "$cleaned" | grep -c '|' || true)
if [[ "$got" -ne "$expected" ]]; then
  echo "carts.clean-carts: $got records, expected $expected" >&2
  exit 1
fi
if printf '%s\n' "$cleaned" | grep -q 'not a cart event'; then
  echo "carts.clean-carts holds a record that is not a cart event" >&2
  exit 1
fi
while IFS='|' read -r key value; do
  [[ -z "$key" ]] && continue
  if [[ "$value" != *"\"cartId\":\"$key\""* ]]; then
    echo "carts.clean-carts: $value is keyed $key, not by its cart" >&2
    exit 1
  fi
done <<<"$cleaned"
echo "carts.clean-carts: $got clean cart events, each keyed by its cart"

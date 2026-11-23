#!/usr/bin/env bash
# Checks what the pipeline wrote for produce.sh's events. On carts.clean-carts: every cart event
# that decodes, keyed by its cart, and nothing else. On carts.cart-counts: one count per cart and
# closed minute, each of the one event produce.sh puts in it. A window closes once an event 10
# seconds past its end has been read, so the last minute stays open. Exits 0 when it all matches.
#
#   samples/cart-pipeline/verify.sh localhost:9092 50
set -euo pipefail
bootstrap="${1:-localhost:9092}"
count="${2:-50}"
expected=$((count - count / 10))

read_topic() { # topic, how many
  docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server "$bootstrap" --topic "$1" --from-beginning \
    --property print.key=true --property key.separator='|' \
    --max-messages "$2" --timeout-ms 30000 2>/dev/null || true
}

cleaned=$(read_topic carts.clean-carts "$expected")
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

# The closed windows: minute w of a cart that has an event in it, ending at or before the last
# event's time less the counter's 10 seconds of lateness.
last=$(((count - 1) * 6))
windows=0
for ((i = 0; i < count; i++)); do
  ((i % 10 == 9)) && continue
  w=$((i * 6 / 60))
  (((w + 1) * 60 <= last - 10)) && windows=$((windows + 1))
done

counts=$(read_topic carts.cart-counts "$windows")
got=$(printf '%s\n' "$counts" | grep -c '|' || true)
if [[ "$got" -ne "$windows" ]]; then
  echo "carts.cart-counts: $got counts, expected $windows" >&2
  exit 1
fi
distinct=$(printf '%s\n' "$counts" | grep '|' | sed -E 's/.*"windowStart":([0-9]+).*/\1/' |
  paste -d' ' <(printf '%s\n' "$counts" | grep '|' | cut -d'|' -f1) - | sort -u | wc -l | tr -d ' ')
if [[ "$distinct" -ne "$windows" ]]; then
  echo "carts.cart-counts: $distinct distinct cart windows in $got counts" >&2
  exit 1
fi
while IFS='|' read -r key value; do
  [[ -z "$key" ]] && continue
  if [[ "$value" != *"\"cartId\":\"$key\""* || "$value" != *'"count":1}' ]]; then
    echo "carts.cart-counts: $value keyed $key is not one event of its cart" >&2
    exit 1
  fi
done <<<"$counts"
echo "carts.cart-counts: $got counts, one per cart and closed minute"

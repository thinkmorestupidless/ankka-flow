# cart-cleaner

The sample pipeline's cleaner (`samples/cart-pipeline/cleaner`).

- engine: `pekko-streams`
- name: `cart-cleaner`
- description: `Drops cart events that do not decode or whose total is above the maximum, and writes the rest clean.`
- inlet `in`: contract `cart-events.v1` with the schema `samples/cart-pipeline/schemas/cart-events.v1.json`
- outlet `out`: contract `clean-carts.v1`
- parameter `max-total`: INTEGER, default `10000`, description `Events whose total is above this are dropped.`

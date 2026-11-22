# cart-counter

The sample pipeline's counter (`samples/cart-pipeline/counter`).

- engine: `flink`, entry class `cart.counter.CartCounter`, Flink `2.2`
- name: `cart-counter`
- description: `Counts each cart's events in windows of the time they carry.`
- inlet `in`: contract `clean-carts.v1`
- outlet `out`: contract `cart-counts.v1`
- parameter `window`: DURATION, default `1m`, description `The length of each counting window.`

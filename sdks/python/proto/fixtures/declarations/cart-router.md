# cart-router

- name: `cart-router`
- description: `Routes cart events to the valid or review outlet.`
- inlet `in`: json, schema name `cart-events.v1`
- outlet `valid`: json, schema name `cart-events.v1`
- outlet `review`: json, schema name `cart-events.v1`
- parameter `review-threshold`: INTEGER, default `100`,
  description `Carts with a total above this go to the review outlet.`

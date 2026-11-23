Feature: a pipeline reads and writes an ankka project's topics
  A blueprint names an ankka topic by its project and name, and each port's contract is checked
  against the contract the ankka project declares. To ankka a pipeline is a machine, which reads
  and writes only what the project grants it.

  Background:
    Given an ankka project "checkout" that declares a topic "cart-checkouts" with the contract "checkout-notice.v1"
    And a topic "clean-checkouts" with the contract "clean-checkout.v1"
    And a machine "pipelines" that the ankka project grants both topics

  Scenario: an inlet reads an ankka topic by its project and name
    Given a blueprint whose inlet reads the ankka topic "checkout/cart-checkouts" with the contract "checkout-notice.v1"
    When the pipeline is deployed as the machine "pipelines"
    Then the pipeline reads what the ankka project writes to the topic
    And the pipeline is Ready

  Scenario: an outlet writes an ankka topic by its project and name
    Given a blueprint whose outlet writes the ankka topic "checkout/clean-checkouts" with the contract "clean-checkout.v1"
    When the pipeline is deployed as the machine "pipelines"
    Then an ankka consumer of the project reads what the pipeline writes to the topic

  Scenario Outline: a port whose contract differs from the ankka topic's is refused
    Given a blueprint whose <port> is on the ankka topic <topic> with the contract <contract>
    When flow verifies the blueprint with the ankka project's declarations
    Then flow refuses, naming the port's contract and the ankka topic's

    Examples:
      | port   | topic                      | contract             |
      | inlet  | "checkout/cart-checkouts"  | "checkout-notice.v2" |
      | outlet | "checkout/clean-checkouts" | "clean-checkout.v2"  |

  Scenario: an ankka topic the project does not declare is refused by name
    Given a blueprint whose inlet reads the ankka topic "checkout/cart-returns"
    When flow verifies the blueprint with the ankka project's declarations
    Then flow refuses, naming the topic and listing the topics the ankka project declares

  Scenario: an ankka topic declared managed is refused
    Given a blueprint that names the ankka topic "checkout/cart-checkouts" and declares it managed
    When flow verifies the blueprint
    Then flow refuses, saying an ankka topic is the ankka project's and a pipeline never creates it

  Scenario: a blueprint verified without the project's declarations says its ankka topics were not checked
    Given a blueprint whose inlet reads the ankka topic "checkout/cart-checkouts" with the contract "checkout-notice.v1"
    When flow verifies the blueprint without the ankka project's declarations
    Then the blueprint verifies
    And flow prints a note saying the ankka topics were not checked

  Scenario: a pipeline reaches ankka as the machine its deploy-time configuration names
    Given a pipeline whose deploy-time configuration names the machine "pipelines"
    When the pipeline reads the ankka topic "checkout/cart-checkouts"
    Then ankka sees the machine "pipelines" reading the topic
    And flow registered no machine and made no grant

  Scenario: a topic the machine holds no grant for is reported by name
    Given a pipeline reading the ankka topic "checkout/cart-checkouts" as a machine with no grant for it
    When the pipeline is deployed
    Then the pipeline has a condition naming the topic and the right it lacks
    And the pipeline is Ready once the ankka project grants the machine the topic

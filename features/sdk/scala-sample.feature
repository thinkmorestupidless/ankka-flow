Feature: The Scala cart router beside the Python one
  The cart router sample exists in Scala and in Python: the same streamlet, the same blueprint, the
  same tests, its own descriptor and image.

  Scenario: the Scala cart router and the Python one declare the same streamlet
    Given the Scala cart router and the Python cart router
    When each one's descriptor is written
    Then the streamlet part of the two descriptors is identical byte for byte
    And the SDK part of each names its own SDK, and nothing else differs
    And "flow verify" of the shared blueprint gives the same output against either

  Scenario: the Scala cart router runs the laptop walkthrough
    Given Kafka and the sidecar in containers, and the Scala cart router served on the host
    When cart events are produced to the input topic
    Then each event reaches the outlet the blueprint wires, routed by the parameter
    And the round trip is the one the Python cart router gives

  Scenario: the Scala cart router's tests and descriptor are checked on every change
    Given a change that can affect the Scala SDK or the Scala cart router
    When CI runs
    Then the Scala cart router's tests pass
    And its committed descriptor equals what the build writes

  Scenario: the README shows the router in Scala and in Python from tested code
    Given the README
    When a reader looks for a streamlet
    Then the cart router is shown in Scala and in Python
    And each is included from its sample's tested code, checked for drift

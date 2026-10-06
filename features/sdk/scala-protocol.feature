Feature: The Scala SDK speaks the protocol
  The descriptor the Scala SDK writes and the conversation it runs are the protocol's, proven by
  the descriptor fixtures and the conformance suite, exactly as for the Python SDK.

  Background:
    Given the Scala SDK

  Scenario Outline: the descriptor written in Scala equals the fixture's bytes
    Given the fixture streamlet "<fixture>" declared in Scala
    When the Scala SDK writes its descriptor
    Then the descriptor equals the fixture's bytes

    Examples:
      | fixture     |
      | minimal     |
      | cart-router |
      | every-type  |
      | many-ports  |
      | sink        |
      | conformance |

  Scenario Outline: a declaration the protocol refuses is refused before a descriptor is written
    Given a streamlet declared in Scala with <problem>
    When the Scala SDK writes its descriptor
    Then the declaration is refused, naming the problem
    And no descriptor is written

    Examples:
      | problem                                   |
      | two ports with one name                   |
      | an inlet with an empty schema name        |
      | a parameter with a default of another type |
      | no inlet and no outlet                    |

  Scenario: the conformance suite passes against the Scala reference streamlet
    Given the reference streamlet declared in Scala and served on a port
    When the conformance suite is run against that port
    Then every case that applies to a process passes

  Scenario: the sidecar runs a Scala streamlet as it runs a Python one
    Given the cart router declared in Scala, served on the process port, and its descriptor deployed
    When the sidecar starts beside it
    Then discovery equals the deployed descriptor and the sidecar runs the streamlet
    And cart events reach the outlets as they do with the Python cart router

  Scenario: the Scala SDK's copy of the protocol is the repository's
    Given the Scala SDK's copy of the protocol
    When the build compares it with the repository's protocol
    Then the two are identical byte for byte
    And a difference fails the build

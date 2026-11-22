Feature: a streamlet is run by the engine it is written for
  A streamlet's descriptor names its engine. A blueprint wires streamlets of any engines over
  topics, and their contracts, lag and reset are the same whatever runs them.

  Background:
    Given a cluster that runs pipelines

  Scenario: a descriptor names the engine its streamlet is written for
    Given a streamlet declared with the Pekko Streams SDK
    When the SDK writes the streamlet's descriptor
    Then the descriptor names the engine "pekko-streams"

  Scenario: a Pekko Streams streamlet reads its inlet and writes its outlet
    Given a pipeline of one Pekko Streams streamlet between two managed topics
    When a record is written to the inlet's topic
    Then the streamlet's result for the record is on the outlet's topic
    And the streamlet's pod has one container

  Scenario: a Pekko Streams streamlet commits a record only after what it wrote is confirmed
    Given a pipeline of one Pekko Streams streamlet between two managed topics
    When the streamlet fails after writing a record's result and before committing the record
    Then the record is read again
    And no record is skipped

  Scenario: a Pekko Streams streamlet is reset to read its topics again from the start
    Given a Pekko Streams streamlet that has read every record of its inlet's topic
    When the streamlet is reset
    Then the streamlet reads the topic again from the start

  Scenario: a Flink streamlet reads its inlet and writes its outlet
    Given a pipeline of one Flink streamlet between two managed topics
    When a record is written to the inlet's topic
    Then the streamlet's result for the record is on the outlet's topic

  Scenario: a blueprint wires streamlets of different engines over one topic
    Given a blueprint whose Pekko Streams streamlet writes a topic that its Flink streamlet reads
    And both ports declare the contract "cart-events.v1"
    When flow verifies the blueprint and the pipeline is deployed
    Then the blueprint verifies
    And the pipeline is Ready
    And the Flink streamlet reads what the Pekko Streams streamlet writes

  Scenario Outline: a topic whose ports disagree on a contract is refused whatever their engines
    Given a blueprint whose <writer> writes a topic with the contract "cart-events.v1"
    And whose <reader> reads that topic with the contract "cart-events.v2"
    When flow verifies the blueprint
    Then flow refuses, naming both ports and both contracts

    Examples:
      | writer                  | reader                  |
      | Pekko Streams streamlet | Flink streamlet         |
      | Flink streamlet         | Pekko Streams streamlet |
      | Flink streamlet         | Flink streamlet         |
      | Pekko Streams streamlet | Pekko Streams streamlet |

  Scenario: a descriptor naming an engine flow does not know is refused
    Given a descriptor that names the engine "spark"
    When flow verifies a blueprint that names its streamlet
    Then flow refuses, naming the engine and the engines it knows

  Scenario: a descriptor that names no engine is refused
    Given a descriptor that names no engine
    When flow verifies a blueprint that names its streamlet
    Then flow refuses, saying a descriptor must name an engine

  Scenario: lag is read for every streamlet whatever its engine
    Given a pipeline of a Pekko Streams streamlet and a Flink streamlet
    When the pipeline's lag is read
    Then the answer holds the lag of each streamlet's inlet

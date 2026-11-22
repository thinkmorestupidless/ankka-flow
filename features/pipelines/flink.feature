Feature: a Flink streamlet keeps state
  A Flink streamlet groups records in windows and keeps its state through a failure and a new
  image. A reset discards the state. A cluster that lacks what Flink needs refuses the pipeline.

  Background:
    Given a cluster with the Flink operator and a state store
    And a Flink streamlet that counts cart events for each key in windows of one minute

  Scenario: a Flink streamlet groups records into windows by the time each record carries
    Given two cart events with one key whose times are in one minute, and a third in the next minute
    When the events are written to the inlet's topic in any order
    Then the outlet's topic holds a count of two for the first window and one for the next

  Scenario: a Flink streamlet that fails continues from its state and counts no record twice
    Given the streamlet has counted cart events in a window that is still open
    When the streamlet's pods fail and are replaced
    Then the window's count on the outlet's topic equals the cart events written for it
    And no window's count is written twice

  Scenario: a Flink streamlet keeps its state when a new image of it is deployed
    Given the streamlet has counted cart events in a window that is still open
    When a new image of the streamlet is deployed
    Then the window's count on the outlet's topic holds the events counted before and after

  Scenario: a new image that cannot read the state leaves the streamlet failed and says so
    Given the streamlet has counted cart events in a window that is still open
    When a new image that cannot read the state is deployed
    Then the streamlet is failed, with a condition saying its state could not be read
    And the state is kept

  Scenario: a reset Flink streamlet discards its state and reads its topics again from the start
    Given the streamlet has counted cart events in a window that is still open
    When the streamlet is reset
    Then its state is discarded
    And the streamlet reads the inlet's topic again from the start

  Scenario Outline: a pipeline with a Flink streamlet is refused where the cluster lacks what Flink needs
    Given a cluster without <what is missing>
    When a pipeline with a Flink streamlet is deployed
    Then the pipeline is refused, with a condition naming <what is missing>
    And none of its streamlets is started

    Examples:
      | what is missing    |
      | the Flink operator |
      | a state store      |

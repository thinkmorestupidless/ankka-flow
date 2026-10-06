Feature: A streamlet written in Scala
  A streamlet author declares a streamlet in Scala, implements process over one batch, and tests it
  with the harness, with no Kafka and no sidecar. The Scala SDK decodes nothing and keeps nothing.

  Background:
    Given the Scala SDK

  Scenario: a streamlet declared in Scala routes a batch as the Python one does
    Given the cart router declared in Scala, with the same inlet, outlets and parameter as the Python one
    When the harness runs a batch of cart events through it
    Then each record is on the outlet the Python cart router would emit it to
    And each emit keeps the record's key, headers and bytes

  Scenario: the harness batches and partitions records by key as the sidecar does
    Given records put on an inlet with keys, and a partition count and a batch size chosen by the test
    When the harness runs the streamlet
    Then records with one key are in one partition, in the order they were put
    And every batch holds records of one partition only, no more than the batch size

  Scenario: a failing batch leaves no emit behind
    Given a streamlet whose process emits a record and then fails
    When the harness runs a batch through it
    Then the failure is recorded with the batch
    And no emit of that batch is on any outlet

  Scenario: an emit to an undeclared outlet fails the batch
    Given a streamlet whose process emits to an outlet it did not declare
    When the harness runs a batch through it
    Then the batch fails, naming the outlet
    And no emit of that batch is on any outlet

  Scenario: a parameter's value comes from the deploy-time configuration, or its default
    Given a streamlet declaring a parameter with a default
    When the harness runs it with a deploy-time configuration that sets the parameter
    Then process reads the configured value, typed as declared
    And without that setting process reads the default

  Scenario: the SDK decodes nothing and keeps nothing
    Given a streamlet whose process emits each record as it came
    When the harness runs two batches through it
    Then every emitted value is the bytes that were put, unchanged
    And nothing from the first batch is held for the second

Feature: a streamlet runs on a laptop with no cluster
  A streamlet author runs each streamlet as a command on the host against Kafka in a container.
  Nothing of Kubernetes is needed, and what a Flink streamlet remembers there is not kept.

  Background:
    Given Kafka in a container on a laptop, and no cluster

  Scenario Outline: a streamlet runs as a command on the host in the laptop walkthrough
    Given a <streamlet> whose settings name the laptop's Kafka and its topics
    When the streamlet is run as a command on the host
    And a record is written to the inlet's topic
    Then the streamlet's result for the record is on the outlet's topic

    Examples:
      | streamlet               |
      | Pekko Streams streamlet |
      | Flink streamlet         |

  Scenario: a Flink streamlet on a laptop keeps its state only while it runs
    Given a Flink streamlet run as a command on the host
    When the command is stopped and run again
    Then the streamlet starts with no state
    And it reads its topics from where it had committed

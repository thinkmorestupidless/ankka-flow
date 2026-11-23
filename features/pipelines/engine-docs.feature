Feature: the documentation says which engine to choose
  A reader learns from one page whether a stage is a Flink streamlet, a Pekko Streams streamlet or
  an ankka consumer, and from the limitations page what each engine promises.

  Scenario: the engines page says which need selects each engine and which an ankka consumer
    Given the engines page
    When a reader looks for a stage that needs windows or state, one that needs neither, and one service reading one topic
    Then the page names a Flink streamlet for the first, a Pekko Streams streamlet for the second and an ankka consumer for the third

  Scenario: the limitations page says what each engine promises about delivery
    Given the limitations page
    When a reader looks for each engine's delivery
    Then the page says a Pekko Streams streamlet is at least once
    And says a Flink streamlet counts each record once between managed topics and is at least once into ankka

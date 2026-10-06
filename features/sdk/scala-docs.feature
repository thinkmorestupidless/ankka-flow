Feature: Every page with a Python side has a Scala side
  A Scala reader finds the Scala equivalent of every Python code sample, a guide for writing a
  streamlet in Scala, a Scala SDK reference, and skills that know both SDKs.

  Scenario: every page with a Python side has a Scala side from tested code
    Given a page showing Python streamlet code
    When a Scala reader reads it
    Then the page shows the Scala equivalent, included from tested code
    And the docs build passes

  Scenario: a Scala reader is told what to install
    Given the install page
    When a Scala reader looks for what a streamlet needs
    Then the page says what a Scala author installs
    And the Scala SDK is named with its published coordinates

  Scenario: the skills describe the Scala SDK
    Given the skills rendered from the pages
    When a coding agent writes a streamlet in Scala
    Then the skills describe the Scala SDK as they describe the Python SDK

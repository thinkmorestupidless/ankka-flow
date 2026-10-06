Feature: The documentation says install, and contributors still build
  A reader gets flow by installing it; a contributor finds the build from source where contributors
  look.

  Scenario: the install page offers the tap, then the archive, and no build from source
    Given the install page
    When a reader looks for how to get "flow"
    Then the first thing offered is the tap's install command
    And the archive for each platform is offered after it, with how to verify its checksum
    And the build from source is not on the page

  Scenario: no guide begins by building flow
    Given a guide or a sample's README that told the reader to build "flow"
    When it is read
    Then it tells the reader to install "flow", or assumes they have

  Scenario: a contributor finds the build from source, and it still works
    Given a contributor
    When they look for how to build "flow" from source
    Then the contributing page names "just cli" and the build task
    And both still build the JVM build

  Scenario: the skills say install
    Given the skills rendered from the documentation
    When they describe getting started
    Then they say install, not build
